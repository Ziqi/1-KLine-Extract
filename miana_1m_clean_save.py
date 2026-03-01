#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Download 1-minute Kline from miana, clean/sort, and save locally.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Tuple

from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Thread

import pandas as pd
import requests


MIANA_KLINE_URL = "https://miana.com.cn/api/stock/v2/kline"
MIANA_LIST_URL = "https://miana.com.cn/api/stock/v1/stockList"


def fetch_stock_list(token: str, market: str) -> List[Dict]:
    params = {"token": token, "market": market, "format": "json"}
    r = requests.get(MIANA_LIST_URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if payload.get("code") != 200:
        raise RuntimeError(f"stockList error: {payload}")
    return payload.get("data", []) or []


def build_symbols(token: str, include_bjs: bool) -> List[str]:
    data = fetch_stock_list(token, "cn_hs_a")
    if include_bjs:
        try:
            data += fetch_stock_list(token, "cn_bjs")
        except Exception:
            pass
    ex_map = {"xshg": "sh", "xshe": "sz", "bjse": "bj"}
    symbols = []
    for item in data:
        ex_raw = str(item.get("exchangeCode", "")).lower().strip()
        ex = ex_map.get(ex_raw, ex_raw)
        code = str(item.get("code", "")).strip()
        if ex in ("sh", "sz", "bj") and len(code) == 6:
            symbols.append(f"{ex}{code}")
    symbols = sorted(set(symbols))
    return symbols


def fetch_kline_1m(symbol: str, start_dt: datetime, end_dt: datetime, token: str, fq: str = "qfq") -> pd.DataFrame:
    params = {
        "token": token,
        "symbol": symbol,
        "type": "1min",
        "beginDate": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "endDate": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "order": "ASC",
        "fq": fq,
        "format": "json",
        "limit": "2000",
    }
    r = requests.get(MIANA_KLINE_URL, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()
    if int(payload.get("code", -1)) != 200:
        raise RuntimeError(f"kline error code={payload.get('code')} msg={payload.get('msg', '')}")
    rows = payload.get("data") or []
    return pd.DataFrame(rows)


def clean_1m(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["dt", "open", "high", "low", "close", "volume", "amount"])
    ren = {
        "date": "dt",
        "openPrice": "open",
        "highPrice": "high",
        "lowPrice": "low",
        "closePrice": "close",
        "price": "close",
        "vol": "volume",
        "turnover": "amount",
        "amt": "amount",
    }
    for k, v in ren.items():
        if k in df.columns and v not in df.columns:
            df[v] = df[k]
    for c in ("dt", "open", "high", "low", "close", "volume", "amount"):
        if c not in df.columns:
            df[c] = None
    df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
    for c in ("open", "high", "low", "close", "volume", "amount"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["dt", "open", "high", "low", "close"])
    df = df.sort_values("dt").drop_duplicates("dt", keep="last").reset_index(drop=True)

    t = df["dt"].dt.time
    drop_1300 = t == datetime.strptime("13:00:00", "%H:%M:%S").time()
    drop_1501_1531 = (t >= datetime.strptime("15:01:00", "%H:%M:%S").time()) & (
        t <= datetime.strptime("15:31:00", "%H:%M:%S").time()
    )
    df = df.loc[~(drop_1300 | drop_1501_1531)].copy()

    added_rows = []
    for day, g in df.groupby(df["dt"].dt.normalize()):
        g = g.sort_values("dt")
        has_1500 = (g["dt"].dt.time == datetime.strptime("15:00:00", "%H:%M:%S").time()).any()
        if has_1500 or g.empty:
            continue
        last = g.iloc[-1]
        new_row = last.copy()
        new_row["dt"] = pd.Timestamp(day) + pd.Timedelta(hours=15)
        new_row["open"] = last["close"]
        new_row["high"] = last["close"]
        new_row["low"] = last["close"]
        new_row["close"] = last["close"]
        new_row["volume"] = 0
        added_rows.append(new_row)

    if added_rows:
        df = pd.concat([df, pd.DataFrame(added_rows)], ignore_index=True)
        df = df.drop_duplicates("dt", keep="last").sort_values("dt").reset_index(drop=True)

    return df[["dt", "open", "high", "low", "close", "volume", "amount"]]


def iter_windows(start_dt: datetime, end_dt: datetime, window_days: int = 7):
    cur = start_dt
    step = timedelta(days=window_days)
    while cur <= end_dt:
        w1 = min(end_dt, cur + step - timedelta(seconds=1))
        yield cur, w1
        cur = w1 + timedelta(seconds=1)


def _fetch_one_symbol(
    sym: str,
    start_dt: datetime,
    end_dt: datetime,
    token: str,
    fq: str,
    out_dir: Path,
    window_days: int,
    retries: int,
) -> Tuple[str, bool, str]:
    last_err = ""
    out_path = out_dir / f"{sym}.csv"
    
    # 增量更新：如果本地文件已存在，读取最后一条记录的时间
    existing_df = pd.DataFrame()
    actual_start_dt = start_dt
    if out_path.exists():
        try:
            existing_df = pd.read_csv(out_path, parse_dates=["dt"])
            if not existing_df.empty:
                last_dt = existing_df["dt"].max()
                if last_dt >= end_dt:
                    return sym, True, f"already up-to-date (rows={len(existing_df)})"
                if last_dt > actual_start_dt:
                    actual_start_dt = last_dt
        except Exception as e:
            pass # 若读取失败则忽略，进行全量拉取

    for attempt in range(retries + 1):
        try:
            frames = []
            for w0, w1 in iter_windows(actual_start_dt, end_dt, window_days=window_days):
                df = fetch_kline_1m(sym, w0, w1, token, fq=fq)
                if df is not None and not df.empty:
                    frames.append(df)
            
            if frames:
                raw = pd.concat(frames, ignore_index=True)
            else:
                raw = pd.DataFrame()
                
            cleaned_new = clean_1m(raw)
            
            if not existing_df.empty:
                # 合并新旧数据并去重
                if cleaned_new.empty:
                    final_df = existing_df
                else:
                    final_df = pd.concat([existing_df, cleaned_new], ignore_index=True)
                    final_df = final_df.drop_duplicates("dt", keep="last").sort_values("dt").reset_index(drop=True)
            else:
                final_df = cleaned_new

            if final_df.empty:
                return sym, False, "empty"
            
            final_df.to_csv(out_path, index=False, encoding="utf-8")
            return sym, True, f"saved rows={len(final_df)} (new={len(cleaned_new)})"
        except Exception as exc:
            last_err = str(exc)
            if attempt < retries:
                time.sleep(1.0 + attempt)
            continue
    return sym, False, last_err or "error"


def main():
    parser = argparse.ArgumentParser(description="Download miana 1m, clean/sort, save.")
    parser.add_argument("--token", default=os.getenv("MIANA_TOKEN", ""), help="miana token (or MIANA_TOKEN env)")
    parser.add_argument("--out-dir", default="./out_1m", help="output directory")
    parser.add_argument("--start", default="2025-01-01 00:00:00", help="start datetime")
    parser.add_argument("--end", default=datetime.now().strftime("%Y-%m-%d %H:%M:%S"), help="end datetime")
    parser.add_argument("--max-symbols", type=int, default=0, help="limit symbols")
    parser.add_argument("--no-bjs", action="store_true", help="exclude BJ market")
    parser.add_argument("--symbol", default="", help="single symbol, e.g. sh600519")
    parser.add_argument("--fq", default="qfq", help="fq mode")
    parser.add_argument("--window-days", type=int, default=7, help="download window in days")
    parser.add_argument("--workers", type=int, default=8, help="parallel workers")
    parser.add_argument("--retries", type=int, default=2, help="retry per symbol")
    parser.add_argument("--heartbeat", type=float, default=30.0, help="heartbeat seconds")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    if not args.token:
        logging.error("missing token: provide --token or MIANA_TOKEN env")
        raise RuntimeError("missing token: provide --token or MIANA_TOKEN env")

    if args.window_days > 8:
        logging.error(f"window-days ({args.window_days}) 不能大于 8，会导致 API 数据可能被截断丢失。")
        raise ValueError("window-days 超过上限，最大允许 8 天")

    start_dt = pd.to_datetime(args.start)
    end_dt = pd.to_datetime(args.end)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.symbol:
        symbols = [args.symbol.lower().strip()]
    else:
        symbols = build_symbols(args.token, include_bjs=not args.no_bjs)
        if args.max_symbols and args.max_symbols > 0:
            symbols = symbols[: args.max_symbols]

    total = len(symbols)
    ok = 0
    fail = 0
    done = 0
    last_sym = ""
    lock = Lock()
    t0 = time.time()

    def heartbeat():
        while True:
            time.sleep(max(1.0, float(args.heartbeat)))
            with lock:
                elapsed = time.time() - t0
                logging.info(f"[heartbeat] done={done}/{total} ok={ok} fail={fail} last={last_sym} elapsed={elapsed:.1f}s")
            if done >= total:
                break

    hb = Thread(target=heartbeat, daemon=True)
    hb.start()

    def _wrap(sym: str):
        return _fetch_one_symbol(
            sym=sym,
            start_dt=start_dt,
            end_dt=end_dt,
            token=args.token,
            fq=args.fq,
            out_dir=out_dir,
            window_days=int(args.window_days),
            retries=int(args.retries),
        )

    workers = max(1, int(args.workers))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(_wrap, sym): sym for sym in symbols}
        for fut in as_completed(futures):
            sym, success, msg = fut.result()
            with lock:
                done += 1
                last_sym = sym
                if success:
                    ok += 1
                else:
                    fail += 1
            status = "ok" if success else "fail"
            log_msg = f"[{done}/{total}] {sym} {status} {msg}"
            if success:
                logging.info(log_msg)
            else:
                logging.error(log_msg)

    logging.info(f"All tasks done. ok={ok} fail={fail}")


if __name__ == "__main__":
    main()
