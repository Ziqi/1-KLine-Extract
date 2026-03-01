#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Convert 1-minute Miana Kline data to 5-minute Kline data suitable for Kronos Model training.
Kronos requires columns: ['timestamps', 'open', 'high', 'low', 'close', 'volume', 'amount']
"""

import os
import glob
import logging
import argparse
import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def resample_single_file(input_file: str, output_dir: str):
    """
    Read a 1m CSV, resample it to 5m, and save it to output_dir.
    Resampling logic:
    - open: first
    - high: max
    - low: min
    - close: last
    - volume: sum
    - amount: sum
    """
    try:
        # Load the 1m data
        df = pd.read_csv(input_file)
        if df.empty:
            return f"Skipped {os.path.basename(input_file)}: Empty file"

        # Ensure 'dt' is datetime and set as index
        df['dt'] = pd.to_datetime(df['dt'])
        df.set_index('dt', inplace=True)

        # Define the aggregation dictionary
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum',
            'amount': 'sum'
        }

        # Perform the 5-minute resampling
        # '5T' means 5 minutes. 'closed='right', label='right'' is often standard in finance,
        # but for A-shares, typically a 5-min bar starting at 09:30 and ending at 09:35 is labeled 09:35.
        df_5m = df.resample('5T', closed='right', label='right').agg(agg_dict)

        # Drop rows where there was no trading (NaN in 'open')
        df_5m.dropna(subset=['open'], inplace=True)

        # Reset index and rename 'dt' to 'timestamps' to match Kronos input requirements
        df_5m.reset_index(inplace=True)
        df_5m.rename(columns={'dt': 'timestamps'}, inplace=True)

        # Reorder columns as expected by Kronos
        cols = ['timestamps', 'open', 'high', 'low', 'close', 'volume', 'amount']
        df_5m = df_5m[cols]

        # Save to the output directory
        base_name = os.path.basename(input_file)
        out_path = os.path.join(output_dir, base_name)
        df_5m.to_csv(out_path, index=False)
        return f"Success {base_name}: {len(df)} -> {len(df_5m)} rows"

    except Exception as e:
        return f"Error {os.path.basename(input_file)}: {str(e)}"

def main():
    parser = argparse.ArgumentParser(description="Resample 1m Kline CSVs to 5m for Kronos.")
    parser.add_argument("--input-dir", default="./out_1m", help="Directory containing 1m CSV files")
    parser.add_argument("--output-dir", default="./out_5m", help="Directory to save 5m CSV files")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4, help="Number of parallel workers")

    args = parser.parse_args()

    in_dir = Path(args.input_dir)
    out_dir = Path(args.output_dir)

    if not in_dir.exists():
        logging.error(f"Input directory does not exist: {in_dir}")
        return

    out_dir.mkdir(parents=True, exist_ok=True)

    csv_files = glob.glob(str(in_dir / "*.csv"))
    total_files = len(csv_files)
    logging.info(f"Found {total_files} CSV files to process. Output to: {out_dir}")

    # Process files in parallel
    success_count = 0
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(resample_single_file, f, str(out_dir)) for f in csv_files]
        
        for i, future in enumerate(futures, 1):
            res_msg = future.result()
            if res_msg.startswith("Success"):
                success_count += 1
            if i % 100 == 0 or i == total_files:
                logging.info(f"Processed {i}/{total_files} | Latest: {res_msg}")

    logging.info(f"Finished. Successfully resampled {success_count} files to 5m format.")

if __name__ == "__main__":
    main()
