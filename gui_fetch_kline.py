#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import requests
import datetime
import threading
import pandas as pd
import tkinter as tk
from tkinter import messagebox
from tkinter.scrolledtext import ScrolledText
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from pathlib import Path

# === API Config ===
MIANA_TOKEN = os.getenv("MIANA_TOKEN", "YOUR_API_TOKEN_HERE")  # 为了安全，请替换为您自己的 Token 或使用环境变量
MIANA_LIST_URL = "https://miana.com.cn/api/stock/v1/stockList"
MIANA_KLINE_URL = "https://miana.com.cn/api/stock/v2/kline"

class DashFrame(tk.Frame):
    def __init__(self, master, title, bg_color, fg_color, dash_color, font, *args, **kwargs):
        super().__init__(master, bg=bg_color, *args, **kwargs)
        self.bg_color = bg_color
        self.dash_color = dash_color
        
        self.canvas = tk.Canvas(self, bg=bg_color, highlightthickness=0)
        self.canvas.place(relx=0, rely=0, relwidth=1, relheight=1)
        
        # 中间的容器给外面放东西
        self.content = tk.Frame(self, bg=bg_color)
        self.content.pack(fill=BOTH, expand=True, padx=12, pady=(25, 12)) 
        
        # 绑定位姿变化以重绘虚线
        self.bind("<Configure>", self._draw)
        
        self.title_text = title
        self.fg_color = fg_color
        self.font = font
        
    def _draw(self, event=None):
        self.canvas.delete("all")
        w, h = self.winfo_width(), self.winfo_height()
        if w < 10 or h < 10: return
        
        # 画虚线框
        self.canvas.create_rectangle(2, 10, w-2, h-2, outline=self.dash_color, dash=(5, 5))
        
        # 补一块背景遮住线，再画文字
        # 估算文字宽度... 或直接固定一个底
        self.canvas.create_rectangle(15, 0, 15 + len(self.title_text)*10, 20, fill=self.bg_color, outline="")
        self.canvas.create_text(20, 10, text=self.title_text, anchor="w", font=self.font, fill=self.fg_color)


class KlineDataFetcherGUI(ttk.Window):
    def __init__(self):
        super().__init__()  # Removed themename="cyborg" to fix macOS OpenGL minimize freeze
        self.style = ttk.Style(theme="cyborg")
        self.title("全市场一分钟K线历史数据抓取控制台")
        self.geometry("1100x860")
        self.minsize(1050, 800)
        
        # 修复 macOS 最小化彻底卡死崩溃的问题
        try:
            self.createcommand('::tk::mac::ReopenApplication', self.deiconify)
            self.protocol("WM_DELETE_WINDOW", self.on_closing)
        except Exception:
            pass
        
        # 极简黑金主题色 (Flat Dark Gold)
        self.c_bg = "#080808"        # 终极深邃黑
        self.c_panel = "#101010"     # 面板底色更沉
        self.c_gold = "#F0B90B"      # 极客明黄亮金
        self.c_gold_dim = "#715A2B"  # 昏暗辅助金
        self.c_fg = "#E1C699"        # 【全局非白】黑金体系替代白色
        self.c_green = "#00D47C"     # 荧光极客绿
        self.c_red = "#FF3B30"       # 警报红
        
        # 定义极客字体簇 (优先等宽与代码字体)
        self.font_title = ("Menlo", 36, "bold")
        self.font_base = ("Menlo", 14)
        self.font_base_lg = ("Menlo", 16)
        self.font_log = ("Menlo", 13)
        
        # 覆盖 ttk 样式，实现极致扁平和去凸起化
        style = ttk.Style()
        style.configure(".", font=self.font_base, background=self.c_bg, foreground=self.c_fg)
        
        # 极简边框 LabelFrame (模拟独立卡片外框)
        style.configure("TLabelframe", background=self.c_bg, bordercolor=self.c_gold_dim, borderwidth=1, relief="solid")
        style.configure("TLabelframe.Label", font=("Menlo", 15, "bold"), foreground=self.c_gold, background=self.c_bg)
        
        # 扁平黑金按钮 (Outline 风格模拟极简虚线/细线)
        style.configure("FlatGold.TButton", font=self.font_base_lg, background=self.c_bg, foreground=self.c_gold, bordercolor=self.c_gold, borderwidth=1)
        style.map("FlatGold.TButton", background=[("active", "#1A140B")], foreground=[("active", "#FFD700")])
        
        style.configure("FlatRed.TButton", font=self.font_base_lg, background=self.c_bg, foreground=self.c_red, bordercolor=self.c_red, borderwidth=1)
        style.map("FlatRed.TButton", background=[("active", "#1A0505")])
        
        # 沉浸式极简无箭头滚动条 (Hidden Scrollbar)
        style.layout('Hidden.Vertical.TScrollbar', 
             [('Vertical.Scrollbar.trough', {'children': [('Vertical.Scrollbar.thumb', {'expand': '1', 'sticky': 'nswe'})], 'sticky': 'ns'})])
        style.configure("Hidden.Vertical.TScrollbar", background=self.c_gold_dim, troughcolor=self.c_bg, bordercolor=self.c_bg, relief="flat")
        style.map("Hidden.Vertical.TScrollbar", background=[("active", self.c_gold)])
        
        style.layout('Hidden.Horizontal.TScrollbar', 
             [('Horizontal.Scrollbar.trough', {'children': [('Horizontal.Scrollbar.thumb', {'expand': '1', 'sticky': 'nswe'})], 'sticky': 'we'})])
        style.configure("Hidden.Horizontal.TScrollbar", background=self.c_gold_dim, troughcolor=self.c_bg, bordercolor=self.c_bg, relief="flat")
        style.map("Hidden.Horizontal.TScrollbar", background=[("active", self.c_gold)])
        
        # Treeview 表格高级样式
        style.configure("Treeview", background=self.c_panel, foreground=self.c_fg, fieldbackground=self.c_panel, borderwidth=0, font=self.font_base, rowheight=32)
        style.map("Treeview", background=[("selected", "#2A2111")], foreground=[("selected", self.c_gold)])
        style.configure("Treeview.Heading", font=("Menlo", 13, "bold"), background=self.c_bg, foreground=self.c_gold, borderwidth=1)
        
        self.stock_list = []
        self.stock_dict = {} 
        
        # Cross-directory absolute path resolution
        base_path = Path(__file__).resolve().parent.parent
        self.out_dir = base_path / "1-KLine-Extract" / "gui_downloads"
        self.out_dir.mkdir(parents=True, exist_ok=True)
        
        # 观察池队列 (Market Radar Pool)
        self.radar_pool = [] # List of dicts: {"name": str, "symbol": str, "display": str}
        
        self.fetch_thread = None
        self.is_paused = False
        self.stop_requested = False
        
        self.setup_ui()
        self.load_stock_list_thread()
        self.poll_downloads_dir()

    def setup_ui(self):
        self.configure(bg=self.c_bg)
        
        # Force macOS to bring this window to the very front
        self.lift()
        self.attributes('-topmost', True)
        self.after(1000, lambda: self.attributes('-topmost', False))
        os.system('''osascript -e 'tell application "System Events" to set frontmost of the first process whose unix id is %d to true' ''' % os.getpid())
        
        # --- HEADER ---
        header_frame = tk.Frame(self, bg=self.c_bg, pady=15)
        header_frame.pack(fill=X, padx=20)
        
        tk.Label(header_frame, text="全市场 1分钟K线数据下载器", font=self.font_title, fg=self.c_gold, bg=self.c_bg).pack(side=LEFT)
        self.status_sign = tk.Label(header_frame, text="系统就绪", font=("Menlo", 16, "bold"), fg=self.c_gold_dim, bg=self.c_bg)
        self.status_sign.pack(side=RIGHT, anchor=S)

        # --- BODY ---
        body_frame = tk.Frame(self, bg=self.c_bg)
        body_frame.pack(fill=BOTH, expand=True, padx=20, pady=(0, 20))
        
        # --- LEFT PANEL: CONTROLS ---
        left_panel = tk.Frame(body_frame, width=400, bg=self.c_bg)
        left_panel.pack(side=LEFT, fill=Y, padx=(0, 20))
        left_panel.pack_propagate(False)
        
        # 0. Market Radar Pool (观察池)
        radar_lf = DashFrame(left_panel, title=" 股票待下载区 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        radar_lf.pack(fill=X, pady=(0, 15))
        
        tk.Label(radar_lf.content, text="股票观察池 WATCHLIST (支持Shift多选/双向清除):", font=self.font_base, fg=self.c_gold_dim, bg=self.c_bg).pack(anchor=W, pady=(0, 5))
        
        pool_frame = tk.Frame(radar_lf.content, bg=self.c_panel)
        pool_frame.pack(fill=X)
        self.pool_listbox = tk.Listbox(pool_frame, height=5, font=("Menlo", 14, "bold"), bg=self.c_panel, fg=self.c_gold, selectbackground="#2A2111", selectforeground=self.c_gold, borderwidth=0, highlightthickness=1, highlightbackground=self.c_gold_dim, selectmode=EXTENDED)
        
        pl_scroll = ttk.Scrollbar(pool_frame, orient=tk.VERTICAL, command=self.pool_listbox.yview, style="Hidden.Vertical.TScrollbar")
        self.pool_listbox.configure(yscrollcommand=pl_scroll.set)
        
        pl_scroll.pack(side=RIGHT, fill=tk.Y)
        self.pool_listbox.pack(side=LEFT, fill=X, expand=True, padx=2, pady=2)
        
        self.pool_listbox.bind('<Delete>', self.on_remove_from_pool)
        self.pool_listbox.bind('<BackSpace>', self.on_remove_from_pool)
        
        # 多选清除按键
        btn_frame = tk.Frame(radar_lf.content, bg=self.c_bg)
        btn_frame.pack(fill=X, pady=(8, 0))
        ttk.Button(btn_frame, text="[ 批量剥离选中目标 ]", style="FlatGold.TButton", command=self.on_remove_from_pool).pack(side=RIGHT)
        
        # 1. Search Box (Minimal) -> INJECTION
        search_lf = DashFrame(left_panel, title=" 操作面板 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        search_lf.pack(fill=X, pady=(0, 15))
        
        # --- 模式一：极速检索 / 直接列队 ---
        txt1 = "[模式1] 手动检索与注入:\n支持单代码回车；\n或直接粘贴研报段落，按回车进行行内嗅探"
        tk.Label(search_lf.content, text=txt1, justify=LEFT, font=self.font_base, fg=self.c_fg, bg=self.c_bg).pack(anchor=W, pady=(0, 5))
        self.search_var = tk.StringVar()
        self.search_var.trace("w", self.on_search_typing)
        
        self.search_entry = tk.Entry(search_lf.content, textvariable=self.search_var, font=("Menlo", 18, "bold"), bg=self.c_panel, fg=self.c_gold, insertbackground=self.c_gold, relief="flat", highlightthickness=1, highlightbackground=self.c_gold_dim)
        self.search_entry.pack(fill=X, pady=(0, 2))
        self.search_entry.bind('<Return>', self.on_search_enter_hit)
        
        self.match_label = tk.Label(search_lf.content, text="未检索到匹配的股票", font=self.font_base_lg, fg=self.c_gold_dim, bg=self.c_bg)
        self.match_label.pack(anchor=W, pady=(0, 12))
        
        # --- 模式二：剪贴板雷达嗅探 ---
        txt2 = "[模式2] 系统全局剪贴板嗅探:\n无需粘贴，复制包含标的文本后\n直接点击下方雷达按钮"
        tk.Label(search_lf.content, text=txt2, justify=LEFT, font=self.font_log, fg=self.c_fg, bg=self.c_bg).pack(anchor=W, pady=(0, 5))
        
        batch_btn = ttk.Button(search_lf.content, text="[ 呼叫剪贴板雷达嗅探 ]", padding=(0, 4), style="FlatGold.TButton", command=self.on_batch_paste)
        batch_btn.pack(fill=X, pady=(5, 5))
        
        # 2. Date Range
        date_lf = DashFrame(left_panel, title=" 时间范围过滤 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        date_lf.pack(fill=X, pady=(0, 15))
        
        self.all_history_var = tk.BooleanVar(value=False)
        self.all_history_cb = tk.Checkbutton(date_lf.content, text="忽略时间，自动感知该股上市起点提取全局数据", variable=self.all_history_var, command=self.toggle_dates, font=self.font_base, fg=self.c_gold, bg=self.c_bg, selectcolor=self.c_panel, activebackground=self.c_bg, activeforeground=self.c_gold)
        self.all_history_cb.pack(fill=X, anchor=W, pady=(0, 15))
        
        self.date_pickers_frame = tk.Frame(date_lf.content, bg=self.c_bg)
        self.date_pickers_frame.pack(fill=X)
        
        # Start Comboboxes
        s_frame = tk.Frame(self.date_pickers_frame, bg=self.c_bg)
        s_frame.pack(fill=X, pady=(0, 10))
        tk.Label(s_frame, text="开始", width=4, font=self.font_base, bg=self.c_bg, fg=self.c_fg).pack(side=LEFT)
        start_date = datetime.datetime.now() - datetime.timedelta(days=365)
        self.start_y = ttk.Combobox(s_frame, values=[str(y) for y in range(1990, 2030)], width=5, font=self.font_base)
        self.start_y.set(start_date.strftime("%Y"))
        self.start_y.pack(side=LEFT, padx=2)
        self.start_m = ttk.Combobox(s_frame, values=[f"{m:02d}" for m in range(1, 13)], width=3, font=self.font_base)
        self.start_m.set(start_date.strftime("%m"))
        self.start_m.pack(side=LEFT, padx=2)
        self.start_d = ttk.Combobox(s_frame, values=[f"{d:02d}" for d in range(1, 32)], width=3, font=self.font_base)
        self.start_d.set(start_date.strftime("%d"))
        self.start_d.pack(side=LEFT, padx=2)
        
        # End Comboboxes
        e_frame = tk.Frame(self.date_pickers_frame, bg=self.c_bg)
        e_frame.pack(fill=X)
        tk.Label(e_frame, text="结束", width=4, font=self.font_base, bg=self.c_bg, fg=self.c_fg).pack(side=LEFT)
        end_date = datetime.datetime.now()
        self.end_y = ttk.Combobox(e_frame, values=[str(y) for y in range(1990, 2030)], width=5, font=self.font_base)
        self.end_y.set(end_date.strftime("%Y"))
        self.end_y.pack(side=LEFT, padx=2)
        self.end_m = ttk.Combobox(e_frame, values=[f"{m:02d}" for m in range(1, 13)], width=3, font=self.font_base)
        self.end_m.set(end_date.strftime("%m"))
        self.end_m.pack(side=LEFT, padx=2)
        self.end_d = ttk.Combobox(e_frame, values=[f"{d:02d}" for d in range(1, 32)], width=3, font=self.font_base)
        self.end_d.set(end_date.strftime("%d"))
        self.end_d.pack(side=LEFT, padx=2)
        
        # 3. Action Controls
        ctrl_lf = DashFrame(left_panel, title=" 下载控制 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        ctrl_lf.pack(fill=BOTH, expand=True)

        self.start_btn = ttk.Button(ctrl_lf.content, text="开始下载数据", style="FlatGold.TButton", command=self.on_start_click)
        self.start_btn.pack(fill=X, pady=(5, 15), ipady=5)
        
        self.pause_btn = ttk.Button(ctrl_lf.content, text="挂起当前通道", style="FlatGold.TButton", command=self.on_pause_click, state=DISABLED)
        self.pause_btn.pack(fill=X, pady=(0, 15), ipady=5)
        
        self.stop_btn = ttk.Button(ctrl_lf.content, text="停止下载", style="FlatRed.TButton", command=self.on_stop_click, state=DISABLED)
        self.stop_btn.pack(fill=X, pady=(0, 15), ipady=5)

        # --- RIGHT PANEL ---
        right_panel = tk.Frame(body_frame, bg=self.c_bg)
        right_panel.pack(side=LEFT, fill=BOTH, expand=True)
        
        # 顶部的列表 Labelframe
        assets_lf = DashFrame(right_panel, title=" 已下载 1分钟K线列表 (选中按 Delete 删除) ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        assets_lf.pack(fill=BOTH, expand=True, pady=(0, 20))
        
        columns = ("name", "code", "start", "end", "size", "open", "delete")
        # 外套一个 Frame 限制 Tree 和 Scrollbar
        tree_container = tk.Frame(assets_lf.content, bg=self.c_bg)
        tree_container.pack(fill=BOTH, expand=True)
        
        self.tree = ttk.Treeview(tree_container, columns=columns, show="headings", selectmode="extended")
        self.tree.heading("name", text="股票名称")
        self.tree.heading("code", text="代码")
        self.tree.heading("start", text="开始日期")
        self.tree.heading("end", text="结束日期")
        self.tree.heading("size", text="文件大小")
        self.tree.heading("open", text="[ 打开 ]")
        self.tree.heading("delete", text="[ 删除 ]")
        
        self.tree.column("name", width=120, anchor=tk.W)
        self.tree.column("code", width=100, anchor=tk.W)
        self.tree.column("start", width=100, anchor=tk.CENTER)
        self.tree.column("end", width=100, anchor=tk.CENTER)
        self.tree.column("size", width=100, anchor=tk.E)
        self.tree.column("open", width=70, anchor=tk.CENTER)
        self.tree.column("delete", width=70, anchor=tk.CENTER)
        
        # 使用 Hidden 滚动条
        tree_yscroll = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.tree.yview, style="Hidden.Vertical.TScrollbar")
        self.tree.configure(yscrollcommand=tree_yscroll.set)
        
        tree_yscroll.pack(side=RIGHT, fill=tk.Y)
        self.tree.pack(side=LEFT, fill=BOTH, expand=True)
        
        # Bindings for tree
        self.tree.bind('<ButtonRelease-1>', self.on_tree_click)
        self.tree.bind('<Delete>', self.on_delete_asset)
        self.tree.bind('<BackSpace>', self.on_delete_asset)
        
        # BOTTOM RIGHT: Console Stream
        console_lf = DashFrame(right_panel, title=" 运行日志 ", bg_color=self.c_bg, fg_color=self.c_gold, dash_color=self.c_gold_dim, font=("Menlo", 15, "bold"))
        console_lf.pack(fill=BOTH, expand=True)
        
        # Fake progress bar in console style
        prog_frame = tk.Frame(console_lf.content, bg=self.c_bg)
        prog_frame.pack(fill=X, pady=(5, 5))
        
        self.prog_lbl = tk.Label(prog_frame, text="待命：准备下载", font=self.font_base_lg, fg=self.c_gold, bg=self.c_bg)
        self.prog_lbl.pack(anchor=W)
        
        prog_bar_border = tk.Frame(console_lf.content, bg=self.c_gold_dim, height=4)
        prog_bar_border.pack(fill=X, expand=False, padx=2)
        self.prog_bar_fill = tk.Frame(prog_bar_border, bg=self.c_gold, width=0, height=4)
        self.prog_bar_fill.pack(side=LEFT)
        self.prog_bar_border = prog_bar_border
        
        # Hand-made ScrolledText using ttk.Scrollbar to follow theme
        txt_frame = tk.Frame(console_lf.content, bg=self.c_panel)
        txt_frame.pack(fill=BOTH, expand=True, pady=(10, 0), padx=5)
        
        self.log_widget = tk.Text(txt_frame, font=self.font_log, bg=self.c_panel, fg=self.c_fg, insertbackground=self.c_fg, wrap=tk.WORD, borderwidth=0, highlightthickness=0, spacing1=4, spacing3=4)
        txt_scroll = ttk.Scrollbar(txt_frame, orient=tk.VERTICAL, command=self.log_widget.yview, style="Hidden.Vertical.TScrollbar")
        self.log_widget.configure(yscrollcommand=txt_scroll.set)
        
        txt_scroll.pack(side=RIGHT, fill=tk.Y)
        self.log_widget.pack(side=LEFT, fill=BOTH, expand=True)
        
        # 定义日志高亮 Tag (Markdown style)
        self.log_widget.tag_config("info", foreground=self.c_fg)
        self.log_widget.tag_config("warn", foreground="#FF9800", font=("Menlo", 13, "bold"))
        self.log_widget.tag_config("err", foreground=self.c_red, font=("Menlo", 13, "bold"))
        self.log_widget.tag_config("succ", foreground=self.c_green, font=("Menlo", 13, "bold"))
        self.log_widget.tag_config("sys", foreground=self.c_gold, font=("Menlo", 13, "bold"))
        
        self.log_widget.configure(state='disabled')

    def set_progress(self, percent):
        total_w = self.prog_bar_border.winfo_width()
        if total_w <= 1: total_w = 600
        w = int((percent / 100.0) * total_w)
        self.prog_bar_fill.config(width=w)

    def on_search_typing(self, *args):
        val = self.search_var.get().strip().lower()
        if not val:
            self.match_label.config(text="请在上方输入代码进行检索", fg=self.c_gold_dim)
            self._temp_match = None
            return
            
        if not hasattr(self, 'stock_list') or len(self.stock_list) == 0:
            self.match_label.config(text="[-] 核心库为空(远端断网或未就绪)，无法支持联想匹配", fg=self.c_red)
            self._temp_match = None
            return
            
        found = None
        for item in self.stock_list:
            if val in item.lower():
                found = item
                break
                
        if found:
            # 找到匹配，暂存起来供回车键提交
            self._temp_match = {
                "display": found,
                "symbol": self.stock_dict[found],
                "name": found.split(" ")[0]
            }
            self.match_label.config(text=f"[√] 雷达锁定:: {found}  (按回车入列)", fg=self.c_green)
        else:
            self.match_label.config(text="[x] 盲区：无确切坐标点，请检查输入", fg=self.c_red)
            self._temp_match = None

    def _check_local_exists_and_warn(self, symbol, name):
        """返回 True 允许继续，返回 False 中止"""
        exist = list(self.out_dir.glob(f"*_{symbol}_1m_*.csv"))
        if exist:
            return messagebox.askyesno("本地资产覆盖警告", f"目标 [{name}] ({symbol}) 在硬盘中已有存量包。\n\n再次提取会造成片段的覆盖或堆叠。确实要将其入列吗？")
        return True

    def on_search_enter_hit(self, event):
        """当用户在输入框按回车时，将暂存命中的标的装填进雷达观察池中，或触发长文本的多点打散"""
        val = self.search_var.get().strip()
        
        if hasattr(self, '_temp_match') and self._temp_match:
            item = self._temp_match
            # 查重
            exists = any(p["symbol"] == item["symbol"] for p in self.radar_pool)
            if not exists:
                if self._check_local_exists_and_warn(item["symbol"], item["name"]):
                    self.radar_pool.append(item)
                    self._update_pool_ui()
                    self.log_msg(f"[+] 目标已装填入列: {item['display']}")
            else:
                self.log_msg(f"[-] 目标已存在雷达池中，跳过重复装填: {item['display']}")
                
            # 清空输入装填下一发
            self.search_var.set("")
            self.search_entry.focus_set()
        else:
            # 没有构成精准匹配，尝试行内无差别批量段落嗅探
            if len(val) > 6 or ',' in val or '，' in val or ' ' in val:
                self.log_msg(f"[*] 侦测到非常规输入矩阵，自适应转入[行内文本模糊嗅探引擎]进行分解处理...")
                self.on_batch_paste(direct_text=val)
                self.search_var.set("")
                self.search_entry.focus_set()
            
    def on_batch_paste(self, direct_text=None):
        if direct_text is not None:
            text = direct_text
        else:
            try:
                text = self.clipboard_get()
            except:
                messagebox.showwarning("剪贴板空", "未能在系统剪贴板中检测到可读的文本数据。")
                return
            
        if not text.strip(): return
        
        import re
        import difflib
        
        added_count, skipped_count = 0, 0
        unknown_codes = set()
        codes_found = []
        
        # 1. 尝试嗅探所有类股票 6位数字以及前缀
        potential_codes = re.findall(r'(?i)(sh|sz|bj)?(\d{6})', text)
        for prefix, digits in potential_codes:
            codes_found.append(f"{prefix.lower()}{digits}" if prefix else digits)
                
        # 2. 词霸硬解：从文本里切分出碎词进行模糊识别 (支持简称和错别字)
        words = re.split(r'[\s\n\t,，、。；;/|]+', text.strip())
        names_found = set()
        
        to_add = []
        
        # 如果核心库断网没拉下来，就跳过所有基于中文的模糊比对
        if not hasattr(self, 'stock_list') or not self.stock_list:
            if words and not codes_found:
                messagebox.showerror("核心库未挂载", "当前与远端 MIANA 的连接已断开 (底层库为空)。\n\n智能嗅探无法进行中文名和错别字的模糊反查，仅支持纯 6 位数字代码探测。")
                return
        else:
            pure_names = [item.split(" (")[0] for item in self.stock_list]
            name_to_item = {item.split(" (")[0]: item for item in self.stock_list}
            
            for w in set(words):
                w = w.strip()
                if not w or len(w) < 2: continue
                
                # 避开纯数字长串 (交给正则已处理)
                if re.match(r'^\d+$', w): continue
                
                # 精确包含匹配 (如 "中微" -> 匹配到 "中微公司")
                exacts = [n for n in pure_names if w == n or w in n or n in w]
                if exacts:
                    # 过滤掉像 "股份" 这样宽泛无脑匹配到一堆公司的情况
                    if len(exacts) <= 8:
                        # 选最短的最贴切 (中微 -> 中微公司，而不是 中微半导体器材)
                        best = sorted(exacts, key=len)[0]
                        names_found.add(best)
                else:
                    # 错别字/模糊匹配 (如 "华鲁恒生" -> 匹配到 "华鲁恒升")
                    if len(w) >= 2:
                        matches = difflib.get_close_matches(w, pure_names, n=1, cutoff=0.55)
                        if matches:
                            names_found.add(matches[0])
            
            for nf in names_found:
                item = name_to_item[nf]
                to_add.append({"display": item, "symbol": self.stock_dict[item], "name": nf})
                
        for cf in codes_found:
            found = False
            for item in getattr(self, 'stock_list', []):
                sym = self.stock_dict[item]
                if cf == sym or (len(cf)==6 and sym.endswith(cf)):
                    to_add.append({"display": item, "symbol": sym, "name": item.split(" ")[0]})
                    found = True
                    break
            if not found and len(cf) == 6:
                unknown_codes.add(cf)
                    
        # 去重
        unique_to_add = []
        seen_syms = set()
        for t in to_add:
            if t["symbol"] not in seen_syms:
                seen_syms.add(t["symbol"])
                unique_to_add.append(t)
                
        # 对于未知6位码，强行猜测 ETF/指数 补全前缀
        guessed_etfs = []
        for uc in unknown_codes:
            if uc in seen_syms: continue
            if uc.startswith('5') or uc.startswith('000'):
                sym = f"sh{uc}"
                guessed_etfs.append({"display": f"盲区强制/ETF或指数 ({sym})", "symbol": sym, "name": f"ETF_{uc}"})
            elif uc.startswith('159') or uc.startswith('399'):
                sym = f"sz{uc}"
                guessed_etfs.append({"display": f"盲区强制/ETF或指数 ({sym})", "symbol": sym, "name": f"ETF_{uc}"})
        
        if guessed_etfs:
            if messagebox.askyesno("发现未知独立代码", f"嗅探到 {len(guessed_etfs)} 个无法匹配常规A股的 6 位代码（大概率为 ETF 基金或指数）。\n\n是否允许将其强行按 ETF/指数 格式加入列队尝试提取？"):
                unique_to_add.extend(guessed_etfs)
                
        if not unique_to_add:
            messagebox.showinfo("无有效检出", "未能在极客剪贴板数据中嗅探出任何有效目标。")
            return
            
        conflict_items = [item['name'] for item in unique_to_add if list(self.out_dir.glob(f"*_{item['symbol']}_1m_*.csv"))]
        if conflict_items:
            if not messagebox.askyesno("本地资产覆盖警告", f"剪贴板分析完毕，但有 {len(conflict_items)} 只标的 (如 {conflict_items[0]}) 已在本地有过数据落盘。\n\n批量提交将无视警告执行无限制改写，是否继续装填？"):
                return
                
        for item in unique_to_add:
            if any(p["symbol"] == item["symbol"] for p in self.radar_pool):
                skipped_count += 1
            else:
                self.radar_pool.append(item)
                added_count += 1
                
        self._update_pool_ui()
        self.log_msg(f"[+] 剪贴板闪电嗅探阵列加载完成! --> 新增提权目标 {added_count} 只, 剔除已在列 {skipped_count} 只。")
            
    def _update_pool_ui(self):
        self.pool_listbox.delete(0, END)
        for i, item in enumerate(self.radar_pool):
            self.pool_listbox.insert(END, f" [{i+1}] {item['display']}")

    def on_remove_from_pool(self, event=None):
        sel = self.pool_listbox.curselection()
        if not sel: return
        
        removed_names = []
        for idx in sorted(sel, reverse=True):
            removed_item = self.radar_pool.pop(idx)
            removed_names.append(removed_item['display'].split(" ")[0])
            
        self._update_pool_ui()
        self.log_msg(f"[-] 目标已从雷达池中批量剥离 ({len(removed_names)}只): {', '.join(removed_names)}")

    def poll_downloads_dir(self):
        """Auto checks local dir and updates the Treeview with parsed data."""
        if not hasattr(self, '_file_mapping'):
            self._file_mapping = {}
            
        if self.out_dir.exists():
            import re
            files = sorted(self.out_dir.glob("*.csv"), key=os.path.getmtime, reverse=True)
            
            # Read what is currently on screen
            current_iids = self.tree.get_children()
            current_files = [self.tree.item(iid)['values'][-1] for iid in current_iids] # store raw filename in a hidden logic or dictionary
            
            new_files = [f.name for f in files]
            
            if current_files != new_files:
                # 记录选中状态以便还原
                sel_iids = self.tree.selection()
                sel_files = [self._file_mapping.get(iid) for iid in sel_iids if iid in self._file_mapping]
                
                for iid in current_iids:
                    self.tree.delete(iid)
                
                self._file_mapping.clear()
                
                new_sel_iids = []
                for f in files:
                    size_kb = f.stat().st_size / 1024.0
                    size_str = f"{size_kb:.1f} KB"
                    m = re.match(r"^(.*)_(.*)_1m_(\d{8})_to_(\d{8})\.csv$", f.name)
                    if m:
                        name, code, start_d, end_d = m.groups()
                        s_fmt = f"{start_d[:4]}-{start_d[4:6]}-{start_d[6:]}"
                        e_fmt = f"{end_d[:4]}-{end_d[4:6]}-{end_d[6:]}"
                        item_id = self.tree.insert("", END, values=(name, code, s_fmt, e_fmt, size_str, "[ 打开 ]", "[ 删除 ]"))
                        self._file_mapping[item_id] = f.name
                    else:
                        item_id = self.tree.insert("", END, values=(f.name, "N/A", "N/A", "N/A", size_str, "[ 打开 ]", "[ 删除 ]"))
                        self._file_mapping[item_id] = f.name
                        
                    if f.name in sel_files:
                        new_sel_iids.append(item_id)
                        
                if new_sel_iids:
                    self.tree.selection_set(new_sel_iids)
                
        self.after(2000, self.poll_downloads_dir)
        
    def on_tree_click(self, event):
        """Handle click in specific columns to act as buttons."""
        region = self.tree.identify("region", event.x, event.y)
        if region == "cell":
            col = self.tree.identify_column(event.x)
            iid = self.tree.identify_row(event.y)
            if not iid: return
            
            filename = getattr(self, '_file_mapping', {}).get(iid)
            if not filename: return
            filepath = self.out_dir / filename
            
            if col == '#6': # open
                if filepath.exists():
                    os.system(f"open -R '{filepath.absolute()}'")
            elif col == '#7': # delete
                if messagebox.askyesno("清理确认", f"确定在本地磁盘彻底销毁以下资产吗？\n\n{filename}"):
                    try:
                        if filepath.exists():
                            os.remove(filepath)
                            self.log_msg(f"数据已物理销毁: {filename}")
                            self.tree.delete(iid)
                    except Exception as e:
                        messagebox.showerror("删除失败", str(e))
                        
    def on_delete_asset(self, event):
        selection = self.tree.selection()
        if not selection: return
        
        if len(selection) == 1:
            iid = selection[0]
            filename = getattr(self, '_file_mapping', {}).get(iid)
            if filename:
                filepath = self.out_dir / filename
                if messagebox.askyesno("清理确认", f"确定删除文件吗？\n\n{filename}"):
                    try:
                        if filepath.exists():
                            os.remove(filepath)
                            self.log_msg(f"数据已物理销毁: {filename}")
                            self.tree.delete(iid)
                    except Exception as e:
                        messagebox.showerror("删除失败", str(e))
        else:
            if messagebox.askyesno("批量清理确认", f"确定批量删除选中的 {len(selection)} 个文件吗？"):
                for iid in selection:
                    filename = getattr(self, '_file_mapping', {}).get(iid)
                    if filename:
                        filepath = self.out_dir / filename
                        try:
                            if filepath.exists():
                                os.remove(filepath)
                                self.tree.delete(iid)
                        except Exception as e:
                            self.log_msg(f"删除 {filename} 时失败: {str(e)}", "err")
                self.log_msg(f"批量物理销毁完成")

    def toggle_dates(self):
        state = tk.DISABLED if self.all_history_var.get() else tk.NORMAL
        for widget in [self.start_y, self.start_m, self.start_d, self.end_y, self.end_m, self.end_d]:
            if state == tk.DISABLED:
                widget.configure(state='disabled')
            else:
                widget.configure(state='readonly') # Modern behavior, no arbitrary typing text needed

    def log_msg(self, msg, level="info"):
        """
        等级路由: 'info' -> 灰色, 'sys' -> 金色标识, 'succ' -> 绿色加亮, 'warn' -> 橘黄, 'err' -> 红色
        如果 msg 中开头自带 `[x]` 或 `[!]` 则可从文本自动推断段落等级。
        """
        # 自动推断
        if msg.startswith("[+]") or msg.startswith("[√]"): level = "succ"
        elif msg.startswith("[!]") or "警告" in msg: level = "warn"
        elif msg.startswith("[-]") or msg.startswith("[x]") or msg.startswith("FATAL"): level = "err"
        elif msg.startswith("[*]") or msg.startswith("[>]"): level = "sys"
        
        self.log_widget.configure(state='normal')
        ts = datetime.datetime.now().strftime("%H:%M:%S")
        self.log_widget.insert(END, f"[{ts}] {msg}\n", level)
        self.log_widget.see(END)
        self.log_widget.configure(state='disabled')

    def load_stock_list_thread(self):
        self.log_msg("[>] 正在建立与 MIANA 核心接口的连接脉络...")
        self.set_progress(10)
        self.status_sign.config(text="连接中...", fg=self.c_gold)
        threading.Thread(target=self._fetch_stock_list, daemon=True).start()

    def _fetch_stock_list(self):
        try:
            ex_map = {"xshg": "sh", "xshe": "sz", "bjse": "bj"}
            all_data, r1, r2 = [], None, None
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            
            r1 = requests.get(MIANA_LIST_URL, params={"token": MIANA_TOKEN, "market": "cn_hs_a", "format": "json"}, headers=headers, timeout=15)
            if r1.status_code == 200:
                if r1.json().get("code") == 200: all_data.extend(r1.json().get("data", []))
            else:
                raise Exception(f"主板接口网关异常 (HTTP {r1.status_code})，远端 MIANA 服务商可能正在维护。")
            
            r2 = requests.get(MIANA_LIST_URL, params={"token": MIANA_TOKEN, "market": "cn_bjs", "format": "json"}, headers=headers, timeout=15)
            if r2.status_code == 200:
                if r2.json().get("code") == 200: all_data.extend(r2.json().get("data", []))
            else:
                raise Exception(f"北交所接口网关异常 (HTTP {r2.status_code})，远端 MIANA 服务商可能正在维护。")

            for item in all_data:
                ex_raw = str(item.get("exchangeCode", "")).lower().strip()
                ex = ex_map.get(ex_raw, ex_raw)
                code = str(item.get("code", "")).strip()
                name = str(item.get("name", "")).strip()
                
                if ex in ("sh", "sz", "bj") and len(code) == 6:
                    symbol = f"{ex}{code}"
                    display_text = f"{name} ({symbol})"
                    self.stock_list.append(display_text)
                    self.stock_dict[display_text] = symbol

            if len(self.stock_list) > 0:
                self.after(0, self._on_stock_list_loaded)
            else:
                self.after(0, lambda: self.status_sign.config(text="核心库获取为空", fg=self.c_gold_dim))
                self.after(0, lambda: self.log_msg("[!] 警告：从接口获取的股票库为空。"))
        except Exception as e:
            self.after(0, lambda: self.status_sign.config(text="连接异常", fg=self.c_red))
            self.after(0, lambda err=e: self.log_msg(f"[-] 网络连接或解析异常失败: {str(err)}", "err"))

    def _on_stock_list_loaded(self):
        self.set_progress(0)
        self.log_msg(f"[+] 下行链路验证完毕。已将 {len(self.stock_list)} 只全市场标的全部加载入本地内存。")
        self.prog_lbl.config(text="状态：正常 - 等待指令输入层级响应")
        self.status_sign.config(text="系统连接正常", fg=self.c_green)

    def on_start_click(self):
        if not self.radar_pool:
            messagebox.showwarning("雷达池为空", "市场雷达池为空，请先在上方输入标的并按回车装填入列。")
            return
        
        self.start_btn.config(state=DISABLED)
        self.all_history_cb.config(state=DISABLED)
        self.pause_btn.config(state=NORMAL, text="挂起当前通道")
        self.stop_btn.config(state=NORMAL)
        
        self.is_paused, self.stop_requested = False, False
        self.log_widget.configure(state='normal')
        self.log_widget.delete('1.0', END)
        self.log_widget.configure(state='disabled')
        
        if self.all_history_var.get():
            start_date, end_date = "1990-01-01 00:00:00", datetime.datetime.now().strftime("%Y-%m-%d 23:59:59")
            self.log_msg("[*] 宏观全局无限制模式已开启: 将为队列中的每一个标的向根节点下探感知...")
        else:
            start_date = f"{self.start_y.get()}-{self.start_m.get()}-{self.start_d.get()} 00:00:00"
            end_date = f"{self.end_y.get()}-{self.end_m.get()}-{self.end_d.get()} 23:59:59"
            self.log_msg(f"[*] 宏观时间抓取任务已入列: [{start_date}] <-> [{end_date}]")
            
        # 复制一份当前的池，避免在中途被 UI 篡改
        tasks_to_run = list(self.radar_pool)
        
        # 启动宏观队列处理器
        self.fetch_thread = threading.Thread(target=self._run_batch_with_catch, args=(tasks_to_run, start_date, end_date), daemon=True)
        self.fetch_thread.start()

    def _run_batch_with_catch(self, queue, start_date, end_date):
        import random
        total_tasks = len(queue)
        try:
            for idx, item in enumerate(queue):
                if self.stop_requested: break
                
                stock_name = item['name']
                symbol = item['symbol']
                
                # 提示宏观进度
                self.after(0, lambda i=idx, t=total_tasks, s=stock_name: self.log_msg(f"\n[>] =========================================\n[>] 宏观列队进度: {i+1} / {t} | 正在分配通道至: {s}\n[>] =========================================", "sys"))
                
                # 阻塞执行单只抽取任务，直到返回
                success = self._fetch_kline_single(stock_name, symbol, start_date, end_date)
                
                # 若人为发起了停止干预，则中断循环
                if self.stop_requested:
                    break
                    
                # 拟人化间歇防限流
                delay = random.uniform(1.5, 4.0)
                self.after(0, lambda d=delay: self.log_msg(f"[*] 跨标的冷却保护: 拟人化随机休眠 {d:.1f} 秒以避开风控..."))
                time.sleep(delay)
                
        except Exception as e:
            self.after(0, lambda err=e: self.__fetch_done(f"[-] 宏观致命异常阻塞了列队系统: {str(err)}", "err"))
        finally:
            # 清理雷达池，任务跑完或人为中断后排空，方便下一次操作
            self.radar_pool.clear()
            self.after(0, self._update_pool_ui)
            
            if self.stop_requested:
                self.after(0, lambda: self.__fetch_done(f"[!] 安全截断响应：未提取完成的列队已被强制抛弃。", None))
            else:
                self.after(0, lambda: self.__fetch_done(f"[+] 宏观列队大循环结束：成功横扫 {total_tasks} 只市场标的。", None))

    def on_pause_click(self):
        self.is_paused = not self.is_paused
        if self.is_paused:
            self.pause_btn.config(text="放开挂起限制")
            self.log_msg("[!] 已将当前数据传输通道强制挂起。")
            self.prog_lbl.config(text="通道挂起中")
            self.status_sign.config(text="进程已冻结", fg=self.c_gold_dim)
        else:
            self.pause_btn.config(text="挂起当前通道")
            self.log_msg("[>] 数据传输通道已重新开放。")
            self.prog_lbl.config(text="流水线抽取推进中...")
            self.status_sign.config(text="数据灌装中...", fg=self.c_green)

    def on_stop_click(self):
        self.stop_requested = True
        self.pause_btn.config(state=DISABLED)
        self.stop_btn.config(state=DISABLED)
        self.log_msg("[!] 收到安全强制熔断指令：正在清理内存缓冲...",)

    def iter_windows(self, start_dt, end_dt, window_days=7):
        cur = start_dt
        step = datetime.timedelta(days=window_days)
        while cur <= end_dt:
            w1 = min(end_dt, cur + step - datetime.timedelta(seconds=1))
            yield cur, w1
            cur = w1 + datetime.timedelta(seconds=1)

    def _determine_actual_start(self, symbol, fallback_start_dt):
        for year in range(1990, 2027):
            if self.stop_requested: return fallback_start_dt
            try:
                r = requests.get(MIANA_KLINE_URL, params={
                    "token": MIANA_TOKEN, "symbol": symbol, "type": "1min",
                    "beginDate": f"{year}-06-01 00:00:00", "endDate": f"{year}-07-01 00:00:00", "limit": "20", "format": "json"
                }, timeout=5)
                if len(r.json().get("data", [])) > 0:
                    self.after(0, lambda y=year: self.log_msg(f"[+] 成功下探到数据根偏移起点所在年份: {y} 年。"))
                    return pd.to_datetime(f"{year}-01-01 00:00:00")
            except: pass
        return fallback_start_dt

    def _fetch_kline_single(self, stock_name, symbol, start_str, end_str):
        """核心业务逻辑：抓取【单只标的】，返回 bool。独立不持有收尾 UI 控制权"""
        self.after(0, lambda: self.status_sign.config(text="数据灌装中...", fg=self.c_green))
        start_dt = pd.to_datetime(start_str)
        end_dt = pd.to_datetime(end_str)
        
        if self.all_history_var.get():
            start_dt = self._determine_actual_start(symbol, start_dt)
        
        total_days = (end_dt - start_dt).days + 1
        if total_days <= 0:
            self.after(0, lambda: self.log_msg(f"[-] 错误：目标 [{stock_name}] 选择的时间边界无效", "err"))
            return False

        self.after(0, lambda: self.prog_lbl.config(text=f"正在深度下钻提取扇区: {stock_name}"))
        self.after(0, lambda: self.log_msg(f"[>] 开始深度循环，计算游标步长：单次偏移 7 天"))
        
        frames, curr_days_done = [], 0

        import random
        for w0, w1 in self.iter_windows(start_dt, end_dt, window_days=7):
            while self.is_paused and not self.stop_requested: time.sleep(0.5)
            if self.stop_requested: break
            
            retry_count = 0
            max_retries = 3
            success_fetch = False
            
            while retry_count < max_retries and not self.stop_requested:
                try:
                    r = requests.get(MIANA_KLINE_URL, params={
                        "token": MIANA_TOKEN, "symbol": symbol, "type": "1min", "order": "ASC",
                        "beginDate": w0.strftime("%Y-%m-%d %H:%M:%S"),
                        "endDate": w1.strftime("%Y-%m-%d %H:%M:%S"),
                        "fq": "qfq", "format": "json", "limit": "2000"
                    }, timeout=10)
                    
                    if r.status_code != 200:
                        if r.status_code in [429, 502, 503, 504]:
                            retry_count += 1
                            backoff = (2 ** retry_count) + random.uniform(0.5, 2.0)
                            self.after(0, lambda rc=retry_count, sc=r.status_code, b=backoff: self.log_msg(f"[!] 触发远端限流风控 (HTTP {sc})，启用第 {rc} 次指数退避 (深度休眠 {b:.1f}s)...", "warn"))
                            time.sleep(backoff)
                            continue
                        else:
                            raise Exception(f"行情网关拒绝访问 (HTTP {r.status_code})，异常拦截机制生效。")
    
                    data_rows = r.json().get("data", [])
                    if r.json().get("code") == 200 and data_rows:
                        frames.append(pd.DataFrame(data_rows))
                        st, en = data_rows[0].get('date', ''), data_rows[-1].get('date', '')
                        self.after(0, lambda ct=len(data_rows), d1=st, d2=en: self.log_msg(f"[+] 下行包校验通过: [{d1} -> {d2}] 收拢 {ct} 条实体数据"))
                    else:
                        pass # Silently skip null zones
                        
                    success_fetch = True
                    break
                except requests.exceptions.Timeout:
                    retry_count += 1
                    b = (2 ** retry_count) + random.uniform(0.5, 2.0)
                    self.after(0, lambda rc=retry_count, b=b: self.log_msg(f"[!] 远端下行丢包超时，启用第 {rc} 次指数退避 (深度休眠 {b:.1f}s)...", "warn"))
                    time.sleep(b)
                except Exception as e:
                    self.after(0, lambda err=e: self.log_msg(f"[!] 发现致命网络穿透异常: {str(err)}", "err"))
                    break
            
            if not success_fetch and retry_count >= max_retries:
                self.after(0, lambda: self.log_msg("[x] 连续重试被拒，当前切片防反扒机制失败，抛弃并向后推进游标...", "err"))
                
            curr_days_done += 7
            pct = min(100, int((curr_days_done / total_days) * 100))
            self.after(0, lambda p=pct: self.set_progress(p))
            
            # 单次切片安全冷却
            time.sleep(random.uniform(0.2, 0.6))

        if not frames:
            self.after(0, lambda: self.log_msg(f"[!] 警告：请求的时空中未发现 {stock_name} [{symbol}] 的任何K线实体数据", "warn"))
            return False

        self.after(0, lambda: self.log_msg("[*] 开始重组和封装降维数据..."))
        df = pd.concat(frames, ignore_index=True)
        ren = {"date": "dt", "openPrice": "open", "highPrice": "high", "lowPrice": "low", "closePrice": "close", "price": "close", "vol": "volume", "turnover": "amount", "amt": "amount"}
        for k, v in ren.items():
            if k in df.columns and v not in df.columns: df[v] = df[k]
                
        df = df.dropna(subset=['dt', 'close']).sort_values("dt").drop_duplicates("dt", keep="last").reset_index(drop=True)
        df = df[[c for c in ["dt", "open", "high", "low", "close", "volume", "amount"] if c in df.columns]]

        act_start = pd.to_datetime(df['dt'].iloc[0]).strftime("%Y%m%d")
        act_end = pd.to_datetime(df['dt'].iloc[-1]).strftime("%Y%m%d")
        filename = f"{stock_name}_{symbol}_1m_{act_start}_to_{act_end}.csv"
        df.to_csv(self.out_dir / filename, index=False)
        
        # --- DATA INTEGRITY CHECK (数据连续性安全复核) ---
        self.after(0, lambda: self.log_msg(f"[*] 启动本地质量复核：正在扫描时间序列连续性断层..."))
        try:
            df['dt_obj'] = pd.to_datetime(df['dt'])
            # 找出所有的日期间隔
            date_diffs = df['dt_obj'].diff()
            # 如果中间有超过 10 天的断层（比如停牌、退市再上市，或者 Miana 接口漏数据）
            gaps = df[date_diffs > pd.Timedelta(days=10)]
            if not gaps.empty:
                max_gap = date_diffs.max().days
                self.after(0, lambda mg=max_gap: self.log_msg(f"[!] 连续性警告：复核发现该序列内存在长达 {mg} 天的断层！请检查是否为长期停牌。如果异常，建议之后扔进 Slicer 前单独筛选。", "warn"))
            else:
                self.after(0, lambda: self.log_msg(f"[+] 复核通过：未发现长期断层数据丢失，序列连续性良好。", "succ"))
        except Exception as e:
             self.after(0, lambda err=e: self.log_msg(f"[-] 连续性扫描异常: {str(err)}", "warn"))
        
        self.after(0, lambda: self.set_progress(100))
        self.after(0, lambda: self.log_msg(f"[+] 单标的资产已封装落盘完毕 ({stock_name}, 尺寸: {len(df)} 行)", "sys"))
        return True

    def on_closing(self):
        self.stop_requested = True
        self.destroy()

    def __fetch_done(self, msg, style):
        self.start_btn.config(state=NORMAL)
        # self.search_entry.config(state=NORMAL)
        self.all_history_cb.config(state=NORMAL)
        self.pause_btn.config(state=DISABLED)
        self.stop_btn.config(state=DISABLED)
        
        self.status_sign.config(text="系统连接正常", fg=self.c_green)
        self.prog_lbl.config(text=msg.split('\n')[0])
        self.log_msg(msg)

if __name__ == "__main__":
    app = KlineDataFetcherGUI()
    app.mainloop()
