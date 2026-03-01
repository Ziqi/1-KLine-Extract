# KLine Matrix Station 💻📈
## 全市场一分钟 K线数据清洗基站 (极客暗金版)

![KLine Matrix Station UI preview](https://via.placeholder.com/800x500.png?text=KLine+Matrix+Station+-+Geek+Dark+Theme)

**KLine Matrix Station** 是一款专力量身定制、沉浸感拉满的桌面级 Python 爬虫与数据融合终端。它抛弃了传统枯燥的窗口交互，以 **“Flat Dark Gold (极客暗金)”** 作为视觉核心，重塑了针对全市场沪深北及场外 ETF 股票 1 分钟级高频K线数据的批量拉取、规整落地流程。

只需轻点按钮或使用神奇的智能嗅探功能，百万级的 K 线数据流便会自动流向本地化的高规格清洗管道。

---

### ✨ 核心黑科技 (Core Features)

- **[ 沉浸式界盒交互 ]**：全局替换系统默认 UI。采用 Canvas 自绘的“科技破壳虚线 (Dashed Bounds)”，辅以**自动浮现避让式隐形滚动条**，纯黑背景配以高饱暗金字符，呈现骇客帝国级的终端实操体验。
- **[ 剪贴板闪电嗅探阵列 (Clipboard Sniffer) ]**：这是效率的神迹！无需繁琐的输入，你可以直接在网文、研报中复制一段杂乱无章的文字，如：“*久立特材想买一点，兆易创新，兴发集团这几个，还有华鲁恒生（名字都打错了也能认），中微，以及四川美丰*”。按下按钮，基站内置的词汇分析引擎及 `difflib` 模糊匹配容错算法将在一秒内提取出合法标的、猜测 ETF，并挂载入多核列队。
- **[ 宏观雷达列队 (Market Radar Pool) ]**：基于内存级的任务暂存池。所有侦测到的目标资产会被押入列队，全盘执行期间支持随时挂起/断流熔断，且对存在历史留存文件的同名资产设有“文件覆写死锁预警”。
- **[ 物理硬盘直击清理 ]**：内置资产快查面板。鼠标停留即可实时看到落盘数据文件，一键呼出物理源系统目录定位或者进行文件实体彻底擦除。

### ⚙️ 依赖环境环境 (Prerequisites)

- **OS**: macOS / Linux / Windows 均可完美运行 (使用 macOS 系统级打包 `osacompile` 支持原生 `.app` 化体验更佳)
- **Python**: 3.9+
- **关键架构包**:
  ```bash
  pip install requests pandas ttkbootstrap 
  ```

### 🚀 启动指引 (Quick Start)

1. `git clone https://github.com/Ziqi/KLine-Matrix-Station.git`
2. 进入目录目录下: `cd KLine-Matrix-Station`
3. 确保安装好前置包依赖。
4. 执行引擎入口：`python gui_fetch_kline.py`
5. *(仅 macOS 极客推荐)* 你可以通过原生的 AppleScript 套壳将其固化成一个双击即开的桌面伪原生应用：
   `osacompile -o "KLine Matrix Station.app" -e 'do shell script "cd [你的目录] && nohup [python解释器路径] gui_fetch_kline.py > /tmp/gui_kline_app.log 2>&1 &"'`

### 🛡️ 架构流管设计 (Pipeline Logic)

终端执行单只股票的深度下探时，内置以“单次偏移 7 天”的步进环切块抓取，实时监测返回的下位机封包长度，严格容忍单次 API 抖动或 MIANA 远端大盘宕机（拥有自动防 502 Bad Gateway 弹窗拦截能力）。最终利用 `pandas` 滤空合并对齐至秒级维度，落地为纯物理硬盘的高精尖清洗样本 CSV。

### 📜 免责声明
本库所依赖的核心底层 API (`miana.com.cn`) 为第三方供给，本基站仅提供调度、美化展现、过滤分发等纯前端能力，不对任何因原始接口变动导致的数据失真负责，也不构成任何金融交易指导参考。

---
`Author: Ziqi` | `License: MIT` | `Design Language: Cyber-Gold`
