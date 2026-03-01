# KLine Matrix Station 💻📈
## 全市场一分钟 K线数据清洗基站 (极客暗金版) / 1-Minute K-Line Matrix Station

![KLine Matrix Station UI preview](https://via.placeholder.com/800x500.png?text=KLine+Matrix+Station+-+Geek+Dark+Theme)

---

## 🇨🇳 中文说明 (Chinese)

**KLine Matrix Station** 是一款专为量身定制、沉浸感拉满的桌面级 Python 爬虫与数据融合终端。以 **“Flat Dark Gold (极客暗金)”** 作为视觉核心，重塑了针对全市场（沪深北及场外 ETF）股票 1 分钟级高频 K 线数据的批量拉取、规整落地流程。

### ✨ 核心黑科技
- **[ 极客暗金 UI 交互 ]**：全局替换系统默认 UI，采用 Canvas 自绘的边界框，配备自动浮现避让式隐形滚动条，纯黑背景配以高饱暗金字符，呈现骇客帝国级的终端实操体验。
- **[ 剪贴板闪电嗅探阵列 (Clipboard Sniffer) ]**：无需繁琐的输入，你可以直接在网文、研报中复制一段杂乱无章的文字，例如：“久立特材想买一点，兆易创新，兴发集团这几个，还有华鲁恒生，以及四川美丰”。按下嗅探按钮，基站内置的分析引擎将在一秒内提取出合法标的并挂载入多核列队。
- **[ 宏观雷达列队 (Market Radar Pool) ]**：支持 Shift 多选分离，全盘执行期间支持随时挂起/断流熔断，且对存在历史留存文件的同名资产设有“文件覆写预警”。最新版本支持显示文件物理大小和一键物理销毁。
- **[ 军工级防封杀引擎 (Anti-Ban Engine) ]**：内置 `random` 拟人化抖动以及针对 HTTP 429/502 的三段式指数退避重试 (Exponential Backoff) 防封策略。

### 🔌 开发者指南：替换默认的 API 数据源
本项目默认使用 MIANA 作为数据提取源。如果您希望使用其他供应商的 API 数据，请按照以下步骤更改封装格式和解析点：

#### 1. 了解期望的数据 JSON 格式
如果您更换了数据源，请将您的 API 返回数据在中间件层转换为与 MIANA 相似的 JSON 树结构，核心在于 `data` 数组节点：
```json
{
  "code": 200,
  "data": [
    {
      "date": "2024-01-01 09:31:00",
      "open": "10.01",
      "high": "10.05",
      "low": "9.98",
      "close": "10.02",
      "volume": "100000",
      "amount": "1005000"
    }
  ]
}
```

#### 2. 修改源码中的 API 对接点
为了方便全球开发者查阅和魔改，您只需要在 `gui_fetch_kline.py` 文件中修改两处即可：
- **全局变量定义区 (行 16 附近)**: 替换 `MIANA_KLINE_URL` 和 `MIANA_TOKEN`。
- **解析层提取循环 `_fetch_kline_single` (行 800+)**:
  系统会使用 `requests.get` 发送带有 `symbol`, `beginDate`, `endDate`, `type=1min` 的请求。如果您的第三方 API 传参不同，请在此处修改 `params={...}`。
  同时，数据解析的核心阵列位于：
  ```python
  data_rows = r.json().get("data", [])
  ```
  只要您的第三方 API 能够解析出一个包含上述字典字段的 `data_rows` 列表，整个后端的防封杀重试、多线程队列、进度条渲染和存盘引擎均完美兼容！

### 📊 数据接口规范 (Data IO Specs)
- **输入 (Input)**: 自然语言混杂的 A 股公司名称或代码文本（支持剪贴板嗅探）。
- **输出 (Output)**: 标准化的一分钟 K 线历史数据文件（`.csv`）。
  - **文件命名规则**: `[股票简称]_[代码]_1m_[开始日期]_to_[结束日期].csv`
  - **核心字段列表**: `timestamps`, `open`, `high`, `low`, `close`, `volume`, `amount`
- **设计初衷**: 作为整个链路的起源，直接对接云端 API 进行最细粒度的原始提纯，为后续的时间序列量化网络提供绝对纯度的高频预切片流，规避传统 API 的并发和限制痛点。

---

## 🇺🇸 English Documentation

**KLine Matrix Station** is a highly customized, immersive desktop-level Python spider and data integration terminal. It utilizes a **"Flat Dark Gold"** aesthetic to reshape the process of batch-pulling and formatting 1-minute high-frequency K-line data for global markets (including ETFs).

### ✨ Core Features
- **[ Immersive Matrix UI ]**: Completely replaces standard system UI. Features dashed boundaries, auto-hiding invisible scrollbars, and a pure black background with highly saturated dark-gold characters for a cyberpunk "Matrix" operational experience.
- **[ Clipboard Sniffer ]**: No manual typing required. Directly copy messy text or reports (e.g., "I want to buy some Apple, Microsoft, and maybe TSLA"). Click the sniffer button, and the built-in natural language engine will extract valid stock tickers within a second and mount them into the multi-core queue.
- **[ Market Radar Pool ]**: Supports Shift multi-selection, pause/resume during execution, and file-overwrite warnings for existing local assets.
- **[ Military-Grade Anti-Ban Engine ]**: Built-in randomized human-like throttling and a three-stage Exponential Backoff retry strategy for HTTP 429/502 to evade WAF rate limits.

### 🔌 Developer Guide: Customizing the API Data Source
This project uses MIANA as the default data source. If you want to use other API providers, please format the data and modify the API endpoint as follows:

#### 1. Expected JSON Data Format
If you change the data source, please parse your API response into a JSON structure similar to this (focusing on the `data` array):
```json
{
  "code": 200,
  "data": [
    {
      "date": "2024-01-01 09:31:00",
      "open": "10.01",
      "high": "10.05",
      "low": "9.98",
      "close": "10.02",
      "volume": "100000",
      "amount": "1005000"
    }
  ]
}
```

#### 2. Where to Modify the Source Code
For global developers, you only need to modify two locations in the `gui_fetch_kline.py` file to replace the engine:
- **Global Variables (around Line 16)**: Replace `MIANA_KLINE_URL` and `MIANA_TOKEN`.
- **Parser Loop `_fetch_kline_single` (around Line 800+)**:
  The system uses `requests.get` to construct parameters like `symbol`, `beginDate`, `endDate`, and `type=1min`. If your 3rd-party API requires different parameters, modify the `params={...}` dict here.
  The core data extraction logic expects the list:
  ```python
  data_rows = r.json().get("data", [])
  ```
  As long as your custom API is converted to return a list of dictionaries matching these fields, the built-in throttling, queue system, and CSS persistence engine will work out of the box!

---
`Author: Ziqi` | `License: MIT` | `Design Language: Cyber-Gold`
