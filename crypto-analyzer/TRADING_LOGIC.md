# 开仓逻辑文档

> 最后更新：2026-04-13

---

## 一、整体流程图

```
trade_scheduler.py（每12小时触发一轮）
  │
  ├─► discovery_trader.py   ──► Big4（BTC/ETH/BNB/SOL）
  │         │
  │         └─► PatternDiscoveryAgent
  │                   │
  │                   ├─► MarketContextBuilder（数据采集）
  │                   └─► Gemini 2.5 Flash（推理）
  │
  └─► top50_trader.py       ──► 历史盈利 Top50 交易对
            │
            └─► PatternDiscoveryAgent（每批6个，分批调用）
```

---

## 二、调度层（trade_scheduler.py）

| 参数 | 值 |
|---|---|
| 运行间隔 | 每 **12 小时** 一轮 |
| 启动方式 | `nohup python trade_scheduler.py --no-first` |
| Big4 预热时间 | 300秒（等 discovery_trader 完成开仓后再启动 top50） |

每轮顺序：
1. 启动 `discovery_trader.py`（Big4）
2. 等待 5 分钟
3. 启动 `top50_trader.py --batch-size 4`

---

## 三、数据采集层（MarketContextBuilder）

每次分析前，为每个标的拉取以下 **12 个维度**的实时数据：

### 单标的维度
| 维度 | 数据内容 |
|---|---|
| K线结构 | 1h/4h/1d K线，开高低收，EMA趋势 |
| 价格坐标 | 关键支撑/阻力位（1h/4h/1d 枢轴点） |
| 动量指标 | RSI、MACD、EMA多空排列 |
| 成交量 | Taker买入比例、量价背离 |
| 资金费率 | 当前费率、资金费率历史分布（百分位） |
| 持仓量 | OI变化趋势、OI与价格的背离 |
| 最近强平 | 过去1小时该标的的强平方向和金额 |
| **资金费率速度** | 费率变化速度 + 加速度（是否在加速收敛/发散）|
| **Hyperliquid智能钱** | 专业交易者净流入/流出、多空比、胜率、PnL |
| **跨交易所溢价** | Binance vs OKX vs Bybit 价差百分比 |

### 跨资产维度
| 维度 | 数据内容 |
|---|---|
| 相关性矩阵 | 各币与BTC的1h相关系数 |
| 领先滞后关系 | BTC 1-3小时领先信号 |
| ETH/BTC比值 | 资金轮动信号 |
| **全网强平热力图** | 过去1小时全市场强平金额（多头/空头方向） |
| **恐惧贪婪指数** | Alternative.me F&G Index，过去3天趋势 |

---

## 四、Gemini 推理层（PatternDiscoveryAgent）

### 调用方式
- 模型：**Gemini 2.5 Flash**（temperature=0.7，最大8192 tokens）
- 输出格式：强制 JSON（`response_mime_type: application/json`）

### 输入给 Gemini 的内容
完整的市场上下文（约 10,000-15,000 字符），包含上述所有维度的结构化数据，
加上以下任务指令：

```
为每个标的生成一个24小时交易计划，包含：
- 具体入场区间（low/high）
- 止损价（放在最近支撑/阻力位之外）
- 止盈1 / 止盈2
- 胜率估算（基于信号数量叠加，非直觉）
- 置信度（1-10）
- 无效化条件（何时放弃此计划）
```

### Gemini 胜率估算规则

| 信号数量 | 对应胜率 |
|---|---|
| 1个信号（如 RSI 超卖）| 52-55% |
| 2个信号共振 | 55-62% |
| 3个信号共振 | 62-70% |
| 4个以上共振 | 68-75% |
| 极端情绪反转 + 多重支撑 | 70-78% |
| 信号矛盾 | 降低胜率，输出 SKIP |

### Gemini 输出 JSON 结构

```json
{
  "trade_plans": [
    {
      "symbol": "BTC",
      "direction": "LONG / SHORT / SKIP",
      "entry_zone": { "low": 83000, "high": 83500 },
      "stop_loss": 82000,
      "target1": 85000,
      "target2": 87000,
      "win_rate_pct": 68,
      "risk_reward": 2.1,
      "confidence": 7,
      "time_window": "enter within 4h, hold 12-24h",
      "entry_trigger": "1h close above EMA20",
      "invalidation": "1h close below 82500",
      "confluence_signals": ["signal1", "signal2", "signal3"],
      "win_rate_basis": "why this win rate"
    }
  ],
  "market_regime": "trending_up",
  "cross_asset_insight": "...",
  "highest_conviction_trade": "BTC"
}
```

---

## 五、开仓执行层

### 开仓参数（固定）

| 参数 | 值 |
|---|---|
| 每笔保证金 | **1000 USDT** |
| 杠杆 | **5倍** |
| 名义仓位 | 5000 USDT |
| 止损 | 来自 Gemini 信号的 `stop_loss` |
| 止盈 | 来自 Gemini 信号的 `target1` |
| 最大持仓时间 | **12小时**（`max_hold_minutes=720`） |
| 来源标记 | `source = "discovery_trader"` 或 `"top50_trader"` |

### 跳过开仓的情况
- 该标的已有开仓（避免重复）
- Gemini 输出 `direction = "SKIP"`
- 当前总持仓已满（最大 20 仓）

### discovery_trader vs top50_trader 区别

| | discovery_trader | top50_trader |
|---|---|---|
| 标的 | BTC/ETH/BNB/SOL（固定4个） | top_performing_symbols 前50 |
| 每批分析数量 | 4个（一次调用 Gemini） | 4个/批，分批循环 |
| 并发策略 | 先运行，占好 Big4 仓位 | 等 discovery 完成后再运行 |

---

## 六、持仓管理层（Smart Exit Optimizer）

### discovery/top50 仓位的特殊规则

这两类仓位**跳过所有智能平仓逻辑**，只响应以下三种关闭条件：

| 条件 | 触发方式 |
|---|---|
| **止损** | 价格触达 Gemini 给出的 stop_loss 价格 |
| **止盈** | 价格触达 Gemini 给出的 target1 价格 |
| **12小时到期** | 持仓满 720 分钟，强制平仓 |

被跳过的智能逻辑：K线强度衰减、动态超时、回撤保护、震荡市检测等。

### 其他来源（smart_trader）仓位
- 走完整的智能平仓流程（8个优先级检查，每秒一次）

---

## 七、标的池来源（top_performing_symbols）

- 来源：系统历史所有交易的 PnL 统计
- 更新频率：每天凌晨 2 点
- 排名依据：`rank_score`（历史总盈利额）
- 总数：固定 50 个
- **不是市值前50，是我们策略历史上赚钱最多的50个**

当前前10名（截至 2026-04-13）：

| 排名 | 标的 | 历史PnL | 胜率 |
|---|---|---|---|
| 1 | BTC/USDT | +983 USDT | 35.6% |
| 2 | BERA/USDT | +892 USDT | 54.5% |
| 3 | LAYER/USDT | +740 USDT | 56.0% |
| 4 | AVNT/USDT | +723 USDT | 52.3% |
| 5 | TRUMP/USDT | +705 USDT | 64.0% |
| 6 | COMP/USDT | +614 USDT | 62.7% |
| 7 | FIL/USDT | +606 USDT | 54.2% |
| 8 | JUP/USDT | +602 USDT | 48.5% |
| 9 | PIPPIN/USDT | +580 USDT | 48.6% |
| 10 | LINEA/USDT | +533 USDT | 54.2% |

---

## 八、数据采集服务（后台常驻）

| 服务 | 采集内容 | 更新频率 |
|---|---|---|
| fast_collector | K线、资金费率、持仓量 | 每分钟 |
| sentiment_collector | 恐惧贪婪指数 | 每小时 |
| cross_exchange_collector | OKX/Bybit 跨交易所价差 | 每分钟 |
| global_liquidation_collector | 全网强平（Binance WebSocket） | 实时 |
| hyperliquid_scheduler | Hyperliquid 智能资金数据 | 定时 |
| watchdog | 系统健康监控 | 常驻 |

---

## 九、一轮完整交易流程（时序）

```
T+0:00  trade_scheduler 触发
T+0:01  discovery_trader 启动
T+0:02  MarketContextBuilder 拉取 BTC/ETH/BNB/SOL 数据（约 6s）
T+0:02  Gemini 推理（约 30-40s）
T+0:03  解析 JSON，过滤 SKIP
T+0:04  按信号开仓，设置 SL/TP，标记 source=discovery_trader，max_hold=720min
T+0:05  discovery_trader 进入监控循环（每5分钟检查一次仓位状态）

T+5:00  top50_trader 启动（等 discovery 完成后）
T+5:xx  分批处理 top_performing_symbols（每批4个）
        - 每批：MarketContextBuilder 拉取 + Gemini 推理 + 开仓
        - 已有仓位的标的跳过
        - 仓位满 20 个时停止

T+12:00 所有 discovery/top50 仓位到期
        - smart_exit_optimizer 检测到 hold_minutes >= 720
        - 调用 /api/futures/close/{id} 强制平仓
        - 记录 realized_pnl 到 futures_positions 表

T+12:00 trade_scheduler 触发下一轮
```
