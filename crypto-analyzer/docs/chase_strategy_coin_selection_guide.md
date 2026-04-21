# 追击策略选币操作指引

> 适用场景：市场处于高波动行情（单日涨跌>20%的标的明显增多），需要快速筛选可追击标的并验证可行性。

---

## 第一步：思路原型

在正式分析之前，先用一句话描述你的直觉假设。这一步不需要数据，只需要把市场观察转化为可测试的命题。

**示例原型（本次实际使用）：**

> "行情暴涨暴跌，刚刚拉升了大幅度（60%+）但还没完全崩塌（峰值回落<30%）的币，是做空的好时机——它的多头已被消耗，反转动能积聚，但还没到猎人们出逃的地步。"

> "反过来，短期（4h）暴跌15%+的标的，空方动能尚未耗尽，可以直接追空入场。"

**记录思路原型的要素：**

| 要素 | 示例 |
|------|------|
| 方向 | 做空 / 做多 |
| 触发条件 | 48h涨幅>=60% |
| 入场时机 | 从峰值回落<30%（尚未崩塌） |
| 持仓逻辑 | 多头能量耗尽，反转概率高 |
| 风控边界 | 若继续新高则离场（SL=12%） |

---

## 第二步：完整细化分析

将思路原型分解为可量化的参数，同时考虑边界条件和失效场景。

### 2.1 参数化触发条件

**顶部做空（Top Short）：**
```
lookback_period  = 48h
pump_threshold   = 60%~80%（从区间低点到峰值）
drawdown_max     = 30%（从峰值到当前价格的最大容许回落）
confirmation     = 最近6h内无新高（确认动能衰竭）
entry            = 当前价格（市价入场）
sl_pct           = 12%
tp_pct           = 12%
max_hold         = 24h
```

**追击做多（Chase Long）：**
```
trigger_window  = 4h（48根5m K线）
pump_threshold  = 20%（窗口内净涨幅）
entry           = 实时价格
sl_pct          = 8%（触发后翻空）
tp_start        = 5% -> 梯度递增至10%
max_hold        = 8h
cooldown        = 4h（DONE状态后）
```

**追击做空（Chase Dump / 追跌）：**
```
trigger_window  = 4h（48根5m K线）
dump_threshold  = 15%（窗口内净跌幅）
entry           = 实时价格
sl_pct          = 8%（触发后翻多）
tp_start        = 5% -> 梯度递增至10%
max_hold        = 8h
cooldown        = 4h
```

### 2.2 边界条件与失效场景

| 场景 | 说明 | 处理方式 |
|------|------|----------|
| 假突破 | 价格短暂到达阈值后快速回撤 | 要求信号bar在3min内（`bar_age<180s`） |
| 新高延续 | 做空后标的继续上涨 | SL止损；顶空确认6h无新高再入场 |
| 流动性不足 | 小币量小价差大 | FOCUS列表仅保留有持续K线数据的品种 |
| 重复开单 | 策略重启后对同一标的重复入场 | 启动时`_sync_state`从API同步已有仓位 |
| 双向冲突 | 追多和追跌同时对同一标的触发 | `dump_tick`检测到`chase`有持仓时跳过 |

---

## 第三步：寻找数据支撑，筛选目标标的

### 3.1 数据来源

- **本地K线数据库**：`kline_data` 表（MySQL），timeframe=`5m`/`1h`，数据延迟<1min
- **实时价格**：Binance REST API `/api/v3/ticker/price`（通过本地 FastAPI 代理 `/api/futures/price/{sym}`）
- **成交量数据**：`taker_buy_base_volume`（主动买入量，用于判断多空主导）

### 3.2 筛选脚本（顶部做空场景）

```python
# 伪代码逻辑 (参考 strategy_top_short.py / strategy_live.py)
for sym in all_symbols:
    bars_1h = get_klines(sym, '1h', lookback=48)
    low  = min(bar.low for bar in bars_1h)
    peak = max(bar.high for bar in bars_1h)
    curr = get_realtime_price(sym)

    pump    = (peak - low) / low        # 启动到峰值涨幅
    drawdown = (peak - curr) / peak      # 从峰值的回落幅度

    if pump >= 0.60 and drawdown < 0.30:
        candidates.append({sym, pump, drawdown, curr})
```

**实际执行结果示例（2026-04-19）：**

| 标的 | 启动涨幅 | 从峰值回落 | 入场价 | 备注 |
|------|---------|----------|--------|------|
| GIGGLE/USDT | ~80% | ~20% | 市价 | 04-19加入FOCUS |
| BROCCOLI714/USDT | ~60% | ~25% | 市价 | 04-19加入FOCUS |
| ALPACA/USDT | ~200%+ | ~40% | 市价 | 04-18高波动批次 |
| RAVE/USDT | ~100%+ | ~30% | 市价 | 04-18高波动批次 |

### 3.3 筛选脚本（追击策略 - 回测验证）

运行回测脚本验证参数的历史表现：

```bash
# 追多/追空双向回测（基础版）
python strategy_chase.py

# 加入入场质量过滤器（买入率、连续性）
python strategy_chase2.py

# 纯追跌回测（对比追多）
python strategy_chase_short.py
```

**回测关键指标：**
- 全品种平均总收益（等权）
- 多头TP次数 vs 空头TP次数
- RUIN次数（方向判断错误率）
- 参数敏感度（不同RUIN_PCT x PUMP_PCT矩阵）

---

## 第四步：反馈测试标的效果

### 4.1 回测结果评估标准

| 指标 | 达标线 | 说明 |
|------|--------|------|
| 全品种平均收益 | >+10% | 等权平均，7天回测窗口 |
| RUIN率 | <40% | RUIN次数/总交易次数 |
| 追多空TP比 | >2:1 | 追多成功次数显著多于RUIN后追空 |
| 最差品种损失 | >-20% | 单品种最大损失可控 |

### 4.2 实际回测样例（strategy_chase.py输出格式）

```
追击策略回测  RUIN=8%  TP_START=5%  TP_MAX=10%
入场条件: 12根5m内涨>=4%
============================================================
  品种                  总收益     多TP               空TP      RUIN
  NEIRO/USDT          +42.3%  多TP=8次(+38%)   空TP=3次(+14%)  RUIN=2次 <<<
  ENJ/USDT            +18.1%  多TP=5次(+22%)   空TP=2次(+9%)   RUIN=3次
  ALPACA/USDT         +55.2%  多TP=11次(+52%)  空TP=4次(+18%)  RUIN=2次 <<<
```

### 4.3 加入config.yaml的标准

满足以下条件的标的可加入监控列表：
1. 回测7天收益 > +10%（或跌势明显验证为做空候选）
2. 近3天有连续K线数据（`kline_data`表中有记录）
3. 24h成交量足够（避免流动性风险）

```yaml
# config.yaml 追加位置（按批次注释）
# 高波动新增 (MM-DD, 24h涨跌>=25%)
- NEWCOIN/USDT
```

---

## 第五步：接入现有策略进行测试交易

### 5.1 接入流程

```
回测通过
  ↓
加入 config.yaml symbols 列表（触发K线采集）
  ↓
重启 fast_collector_service.py（开始实时采集数据）
  ↓
加入 strategy_live.py FOCUS 列表
  ↓
重启 strategy_live.py（自动同步持仓，不丢失已开单）
```

### 5.2 手动验证K线数据已就绪

```bash
# 确认新标的已有K线数据
mysql -u root binance-data -e "
  SELECT symbol, COUNT(*) as bars, 
         FROM_UNIXTIME(MAX(open_time)/1000) as latest
  FROM kline_data WHERE timeframe='5m'
    AND symbol='NEWCOIN/USDT'
  GROUP BY symbol;"
```

### 5.3 手动开单（模拟盘先验证）

如果等不及策略自动触发，可以直接调用API手动开单：

```bash
curl -X POST http://localhost:9021/api/futures/open \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": 2,
    "symbol": "NEWCOIN/USDT",
    "position_side": "SHORT",
    "quantity": 100,
    "leverage": 5,
    "stop_loss_price": 1.12,
    "take_profit_price": 0.88,
    "max_hold_minutes": 1440,
    "source": "manual:topshort"
  }'
```

### 5.4 策略对应账户

| 账户ID | 用途 |
|--------|------|
| 1 | 模拟盘（dimension_trader 主力账户） |
| 2 | 模拟盘（strategy_live 追击策略专用） |

**原则：先模拟盘跑至少24h，有盈利记录后再考虑实盘。**

---

## 第六步：实际测试反馈与复盘

### 6.1 查看交易历史

访问前端复盘页面：`http://localhost:9020/futures` -> 复盘分析

或直接查询DB：

```sql
-- 按信号来源统计盈亏
SELECT
    SUBSTRING_INDEX(source, ':', 2) AS strategy,
    position_side,
    COUNT(*) AS trades,
    ROUND(SUM(realized_pnl), 2) AS total_pnl,
    ROUND(AVG(realized_pnl), 2) AS avg_pnl,
    ROUND(SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS win_rate_pct
FROM futures_positions
WHERE account_id = 2 AND status = 'closed'
  AND source LIKE 'strategy_live:%'
GROUP BY strategy, position_side
ORDER BY total_pnl DESC;
```

### 6.2 复盘指标

| 指标 | 说明 |
|------|------|
| 胜率 | 盈利单/总单，追击策略预期50%~65% |
| 盈亏比 | 平均盈利/平均亏损，需>1.5 |
| 最大连续亏损 | 连续RUIN次数，>3次需检查参数 |
| 实际vs回测偏差 | 实盘收益应在回测的60%~120%区间内 |

### 6.3 参数调整决策树

```
胜率 < 40% AND 连续RUIN > 3次
  -> 提高触发阈值（PUMP_PCT/DUMP_PCT）
  -> 或将该标的移出FOCUS

胜率 > 60% AND 每笔盈利偏小
  -> 降低TP_START（提前锁定，如从5%降到3%）
  -> 或增加MARGIN

策略长期无信号
  -> 降低触发阈值（PUMP_PCT/DUMP_PCT）
  -> 或扩充FOCUS品种列表
```

### 6.4 实战结果记录（模板）

| 日期 | 标的 | 方向 | 入场价 | 出场价 | 盈亏% | 触发原因 | 备注 |
|------|------|------|--------|--------|-------|----------|------|
| 04-19 | ORDI/USDT | SHORT | 16.2 | 15.1 | +6.8% | topshort-48h涨80%+回落25% | 首批顶空测试 |
| 04-19 | RAVE/USDT | SHORT | 0.082 | 0.071 | +13.4% | topshort-暴涨后回落 | 成功 |
| 04-19 | HIGH/USDT | SHORT | 4.35 | 3.92 | +9.9% | topshort | 成功 |

---

## 快速参考

### 当前策略文件对照

| 文件 | 用途 |
|------|------|
| `strategy_chase.py` | 追多+翻空回测（基础版） |
| `strategy_chase2.py` | 追多回测（加入场质量过滤） |
| `strategy_chase_short.py` | 纯追空回测（对比分析） |
| `strategy_live.py` | 实盘运行器（追多+追跌+顶空三合一） |
| `strategy_top_short.py` | 手动顶部做空工具（一次性脚本） |
| `strategy_mining.py` | 市场扫描挖掘候选标的 |

### 追击策略三条核心原则

1. **顺势不逆势**：只在有明确动量的方向入场，不猜顶猜底
2. **梯度止盈**：TP命中后以更高目标重开，让利润奔跑
3. **一次翻仓**：LONG SL后只翻一次SHORT，SHORT SL后停止，不无限追单

---

## 补充：跌破启动价格时是否放弃追击

### 什么是启动价格

| 策略 | 启动价格定义 |
|------|------------|
| 顶部做空（topshort） | 48h窗口内的最低价（泵起点） |
| 追多（chase_tick） | 4h检测窗口的开盘价 |
| 追跌（dump_tick） | 4h检测窗口的开盘价 |

### 三种策略的分析结论

**顶部做空：需要主动拦截（已修复）**

这是唯一有实际代码缺口的场景。

```
举例:
  启动价 $1.00 -> 峰值 $2.00 (泵100%)
  6h后确认 -> 尝试入场做空
  
  场景A: 现价 $1.80 (回落10%) -> 正常入场, 做空依据充分
  场景B: 现价 $0.95 (跌破启动价) -> 整个涨幅已被抹平, 顶空依据消失
                                     继续开空等于"追跌入场", 不是"顶部做空"
```

拦截规则（已加入 strategy_live.py）：
- `现价 <= 启动价（lo_win）` → 跳过，不开单
- `从峰值回落 > 50%` → 跳过，做空动能大概率耗尽

---

**追多（chase_tick）：RUIN 机制天然处理，无需额外判断**

```
举例:
  启动价 $1.00, 泵20%, 入场 @ $1.20
  RUIN 线: $1.20 * 0.92 = $1.104  (启动价的 110.4%)

  -> 价格要跌到启动价 $1.00 之前, 必然先经过 $1.104
  -> RUIN 触发 -> 翻空 @ $1.104
  -> 翻空后如果价格继续跌到 $1.00, SHORT 利润 = 9.4%
```

结论：8% RUIN 阈值对于 20%+ 触发条件，会在价格到达启动价之前先触发，后续 SHORT 还会从跌穿启动价中获利。不需要额外规则。

---

**追跌（dump_tick）：对称结论，RUIN 亦处理**

```
举例:
  启动价 $1.00, 跌15%, 入场 SHORT @ $0.85
  RUIN 线: $0.85 * 1.08 = $0.918  (仍低于启动价 $1.00)

  -> 若价格反弹回 $1.00, 必然先经过 $0.918
  -> RUIN 触发 -> 翻多 @ $0.918
  -> 翻多后若价格继续回升到 $1.00, LONG 利润 = 8.9%
```

结论：同追多，RUIN 拦截自然发生在"回破启动价"之前。

---

### 决策速查表

| 场景 | 是否需要放弃追击 | 处理方式 |
|------|----------------|----------|
| 顶空信号触发，现价已跌破启动价 | **是** | 跳过不开单（代码已拦截） |
| 顶空信号触发，从峰值回落>50% | **是** | 跳过不开单（代码已拦截） |
| 追多持仓中，价格向下接近启动价 | **不需要额外判断** | RUIN (-8%) 先触发，自动翻空 |
| 追跌持仓中，价格反弹接近启动价 | **不需要额外判断** | RUIN (+8%) 先触发，自动翻多 |
| DONE 冷却后重新进入 IDLE | **不需要额外判断** | 重新扫描时天然要求当前有新的动量信号 |

### 核心逻辑

> 追击策略的 RUIN 止损（8%）对于 20%+ 触发阈值来说，在数学上保证了：
> **持仓中途的"回破启动价"必然晚于 RUIN 触发**，所以不需要手动判断。
>
> 唯一需要主动拦截的是**顶部做空的入场环节**：如果在扫描信号时现价已跌破启动价，
> 说明市场已经自然完成了做空任务，追进去只是在接抛压尾盘，应坚决跳过。
