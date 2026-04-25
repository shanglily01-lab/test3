# 测试文档 — 量化交易系统

版本：v1.2  
更新日期：2026-04-25  
覆盖范围：strategy_live、strategy_whale（含 W 底 + M 顶）、strategy_bigmid、strategy_f3

---

## 1. 测试策略概览

| 测试层级 | 方式 | 目标 |
|----------|------|------|
| 单元测试 | 函数级别，构造最小输入 | 验证每个条件分支的判断正确 |
| 集成测试 | 回放历史 K 线，对比信号输出 | 验证策略在真实数据上的行为 |
| 边界测试 | 阈值边界值±1档 | 验证阈值精确生效 |
| 风控测试 | 模拟止损/熔断场景 | 验证熔断机制正确保护 |
| 配置热更新测试 | 修改 system_settings 后观察 | 验证参数加载逻辑 |

---

## 2. strategy_live 测试用例

### 2.1 CHASE 子策略

#### TC-L-CHASE-01 基础触发：涨幅刚好到达阈值

**前置条件：**
- 构造 24 根 5m K 线，`close[-1] = open[0] * 1.12`（恰好 +12%）
- 窗口内最大单 bar 涨幅 = 5%（> 3%）
- 从高点回撤 = 0%（无耗竭）
- topshort state = IDLE，chase state = IDLE

**期望结果：** 触发开仓，state 变为 PENDING，挂多单，限价 = close * 0.97

---

#### TC-L-CHASE-02 涨幅不足：未达 12% 阈值

**前置条件：**
- 24 根 5m，close[-1] = open[0] * 1.119（+11.9%）

**期望结果：** 不开仓，state 保持 IDLE

---

#### TC-L-CHASE-03 耗竭过滤：高点回撤 > 6%

**前置条件：**
- 涨幅 +15%，但窗口高点在 close[-1] * 1.08 处（从高点回撤 7.4%）

**期望结果：** 不开仓，日志输出"高点回撤 X.X% > 6%，疑似顶部耗竭"

---

#### TC-L-CHASE-04 耗竭过滤边界：高点回撤 = 6%（恰好等于阈值）

**前置条件：**
- 从高点回撤精确等于 0.06

**期望结果：** 不开仓（判断为 `> 0.06`，等于时应通过，验证代码是 `> CHASE_EXHAUST_MAX_DD`）

> 注：代码为 `if dd_from_peak > CHASE_EXHAUST_MAX_DD`，等于时仍可开仓。

---

#### TC-L-CHASE-05 慢速爬升过滤：无单 bar 涨 3%

**前置条件：**
- 24 根 5m 总涨幅 +14%，但每根 bar 涨幅最大仅 2.9%（慢速攀升）

**期望结果：** 不开仓，日志输出"最大单 bar 涨幅 X.X% < 3%，慢速爬升不追"

---

#### TC-L-CHASE-06 方向冲突：topshort 持仓中

**前置条件：**
- 追涨信号满足全部条件
- 同标的 topshort state = 'SHORT'（持仓中）

**期望结果：** 不开仓，日志输出"顶空 state=SHORT，避免双向对冲"

---

#### TC-L-CHASE-07 日内熔断：当日已 2 次止损

**前置条件：**
- `futures_positions` 中同标的今日 notes='stop_loss' 的记录 = 2 条
- 追涨信号满足

**期望结果：** 不开仓，日志输出"今日已止损 2 次，暂停当日交易"

---

#### TC-L-CHASE-08 日内熔断边界：当日仅 1 次止损

**前置条件：**
- 同标的今日 stop_loss 记录 = 1 条

**期望结果：** 正常开仓（熔断阈值为 >= 2）

---

#### TC-L-CHASE-09 冷却机制：平仓后未满 4h

**前置条件：**
- state = DONE，done_time = now - 3h（未满 4h 冷却）

**期望结果：** 不开仓，state 保持 DONE

---

#### TC-L-CHASE-10 冷却到期后正常重入

**前置条件：**
- state = DONE，done_time = now - 5h（超过 4h）

**期望结果：** state 重置为 IDLE，若当前信号满足则可开仓

---

### 2.2 TOPSHORT Climax 子策略

#### TC-L-TOPSHORT-01 基础触发：标准大阳线顶部

**前置条件：**
- 构造 1h K 线序列，领袖 K 满足：
  - body_pct = 3%（> 2.5%）
  - range_pct = 5%（> 4.5%）
  - body_ratio = 0.60（> 0.42）
  - volume = 3x 前 20 根均量（> 2x）
  - 领袖 K 是 24 根内振幅最大阳线
  - 领袖 K 后已有 2 根 1h K 未超越
  - 现价从高点回落 3%（1.2% ~ 48% 区间内）
  - 信号年龄 10h（< 22h）

**期望结果：** 挂空单，state = PENDING

---

#### TC-L-TOPSHORT-02 放量不足：成交量 < 2 倍均量

**前置条件：**
- 领袖 K 所有形态条件满足，但 volume = 1.8x 均量

**期望结果：** 不挂单

---

#### TC-L-TOPSHORT-03 领袖 K 等待不足：后续仅 1 根 1h

**前置条件：**
- 领袖 K 后仅过了 1 根 1h（POST_LEADER_WAIT_BARS = 2）

**期望结果：** 不挂单，等待第 2 根 1h 收盘后才允许判断

---

#### TC-L-TOPSHORT-04 信号超时：领袖 K 已过 26h

**前置条件：**
- 领袖 K 收盘时间 = now - 27h

**期望结果：** 已挂单的撤单，未挂单的不允许新挂

---

#### TC-L-TOPSHORT-05 现价跌破区间：回撤 > 48%

**前置条件：**
- 领袖 K 高点 = 100，现价 = 49（回撤 51%）

**期望结果：** 跳过，不挂单（已跌透，无做空价值）

---

#### TC-L-TOPSHORT-06 历史数据不足：品种上架时间 < 1.25 天

**前置条件：**
- 1h K 线最早一根距今 < 1.25 天（Climax 条件）

**期望结果：** 跳过，不挂单

---

### 2.3 BOTTOMLONG 子策略

#### TC-L-BOTLONG-01 基础触发：标准大阴线底部（实体模式）

**前置条件：**
- 领袖 K：阴线实体 3%，振幅 5%，实体占振幅 0.60，放量 3x
- 现价从低点反弹 2%（1.2% ~ 48% 区间内）
- 后续已有 2 根 1h K

**期望结果：** 挂多单，state = PENDING

---

#### TC-L-BOTLONG-02 下影线模式触发

**前置条件：**
- 领袖 K：下影占振幅 40%（> 34%），(O-L)/O = 3%（> 2%）
- 振幅 >= 4.5%，放量 >= 2x

**期望结果：** 触发底部做多信号

---

#### TC-L-BOTLONG-03 反弹过深：现价距低点 > 48%

**前置条件：**
- 底部形态成立，但现价已从低点反弹 50%

**期望结果：** 不挂单

---

### 2.4 DUMP 子策略

#### TC-L-DUMP-01 基础触发：4h 跌幅恰好 10%

**前置条件：**
- 48 根 5m K 线，open[0] = 100，close[-1] = 90（跌 10%）
- 现价距最低点反弹 3%（< 8%）

**期望结果：** 触发开空，限价 = close * 1.03

---

#### TC-L-DUMP-02 反弹过深：距低点反弹 > 8%

**前置条件：**
- 跌幅 -15%，但现价从最低点已反弹 9%

**期望结果：** 不开仓

---

#### TC-L-DUMP-03 CHASE 持仓冲突

**前置条件：**
- 同标的 CHASE 已持多仓（_has_any_open = True）

**期望结果：** 不开仓

---

### 2.5 动态移动止盈测试

#### TC-L-TP-01 硬止盈触发（20%）

**前置条件：** 持仓中，pnl_pct = 0.201

**期望结果：** 立即平仓，reason = "hard-tp"

---

#### TC-L-TP-02 小赚档：peak 4% 回落 1% 触发

**前置条件：** peak = 0.04, pnl = 0.03（回落 1% 恰达小赚档阈值）

**期望结果：** 平仓，reason = "trail-tp"（_dynamic_trail_pullback(0.04)=0.01）

---

#### TC-L-TP-03 小赚档未触发：回落 0.8%

**前置条件：** peak = 0.04, pnl = 0.032（回落 0.8% < 1%）

**期望结果：** 不平仓

---

#### TC-L-TP-04 中赚档：peak 7% 回落 2% 触发

**前置条件：** peak = 0.07, pnl = 0.05（回落 2%）

**期望结果：** 平仓（_dynamic_trail_pullback(0.07)=0.02）

---

#### TC-L-TP-05 中赚档不被小赚档规则干扰：peak 7% 回落 1.5%

**前置条件：** peak = 0.07, pnl = 0.055（回落 1.5% < 中赚档 2%）

**期望结果：** 不平仓（peak 已过小赚档，用中赚档 2% 阈值）

---

#### TC-L-TP-06 大赚档：peak 15% 回落 3% 触发

**前置条件：** peak = 0.15, pnl = 0.12（回落 3%）

**期望结果：** 平仓（_dynamic_trail_pullback(0.15)=0.03）

---

#### TC-L-TP-07 peak 不足 3% 不启动 trail

**前置条件：** peak = 0.025, pnl = 0.005（回落 2%）

**期望结果：** 不平仓（_dynamic_trail_pullback 返回 inf）

---

#### TC-L-TP-08 早期止损：浮亏 3.1%

**前置条件：** peak = 0, pnl_pct = -0.031

**期望结果：** 平仓，reason = "early-sl"

---

#### TC-L-TP-09 早期止损未触发：浮亏 2.5%

**前置条件：** pnl_pct = -0.025（< 3% 阈值）

**期望结果：** 不平仓

---

#### TC-L-TP-10 保本止损：曾赚过 4% 回吐到 -0.6%

**前置条件：** peak = 0.04, pnl_pct = -0.006

**期望结果：** 平仓，reason = "breakeven-sl"

---

#### TC-L-TP-11 保本止损未触发：没赚到过 3%

**前置条件：** peak = 0.025, pnl_pct = -0.01（没达到保本启动条件 peak≥3%）

**期望结果：** 不平仓（peak < 3% 不启动保本），交由 early-sl（-3%）兜底；当前 -1% 也不到 early-sl 阈值，不平

---

#### TC-L-TP-12 裸奔模式完全不管

**前置条件：** DISABLE_SL_TP_HOLD=True，pnl_pct = -0.05（远超 early-sl）

**期望结果：** `_trail_tp_check` 开头 return False，不触发任何自动平仓

---

### 2.6 限价单填充测试

#### TC-L-LIMIT-01 LONG 限价单成交触发（含 30s 观察确认）

**前置条件：**
- 挂单 LONG，limit_price = 95
- 当前价格 = 94（< 95）

**期望结果（2026-04-24 更新）：**
- **第一次 tick**：记 `_trigger_first_seen[id]=now`，log "限价单触发观察"，**不成交**（status 仍 PENDING）
- **30s 内再 tick 且仍 triggered**：continue，保持 PENDING
- **30s 之后 tick 且仍 triggered**：删记录，走原成交流程 FILLING → FILLED
- **30s 内价格回弹到 >= 95**：删记录，log "触发回撤，重新等待"，status 保持 PENDING

---

#### TC-L-LIMIT-02 SHORT 限价单成交触发（含 30s 观察确认）

**前置条件：**
- 挂单 SHORT，limit_price = 105
- 当前价格 = 106（> 105）

**期望结果（2026-04-24 更新）：** 同 TC-L-LIMIT-01 镜像逻辑，观察 30s 后仍 triggered 才进入成交流程

---

#### TC-L-LIMIT-01b 触发后观察期内回撤（2026-04-24 新增）

**前置条件：**
- 挂单 LONG，limit_price = 95
- tick T0：cur=94，记录 first_seen
- tick T1（T0+15s）：cur=96（回到 limit 上方）

**期望结果：** T1 时 `_trigger_first_seen.pop(id)` 删记录，log "触发回撤，重新等待"，status 仍 PENDING；后续若再次 cur<=95 则重新进入 30s 观察

---

#### TC-L-LIMIT-03 限价单超时取消

**前置条件：**
- 挂单 PENDING，created_at = now - 3601s（超过 1h）

**期望结果：** status 变为 CANCELLED

---

#### TC-L-LIMIT-04 LONG 限价单受 24h 最低价约束

**前置条件：**
- cur_price = 100，offset = 3%，计算 lp = 97
- low_24h = 98

**期望结果：** 实际挂单价 = max(97, 98) = 98

---

#### TC-L-LIMIT-05 反向滑点熔断（SHORT 反向穿越过大）

**前置条件：**
- SHORT 挂单 limit_price = 0.09579（CHIP 4/23 05:19 重放）
- 扫描时 cur_price = 0.09846，反向偏离 = (0.09846 - 0.09579) / 0.09579 = 2.79%
- 大于 REVERSE_SLIPPAGE_LIMIT(0.015)

**期望结果：** 订单不进入 FILLING；`status='CANCELLED'`，`cancellation_reason='reverse_slippage_0.0279'`；日志输出"反向滑点熔断撤单 ... 偏离=2.79%"

---

#### TC-L-LIMIT-06 反向滑点熔断（LONG 反向穿越过大）

**前置条件：**
- LONG 挂单 limit_price = 100
- cur_price = 98.4，反向偏离 = (100 - 98.4) / 100 = 1.6%（> 1.5%）

**期望结果：** CANCELLED，不填充

---

#### TC-L-LIMIT-07 反向滑点熔断边界（恰好 1.5%）

**前置条件：**
- SHORT 挂单 limit_price = 100，cur_price = 101.5（偏离恰好 1.5%）

**期望结果：** **允许填充**（判断是 `> 0.015`，等于时通过）

---

#### TC-L-LIMIT-08 正常滑点不误伤

**前置条件：**
- SHORT 挂单 limit_price = 100，cur_price = 100.5（正向偏离 0.5%，在容忍范围内）

**期望结果：** 正常填充；若偏离 > 0.1% 则触发 SL/TP 按成交价重算

---

---

## 3. strategy_whale 测试用例

### 3.1 信号评分系统

#### TC-W-SCORE-01 满分场景（做空，全部条件满足）

**前置条件：**
- funding_rate = 0.0006（> 0.0005）→ +3
- long_pct = 0.68（> 0.65）→ +2
- oi_chg_4h = -0.04（< -0.03）→ +2
- vol_ratio = 3.0（> 2.5），price_chg = 0.5%（< 1.5%）→ +3
- taker_buy_ratio = 0.38（< 0.42）→ +1
- 触发器：大阴线 3%（> 2.5%）→ triggered = True
- 总分 = 11

**期望结果：** score = 11，triggered = True，开仓

---

#### TC-W-SCORE-02 恰好达到入场门槛

**前置条件：**
- funding_rate = 0.0003（+2），long_pct = 0.60（+1），oi_chg = -0.01（+1），vol_ratio = 2.0（价格滞，+2），触发器满足
- 总分 = 6（> 5）

**期望结果：** 开仓

---

#### TC-W-SCORE-03 评分不足：总分 4 分

**前置条件：**
- 仅有资金费率 +3，多空比 +1，触发器满足
- 总分 = 4

**期望结果：** 不开仓（< ENTRY_SCORE_MIN(5)）

---

#### TC-W-SCORE-04 触发器未满足：即使评分 >= 5

**前置条件：**
- 评分 = 8，但无大阴线（price_chg = -0.5%，< 2.5%），无跌破支撑（current > min_4h_low * 0.995）

**期望结果：** 不开仓（triggered = False）

---

#### TC-W-SCORE-05 放量但价格变化过大：不计放量分

**前置条件：**
- vol_ratio = 3.0（放量），但 price_chg = -2.5%（超过 1.5% 的滞涨门槛）

**期望结果：** 放量分 = 0，该评分维度不得分

---

#### TC-W-SCORE-06 资金费率边界：恰好 = 0.0001

**前置条件：**
- funding_rate = 0.0001（= FR_MILD_HIGH）

**期望结果：** +1 分（判断为 `>= FR_MILD_HIGH`，等于时得分）

---

### 3.2 仓位管理

#### TC-W-POS-01 单侧满仓：空头已有 3 个持仓

**前置条件：**
- 已有 3 个 SHORT 持仓，新信号评分满足

**期望结果：** 不开仓（MAX_POS_PER_SIDE = 3，已达上限）

---

#### TC-W-POS-02 单侧未满：空头 2 个，可继续开仓

**前置条件：**
- 已有 2 个 SHORT 持仓，新信号满足

**期望结果：** 正常开仓

---

### 3.3 冷却机制

#### TC-W-CD-01 正常止盈后冷却 6h

**前置条件：**
- last_reason = "hard-tp"，done_time = now - 5h（未到 6h）

**期望结果：** 不开仓

---

#### TC-W-CD-02 正常止盈冷却到期

**前置条件：**
- last_reason = "hard-tp"，done_time = now - 7h（超过 6h）

**期望结果：** 允许进入评分判断流程

---

#### TC-W-CD-03 止损后惩罚性冷却 12h

**前置条件：**
- last_reason = "stop_loss"，done_time = now - 11h（未满 12h）

**期望结果：** 不开仓（惩罚性冷却中）

---

#### TC-W-CD-04 止损后冷却到期

**前置条件：**
- last_reason = "stop_loss"，done_time = now - 13h（超过 12h）

**期望结果：** 允许进入评分

---

#### TC-W-CD-05 止损冷却 vs 普通冷却边界对比

**前置条件：**
- 场景 A：last_reason="trail-tp"，done_time = now - 6.5h → 应允许进场
- 场景 B：last_reason="stop_loss"，done_time = now - 6.5h → 应仍在冷却

**期望结果：** A 允许，B 不允许

---

### 3.4 限价单测试（Whale）

#### TC-W-LIMIT-01 SHORT 挂单偏移 0.3%

**前置条件：**
- cur_price = 1000，WHALE_LIMIT_OFFSET_PCT = 0.003
- high_24h = 1010

**期望结果：** limit_price = min(1000 * 1.003, 1010) = min(1003, 1010) = 1003

---

#### TC-W-LIMIT-02 SHORT 受 24h 最高价压制

**前置条件：**
- cur_price = 1000，high_24h = 1001（偏移后 1003 > 1001）

**期望结果：** limit_price = 1001（受 high_24h 约束）

---

#### TC-W-LIMIT-03 Whale 限价单超时 2h 撤单

**前置条件：**
- 挂单 PENDING，created_at = now - 7201s（> 2h）

**期望结果：** 自动撤单，state 重置为 IDLE

---

### 3.5 移动止盈（Whale）

#### TC-W-TP-01 硬止盈 20%

**前置条件：**
- 持仓中，pnl_pct = 0.201

**期望结果：** 平仓，reason = "hard-tp"

---

#### TC-W-TP-02 移动止盈激活并触发

**前置条件：**
- peak = 0.15，当前 pnl = 0.128（回落 2.2% >= 2%）

**期望结果：** 平仓，reason = "trail-tp"

---

---

## 3a. strategy_bigmid 测试用例

### 3a.1 分档归属

#### TC-BM-TIER-01 BIG 门槛

**前置条件：** price_stats_24h.quote_volume_24h = $500,000,001（刚过 BIG 门槛）

**期望结果：** `get_tier()` 返回 `"BIG"`

---

#### TC-BM-TIER-02 MID 门槛

**前置条件：** quote_volume_24h = $100,000,001

**期望结果：** 返回 `"MID"`

---

#### TC-BM-TIER-03 池外

**前置条件：** quote_volume_24h = $99,999,999

**期望结果：** 返回 `None`，该品种不被扫描

---

#### TC-BM-TIER-04 BIGMID_EXCLUDES 硬排除

**前置条件：** 刷新池时 XAU/USDT 成交量 = $1.3B

**期望结果：** 不进入 BIG 列表（`BIGMID_EXCLUDES` 命中），忽略此币

---

#### TC-BM-TIER-05 1000* 前缀黑名单 + 白名单

**前置条件：** 刷新池时候选含 1000PEPE/USDT（$270M）与 1000MOG/USDT（$280M）

**期望结果：** 1000PEPE 进 MID 列表（白名单放行）；1000MOG 被排除

---

### 3a.2 BIG 档 Whale 评分

#### TC-BM-WHALE-01 做空评分过门槛：BTC 顶部结构

**前置条件：**
- BTC/USDT tier=BIG
- funding_rate = 0.00004（+2 做空）
- long_account = 0.48（< 阈值，0 分，BTC 长期偏空）
- OI 4h 变化 = -1.5%（< -1%，+1 做空）
- 最近 3 根 1h 均量 / 前 24 根均量 = 2.3（> 2.0 strong），价格变化 +0.6%（< 1% 滞涨），+3 做空
- 最近 3 根 taker_buy_ratio 均值 = 0.43（< 0.45，+1 做空）
- 触发器：1h 阴线 -0.9%（> 0.8%），触发器 ✓
- 总分：2+0+1+3+1 = 7 ≥ 4

**期望结果：** 开 SHORT，状态机 `(bigmid, BTC/USDT, whale)` = PENDING，source=`strategy_bigmid:whale-entry`，市价入场

---

#### TC-BM-WHALE-02 评分过门槛但触发器缺失

**前置条件：** 评分 6 分（足够），但最新 1h K body 只有 0.5% 且无突破

**期望结果：** 不开仓（触发器必须满足）

---

#### TC-BM-WHALE-03 双向评分同时满足

**前置条件：** short 评分 5 分 + 触发器；long 评分 6 分 + 触发器

**期望结果：** 开 LONG（择优开高分方）

---

#### TC-BM-WHALE-04 双向评分同分

**前置条件：** short 和 long 都 5 分 + 触发器

**期望结果：** 开 SHORT（同分偏向 short，避险优先）

---

#### TC-BM-WHALE-05 funding 阈值缩放验证

**前置条件：** fr=0.00004（= 0.004%）

**期望结果：** 原 whale 阈值下不得分（< 0.0001），BIG Whale 下得 +2（≥ fr_high 0.00003）

---

### 3a.3 MID 档 CHASE / DUMP

#### TC-BM-CHASE-01 MID 触发：HYPE 15m 涨 6.2%

**前置条件：** MID 档，24 根 15m (6h) 涨幅 6.2%，单 bar 最大 1.8%，回撤 2.5%

**期望结果：** 触发开多，SL=5%，TP=10%，限价 = close × (1 - 0.015)

---

#### TC-BM-DUMP-01 MID 反弹过深

**前置条件：** MID 档，跌幅 5.5%，但距最低点已反弹 4.5%（> 4%）

**期望结果：** 不开仓

---

### 3a.4 反向滑点熔断（仅 MID 档有挂单）

#### TC-BM-SLIP-01 MID 熔断：偏离 0.80%

**前置条件：** SHORT LIMIT 100，cur_p = 100.8，tier=MID，阈值 0.75%

**期望结果：** CANCELLED

---

#### TC-BM-SLIP-02 MID 正常滑点：偏离 0.3%

**前置条件：** tier=MID，偏离 0.3%

**期望结果：** 正常填充（小于 0.75% 阈值）

---

### 3a.5 挂单期间 tier 降档

#### TC-BM-DOWNGRADE-01 挂单中成交量跌出 $100M

**前置条件：** 挂单时 tier=MID，2h 后刷新 price_stats_24h 显示该币成交量跌到 $90M

**期望结果：** 下一轮 `_fill_pending_orders` 扫描时 `lookup_tier()` 返回 None → CANCELLED，reason='tier_downgrade'

---

### 3a.6 策略隔离

#### TC-BM-ISOLATE-01 bigmid 不触碰 strategy_live 订单

**前置条件：**
- futures_orders 表中同时存在 `source='strategy_live:chase-entry'` 与 `source='strategy_bigmid:chase-entry'` 的 PENDING 订单

**期望结果：** `strategy_bigmid._fill_pending_orders` 只处理 `strategy_bigmid:%` 的订单，strategy_live 的订单不变

---

#### TC-BM-ISOLATE-02 共用 _has_any_open

**前置条件：** strategy_live 在 SOL/USDT 有 open 持仓

**期望结果：** strategy_bigmid 的 CHASE 信号满足时，`_has_any_open` 返回 True，拒绝重复开仓

---

### 3.6 W 型双底子策略（2026-04-24 改为 15m K 线）

**时间尺度变更说明：** 2026-04-24 从 1h 改为 15m，所有 bar 数常量数值不变，实际时间尺度按 1/4 缩短。以下所有 "bar" 均指 15m K 线；持仓从 3 天缩到 1 天（`max_hold=1440min`）。

#### TC-W-WB-01 标准 W 型触发

**前置条件：** 336 根 15m K 线（=3.5 天），i1=0 b1=100，ic=60 neck=108（反弹 8%），ib2=120 b2=101（±1%），cur=110（>108×1.005）

**期望结果：** 开 LONG，source=`strategy_whale:w-bottom`，无 SL/TP，**max_hold=1440min（1 天）**

---

#### TC-W-WB-02 反弹不足 5%

**前置条件：** rebound=4%

**期望结果：** 不触发

---

#### TC-W-WB-03 两底价差过大（2026-04-24 阈值 5%）

**前置条件：** b1=100, b2=106（6% > 5%）

**期望结果：** 不触发

---

#### TC-W-WB-04 未突破颈线（2026-04-24 阈值 +0.5%）

**前置条件：** cur=108.4, neck=108（108.4 / 108 = 1.0037 < 1.005）

**期望结果：** 不触发

---

#### TC-W-WB-05 B2 距颈线太近（假探底）

**前置条件：** ic=60, ib2=62（只 2 根 = 30min，< 4 根 = 1h）

**期望结果：** 不触发

---

#### TC-W-WB-06 数据不足 3.5 天（2026-04-24 更新）

**前置条件：** 只有 300 根 15m K（< 336 根）

**期望结果：** 不触发（数据不足）

---

#### TC-W-WB-07 同品种 3 天内冷却

**前置条件：** state done_time=now-2.5d

**期望结果：** 不触发，保持 DONE

---

#### TC-W-WB-08 全局持仓数上限

**前置条件：** strategy_state 表已有 3 笔 `w-bottom` state!=IDLE 记录

**期望结果：** 不再开新仓

---

#### TC-W-WB-09 不设 SL/TP

**前置条件：** W 双底触发

**期望结果：** 写入 futures_positions 时 stop_loss_price=NULL, take_profit_price=NULL；
PositionSLTPMonitor 扫描时因 sl/tp 都 NULL 跳过该仓位（monitor 不会平它）

---

## 4. 配置加载测试

#### TC-CFG-01 strategy_live 从 DB 加载参数

**测试步骤：**
1. 在 system_settings 表写入 `live_sl_pct = 0.08`
2. 重启 strategy_live 进程
3. 观察启动日志

**期望结果：** 日志输出"SL=8% TP=20% offset=X% hold=Xh"，CHASE_SL_PCT 等于 0.08

---

#### TC-CFG-02 strategy_whale 从 DB 加载参数

**测试步骤：**
1. 在 system_settings 表写入 `whale_sl_pct = 0.12`
2. 重启 strategy_whale
3. 观察启动日志

**期望结果：** 日志输出"SL=12%"，SL_PCT 等于 0.12

---

#### TC-CFG-03 DB 读取失败时使用默认值

**测试步骤：**
1. 修改 DB_HOST 为无效值，重启进程

**期望结果：** 日志输出"_load_live_config 失败，使用默认值"，进程正常启动，使用代码内默认值

---

#### TC-CFG-04 Web UI 保存参数后 DB 更新

**测试步骤：**
1. 在 system_settings 页面设置趋势跟踪引擎止损 = 12%，保存
2. 查询 system_settings 表 `live_sl_pct`

**期望结果：** DB 中 setting_value = '0.12'

---

---

## 5. 风险控制专项测试

#### TC-RISK-01 同标的持仓去重

**前置条件：**
- BTC/USDT 已有 CHASE LONG 持仓

**期望结果：** BTC/USDT 的 DUMP SHORT 信号被 `_has_any_open` 拦截，不开仓

---

#### TC-RISK-02 日内熔断全流程

**步骤：**
1. 模拟 BTC/USDT 第 1 次止损：写入 futures_positions，notes='stop_loss', close_time=TODAY
2. 触发 BTC/USDT 新信号，期望正常开仓
3. 模拟第 2 次止损
4. 再次触发信号，期望被熔断拦截

**期望结果：** 第 3 次信号被拦截，日志输出"今日已止损 2 次"

---

#### TC-RISK-03 超时平仓全流程

**前置条件：**
- 持仓 futures_positions，timeout_at = now - 1min（已超时）

**期望结果：** 策略检测到超时，调用 POST /api/futures/close，reason = "timeout"

---

#### TC-RISK-04 限价单并发防护（FILLING 中间态）

**前置条件：**
- 同一订单同时被两个循环检查（模拟并发）

**期望结果：** 只有第一个 UPDATE status='FILLING' 成功（乐观锁），第二个 UPDATE 影响行数 = 0，拒绝重复填充

---

---

## 6. 回测验证用例（历史数据）

以下用例需基于实际历史 K 线数据验证策略信号准确性：

| 用例 | 标的 | 日期 | 预期信号类型 | 说明 |
|------|------|------|--------------|------|
| TV-01 | PEPE/USDT | 2026-04 | CHASE | 2h 急涨 15%，无耗竭 |
| TV-02 | Q/USDT | 2026-04-22 | CHASE 被过滤 | 涨幅满足但从高点回撤 > 6%，耗竭过滤生效 |
| TV-03 | BLUR/USDT | 2026-04-22 | CHASE 被过滤 | 涨幅满足但单 bar 最大涨幅 < 3%，慢速爬升过滤 |
| TV-04 | SPK/USDT | 2026-04-22 | CHASE 被过滤 | 追涨信号满足但 topshort 已持空单，冲突过滤 |
| TV-05 | CHIP/USDT | 2026-04-22 | CHASE 熔断 | 当日 2 次止损，后续信号被日内熔断拦截 |

---

## 7. API 集成测试

#### TC-API-01 strategy-config GET

```
GET /api/system/strategy-config
期望返回：
{
  "success": true,
  "data": {
    "live_sl_pct": 0.10,
    "live_hard_tp_pct": 0.20,
    "live_limit_offset_pct": 0.03,
    "live_hold_hours": 6,
    "whale_sl_pct": 0.10,
    "whale_hard_tp_pct": 0.20,
    "whale_limit_offset_pct": 0.003,
    "whale_hold_hours": 6
  }
}
```

---

#### TC-API-02 strategy-config PUT

```
PUT /api/system/strategy-config
Body: {"live_sl_pct": 0.08, "whale_hold_hours": 8}

期望返回：
{"success": true, "message": "策略参数已保存，重载配置后生效"}

验证：SELECT setting_value FROM system_settings WHERE setting_key='live_sl_pct'
期望：'0.08'
```

---

#### TC-API-03 trading-services PUT 只保留 live_trading_enabled

```
PUT /api/system/trading-services
Body: {"live_trading_enabled": true}

期望：仅更新 live_trading_enabled，不影响其他字段
```

---

## 7A. 2026-04-24 新增测试用例

### TC-CHASE-24H-01 24h 跌幅 > 10% 跳过追涨

**前置条件：** 品种 24h change = -12%，5m pump 条件满足（+15%）

**期望结果：** `chase_tick` 在 Step 0 即 return，log "CHASE 跳过 XXX: 24h=-12.0% < -10%，熊市反弹不追"

---

### TC-CHASE-24H-02 24h 跌幅边界

**前置条件：** 品种 24h change = -9.9%

**期望结果：** Step 0 通过，进入正常 pump 判断流程

---

### TC-CHASE-24H-03 24h 数据缺失

**前置条件：** `price_stats_24h` 中 change_24h IS NULL

**期望结果：** Step 0 跳过过滤，进入正常流程（向后兼容缺数据场景）

---

### TC-GRACE-45M-01 入场保护期 45 分钟

**前置条件：** 开仓后 44 分钟，pnl_pct = -4%（超 early-sl 阈值 3%）

**期望结果：** early-sl 不触发（仍在 grace 窗口），硬 SL 10% 兜底

---

### TC-GRACE-45M-02 保护期外触发

**前置条件：** 开仓 46 分钟后，pnl_pct = -3.5%

**期望结果：** early-sl 正常触发平仓

---

### TC-TRIG-CONFIRM-01 30s 观察期内成交拒绝

**前置条件：** T0：LONG limit=95，cur=94（触发）；T0+20s：cur=93.5（仍触发）

**期望结果：** T0 记录观察，T0+20s continue（未满 30s），T0+30s+ 才成交

---

### TC-TRIG-CONFIRM-02 观察期内价格回撤清除观察

**前置条件：** T0：LONG limit=95，cur=94（触发，记录）；T0+15s：cur=95.5（回撤到触发线上方）

**期望结果：** T0+15s 删除 `_trigger_first_seen[id]`，log "触发回撤，重新等待"，status 保持 PENDING

---

### TC-PSYNC-01 MARKET 开仓单同步（2026-04-24 扩展）

**前置条件：** `live_trading_enabled=1`，有 FILLED OPEN_SHORT MARKET 单，paper_pid 对应的 `futures_positions.status='open'`，该 paper_pid 无其他 SYNCED 订单

**期望结果：** 被扫描到、开实盘仓位、`live_sync_status=SYNCED` + `live_position_id` 写入

---

### TC-PSYNC-02 去重：paper 仓已关不同步

**前置条件：** FILLED OPEN MARKET 单 live_sync_status IS NULL，但 paper_pid 对应的 `futures_positions.status='closed'`

**期望结果：** JOIN + `fp.status='open'` 过滤，该订单**不被扫到**（仍保持 NULL，也不 SYNCED）

---

### TC-PSYNC-03 去重：同 paper_pid 已有 SYNCED 不双开

**前置条件：** LIMIT 单 id=A live_sync_status=SYNCED live_position_id=1234；MARKET 单 id=B live_sync_status IS NULL，A 和 B 的 position_id 相同，fp.status='open'

**期望结果：** `NOT EXISTS (SYNCED)` 过滤，B **不被扫到**，避免双开实盘仓位

---

### TC-PSYNC-04 CLOSE_* 订单不被误当开仓

**前置条件：** FILLED CLOSE_LONG MARKET 单 live_sync_status IS NULL

**期望结果：** `side IN ('OPEN_LONG','OPEN_SHORT')` 过滤排除，不同步

---

## 8. 测试环境要求

| 项目 | 要求 |
|------|------|
| 数据库 | MySQL 8.0+，含 kline_data / futures_positions / strategy_state / system_settings |
| Python | 3.10+，依赖 pymysql / requests / python-dotenv |
| FastAPI 服务 | 本地 port 9021 运行 |
| 历史 K 线 | kline_data 至少包含测试标的 30 天 5m/1h 数据 |
| 测试账户 | account_id=2，初始余额足够开 5 笔 MARGIN=500 的仓位 |

---

## 9. 回归测试检查清单

每次修改策略代码后需验证：

- [ ] CHASE 涨幅 12% 触发，11.9% 不触发
- [ ] CHASE 耗竭过滤在高点回撤 > 6% 时生效
- [ ] CHASE 慢速爬升过滤在单 bar 涨幅 < 3% 时生效
- [ ] 反向滑点 > 1.5% 撤单不填；= 1.5% 允许填充；正向滑点不受影响
- [ ] strategy_bigmid 分档归属正确（$500M/100M 门槛 + BIG_WHITELIST 双重判定）
- [ ] BIG 档走 whale 评分；MID 档走 CHASE/DUMP
- [ ] BIG Whale 评分 ≥ 4 且触发器满足才开仓；双向同分偏 SHORT
- [ ] BIG Whale 市价入场（limit_offset=0）；MID 限价偏移 1.5% + 反向滑点 0.75% 熔断
- [ ] strategy_bigmid 只处理 source LIKE 'strategy_bigmid:%' 的订单和仓位
- [ ] 挂单期间 tier 降档到池外 → tier_downgrade 撤单
- [ ] TOPSHORT Climax 放量 < 2x 时不触发
- [ ] TOPSHORT Climax 信号 > 26h 自动撤单
- [ ] DUMP 反弹 > 8% 时不触发
- [ ] 硬止盈 20% 平仓正确
- [ ] 动态移动止盈分档正确：peak 3-5%/1%, 5-10%/2%, ≥10%/3%, <3% 不启动
- [ ] 早期止损：浮亏 ≥ 3% 平仓 (reason='early-sl')；< 3% 不触发
- [ ] 保本止损：peak ≥ **1.5%** 且 pnl ≤ -0.5% 平仓 (reason='breakeven-sl')；peak 不足 1.5% 时此规则不启动
- [ ] DISABLE_SL_TP_HOLD=ON 时上述所有自动出场规则全部跳过
- [ ] 入场 **45 分钟**保护期内，early-sl / breakeven-sl 不触发（仅硬 SL 兜底）；45 分钟后全部启用（2026-04-24 从 30m 放宽）
- [ ] 日内熔断第 2 次止损后第 3 次信号被拦截
- [ ] Whale 评分 4 分不开仓，5 分无触发器不开仓
- [ ] Whale 止损后冷却 12h > 普通冷却 6h
- [ ] system_settings 参数加载正确覆盖默认值
- [ ] **CHASE 24h 跌幅 > 10% 跳过**（2026-04-24 新增）
- [ ] **限价单触发后 30s 观察确认**才成交（2026-04-24 新增，live/whale/bigmid 全部生效）；观察期内价格回撤则清除观察重等
- [ ] **W 底改用 15m + 3.5 天窗口**（2026-04-24），持仓上限 1 天
- [ ] **PaperLimitSyncService 同步 LIMIT + MARKET**（2026-04-24）
- [ ] **PaperLimitSyncService 去重**：fp.status='open' 且 同 paper_pid 无 SYNCED 订单 才同步（防双开/复开）

---

## 10. v1.2 新增测试用例（2026-04-24 ~ 2026-04-25）

### 10.1 strategy_f3 W 底小涨带量 LONG

#### TC-F3-01 标准触发
**前置**：7 天最大跌幅 25%；最近 24h 涨跌 +1%；脱离 24h 最低；最后一根 15m 阳线 1.5%；量比 2x

**期望**：开 LONG，source=`strategy_f3:f3-entry`，限价 = cur×0.995

#### TC-F3-02 24h 已反弹 +3% 拒绝
**前置**：drop=22%，但 ch24=+3%（>+2% 上限）

**期望**：拒绝，无开仓（核心过滤生效）

#### TC-F3-03 阳线幅度过大拒绝
**前置**：所有满足，但最后一根 15m body=4%（>=3% 上限）

**期望**：拒绝（小阳优于大阳）

#### TC-F3-04 黑名单优先级
**前置**：PENGU/USDT 在 F3_BLACKLIST，符合 F3 形态

**期望**：拒绝（F3 黑名单一票）

#### TC-F3-05 白名单覆盖全局黑名单
**前置**：SPK/USDT 在 GLOBAL_BLACKLIST_BASE 但也在 F3_WHITELIST，符合 F3 形态

**期望**：开 LONG（白名单覆盖）

### 10.2 strategy_whale M 顶（默认禁用，启用时验证）

#### TC-MT-01 标准 M 顶触发
**前置**：3.5 天 15m 内 H1=最高，回落到 D（-5%），二次冲高 H2 ∈ [H1±5%]，H1→H2 间隔 1 天，当前价 < D × 0.995

**期望**：开 SHORT，source=`strategy_whale:m-top`，无 SL/TP，1 天 timeout

#### TC-MT-02 两顶差超 5%
**前置**：H2 = H1 × 1.07

**期望**：拒绝（两顶差 > 5%）

#### TC-MT-03 未跌破颈线
**前置**：当前价 = D × 0.998（跌破不足 0.5%）

**期望**：拒绝

### 10.3 入场守卫（strategy_live 5 处）

#### TC-EG-POS-01 LONG 追高拒绝
**前置**：cur 在 3h 区间 95% 位（>90%）

**期望**：chase_tick 拒绝，log "追高 pos=95%"；不开仓

#### TC-EG-POS-02 SHORT 踩底拒绝
**前置**：cur 在 3h 区间 5% 位（<10%）

**期望**：dump/topshort 任何 SHORT 入场拒绝

#### TC-EG-POS-03 破顶拒绝（双向）
**前置**：cur 在 3h 区间 130% 位（已突破上沿）

**期望**：任何方向拒绝，log "破顶 pos=130%"

#### TC-EG-POS-04 破底拒绝（双向）
**前置**：cur 在 3h 区间 -50% 位

**期望**：任何方向拒绝

#### TC-EG-24H-01 chase 24h > +15% 拒绝
**前置**：24h change = +18%

**期望**：chase 拒绝（追顶）

#### TC-EG-24H-02 dump 24h < -15% 拒绝
**前置**：24h change = -18%

**期望**：dump 拒绝（接飞刀）

#### TC-EG-24H-03 bottomlong 24h > +15% 拒绝
**前置**：24h change = +49%

**期望**：bottomlong-climax 拒绝（已涨不是底）— 修 04-25 API3 案例

#### TC-EG-24H-04 topshort 24h < -15% 拒绝
**前置**：24h change = -20%

**期望**：topshort-classic / climax 拒绝（已跌不该再做空）

### 10.4 5m K 线方向确认（4 文件统一）

#### TC-LM5-01 SHORT 触发后下根 5m 阴线成交
**前置**：SHORT 限价 95，cur 涨到 95 → 等下一根 5m K 线收盘 → close=94.8 < open=95.1（阴线）

**期望**：成交，进入 FILLING 流程

#### TC-LM5-02 SHORT 触发后下根 5m 阳线不成交
**前置**：SHORT 限价 95，cur 涨到 95 → 下一根 5m close=95.5 > open=95.1（阳线）

**期望**：清除等待，limit 保留，log "限价 5m 阴未现 不成交, 等下次触发"

#### TC-LM5-03 LONG 触发后阳线成交
**前置**：LONG 限价 90，cur 跌到 90 → 下一根 5m close=90.3 > open=89.9（阳线）

**期望**：成交

#### TC-LM5-04 平 K 算逆向
**前置**：SHORT 触发，下一根 5m close == open（平 K）

**期望**：清除等待，不成交（平 K 视为逆向）

#### TC-LM5-05 价格触发后回撤清除
**前置**：cur 触达 limit_p（记录 first_seen），下一轮 cur 已经离开触发侧

**期望**：清除 first_seen，下次重新触发时再开始等

### 10.5 七上八下限价定价

#### TC-LP-01 SHORT 大跌后挂 4h 高点 80%
**前置**：cur=70，4h_high=100，4h_low=60，offset=3%

**期望**：lp = max(100×0.80, 70×1.03) = max(80, 72.1) = **80**（4h 阻力位）

#### TC-LP-02 SHORT 中段 cur×1.03 优先
**前置**：cur=95，4h_high=100，offset=3%

**期望**：lp = max(80, 97.85) = **97.85**（默认 3% 偏移）

#### TC-LP-03 LONG 大涨后挂 4h 低点 +30%
**前置**：cur=110，4h_low=80，4h_high=120，offset=3%

**期望**：lp = min(80×1.30, 110×0.97) = min(104, 106.7) = **104**（4h 支撑位）

#### TC-LP-04 LONG 中段 cur×0.97 优先
**前置**：cur=85，4h_low=80，offset=3%

**期望**：lp = min(80×1.30, 85×0.97) = min(104, 82.45) = **82.45**

#### TC-LP-05 4h 数据缺失回退 3%
**前置**：4h_high/low 为 None（kline 未收齐 4h）

**期望**：lp = cur × (1±offset)

#### TC-LP-06 24h 高低约束仍生效
**前置**：SHORT lp 计算结果 105，但 24h_high = 102

**期望**：lp = min(105, 102) = 102

### 10.6 白名单 / 黑名单优先级（F3）

#### TC-F3-WL-01 effective_blacklist 白名单覆盖
**前置**：SPK 在 GLOBAL_BLACKLIST_BASE，也在 F3_WHITELIST

**期望**：`_effective_blacklist()` 不含 SPK；`get_universe()` 强制将 SPK 加入扫描池

#### TC-F3-WL-02 universe 强制加入白名单
**前置**：ZBT 在 F3_WHITELIST 但 24h 成交额排不进 top 200

**期望**：universe 列表里仍有 ZBT

### 10.7 引擎 max_profit_pct 修复

#### TC-ENG-01 mark price 刷新更新峰值字段
**前置**：仓位 unrealized_pnl_pct 升到 5%

**期望**：mark price 刷新后，DB 中 max_profit_pct=5（之前 = 0），max_profit_price = 当前 mark_price，max_profit_time = NOW()

#### TC-ENG-02 浮亏不更新峰值
**前置**：仓位 unrealized_pnl_pct = -2%

**期望**：max_profit_pct 不变（GREATEST 兜底为 0）

### 10.8 限价超时

#### TC-LM-TO-01 strategy_live 2h 超时撤单
**前置**：LIMIT created_at = NOW() - 2.1 小时

**期望**：状态置 CANCELLED

#### TC-LM-TO-02 strategy_f3 3h 超时撤单
**前置**：F3 LIMIT created_at = NOW() - 3.1 小时

**期望**：状态置 CANCELLED（F3 等更长）
