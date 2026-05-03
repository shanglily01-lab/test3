# 需求文档 — 量化交易系统

版本：v1.2  
更新日期：2026-04-25  
覆盖范围：strategy_live（小币趋势跟踪）、strategy_whale（庄家对抗 + W 底 + M 顶）、strategy_bigmid（中大市值引擎）、strategy_f3（W 底变种 LONG）

---

## 1. 项目背景

本系统针对加密货币永续合约市场，运行三个独立的自动化交易策略引擎：

- **趋势跟踪引擎**（strategy_live）：小币池（通常成交量 < $100M），5m 级别捕捉强势涨跌，含 CHASE/TOPSHORT/BOTTOMLONG/DUMP 四个子策略。
- **庄家对抗引擎**（strategy_whale）：检测庄家分发/吸筹行为，多维评分做空高杠杆泡沫。
- **中大市值引擎**（strategy_bigmid）：中大币池（成交量 >= $100M），分 BIG / MID 两档按波动比例缩放阈值，MVP 只含 CHASE + DUMP。

三个引擎独立进程运行，共用同一套纸仿真账户（account_id=2）和 FastAPI 服务，通过本地 HTTP API 下单。状态机按 strategy name 隔离（`live` / `whale` / `bigmid`），订单与仓位按 `source LIKE 'strategy_X:%'` 前缀区分。

---

## 2. 业务目标

| 目标 | 指标 |
|------|------|
| 识别趋势启动信号 | CHASE：2小时内涨幅 >= 12% |
| 识别顶部结构 | TOPSHORT：48h涨幅 >= 80% 且 6h内无新高 |
| 识别底部结构 | BOTTOMLONG：1h大阴线 / 长下影 + 2倍放量 |
| 识别追跌机会 | DUMP：4小时内跌幅 >= 10% |
| 识别庄家出货 | WHALE：资金费率+多空比+OI+成交量+隐性大单多维评分 >= 5分 |
| 控制单笔风险 | SL: 止损 8~12%；TP: 硬止盈 20% |
| 控制标的日风险 | 同标的当日止损 >= 2次，暂停当日交易 |
| 防止方向冲突 | 同标的不能同时持有 CHASE 多单 + TOPSHORT 空单 |

---

## 3. 用户角色

- **系统管理员**：通过 Web 配置页调整止损止盈、限价偏移、持仓时长等参数。
- **策略引擎**：自动轮询信号，执行开仓/平仓/风控操作。

---

## 4. 功能需求

### 4.1 趋势跟踪引擎（strategy_live）

#### F-L-01 追涨（CHASE）

| 需求项 | 描述 |
|--------|------|
| F-L-01-01 | 检测品种在过去 24 根 5m K 线（=2h）内涨幅 >= 12% |
| F-L-01-02 | 若当前价格从窗口最高点回撤 > 6%，判定为顶部耗竭，拒绝进场 |
| F-L-01-03 | 窗口内至少一根 5m K 线单 bar 涨幅 >= 3%，排除慢速爬升假信号 |
| F-L-01-04 | 若同标的已有 TOPSHORT 持仓/挂单（state != IDLE/DONE），拒绝进场避免双向对冲 |
| F-L-01-05 | 开多仓，限价低于当前价 3%，受 24h 最低价约束 |
| F-L-01-06 | 止损 8%（可配置），止盈 20%（硬止盈，可配置） |
| F-L-01-07 | 最大持仓时长 6h，超时自动平仓 |
| F-L-01-08 | 动态移动止盈：peak 3-5% 回落 1% 平 / 5-10% 回落 2% / ≥10% 回落 3%；peak < 3% 不启动 |
| F-L-01-10 | 早期止损：浮亏达 3% 直接平仓（reason='early-sl'），单笔最大亏损从 10% 缩到 3% |
| F-L-01-11 | 保本止损：曾浮盈 ≥ **1.5%** 的仓位（2026-04-24 从 3% 放宽），若回吐到 -0.5% 立即平仓（reason='breakeven-sl'），防盈利单翻亏 |
| F-L-01-12 | 以上出场规则受 `disable_sl_tp_hold` 总开关控制，开启时全部跳过（裸奔自生自灭）|
| F-L-01-13 | 出场规则同时由 `PositionSLTPMonitor`（1s 扫描） 和策略 `_trail_tp_check`（60s 轮询）双路执行，先触发者胜；小币快速穿越靠 monitor 兜底 |
| F-L-01-14 | **入场保护期 ENTRY_GRACE_MIN=45 分钟**（2026-04-24 从 30m 放宽）：仓位开仓后前 45 分钟 early-sl 和 breakeven-sl 均不触发（仅硬 SL 10% 兜底），给追涨/追跌入场的瞬时均值回归留缓冲；trail-tp 不受保护期影响 |
| F-L-01-15 | **追涨 24h 趋势过滤**（2026-04-24 新增，chase-entry）：24h change < -10% 的品种直接跳过，不追熊市反弹（避免抓下跌趋势中的技术性反弹飞刀）|
| F-L-01-09 | 平仓/撤单后 4h 冷却，同标的不重新进场 |

#### F-L-02 顶部做空（TOPSHORT）

**F-L-02-A 经典顶空**

| 需求项 | 描述 |
|--------|------|
| F-L-02-A-01 | 48h 内某时间点到当前涨幅 >= 80% |
| F-L-02-A-02 | 涨幅高峰后连续 6 根 1h K 线均未创新高 |
| F-L-02-A-03 | 现价 > 48h 最低价（仍在高位） |
| F-L-02-A-04 | 现价从高峰回落 <= 50%（还未跌穿） |
| F-L-02-A-05 | 品种需有 >= 12 天的 1h K 线历史数据 |
| F-L-02-A-06 | 开空仓，限价高于当前价 3%，受 24h 最高价约束 |
| F-L-02-A-07 | 止损 12%（可配置），止盈 20% |
| F-L-02-A-08 | 每 5 轮扫描（约 5 分钟）执行一次顶空扫描 |

**F-L-02-B Climax 顶空（1h 大阳实体 + 巨量见顶）**

| 需求项 | 描述 |
|--------|------|
| F-L-02-B-01 | 1h K 线振幅 (H-L)/O >= 4.5% |
| F-L-02-B-02 | 阳线实体 (C-O)/O >= 2.5% |
| F-L-02-B-03 | 实体占振幅比 (C-O)/(H-L) >= 42% |
| F-L-02-B-04 | 成交量 >= 前 20 根 1h K 线均量的 2 倍 |
| F-L-02-B-05 | 该 K 为过去 24 根 1h K 线中振幅最大的阳线（筋骨验证） |
| F-L-02-B-06 | 该 K 之后再过 2 根 1h 无更大振幅 K（确认顶部结构完成） |
| F-L-02-B-07 | 现价从高点回撤在 [1.2%, 48%] 区间内 |
| F-L-02-B-08 | 信号有效期：自领袖 K 收盘起 22h 内可开仓，26h 后自动撤单 |
| F-L-02-B-09 | 全局最多同时挂 1 张 Climax 做空限价单 |
| F-L-02-B-10 | 品种最少有 1.25 天 / 28 根 1h 历史数据 |
| F-L-02-B-11 | 上影线变体（TOPCLI_ALLOW_WICK）当前禁用 |

#### F-L-03 底部做多（BOTTOMLONG）

| 需求项 | 描述 |
|--------|------|
| F-L-03-01 | 1h K 线振幅 (H-L)/O >= 4.5% |
| F-L-03-02 | 阴线实体 (O-C)/O >= 2.5%，或下影线 (O-L)/O >= 2% |
| F-L-03-03 | 实体占振幅比 >= 42%（实体模式）或下影占振幅比 >= 34%（下影模式） |
| F-L-03-04 | 成交量 >= 前 20 根 1h K 线均量的 2 倍 |
| F-L-03-05 | 该 K 为过去 24 根 1h K 线中振幅最大的阴线（筋骨验证，底部镜像） |
| F-L-03-06 | 该 K 之后再过 2 根 1h 无更大振幅 K |
| F-L-03-07 | 现价从低点反弹在 [1.2%, 48%] 区间内 |
| F-L-03-08 | 信号有效期：自领袖 K 收盘起 22h 内可开仓，26h 后自动撤单 |
| F-L-03-09 | 全局最多同时挂 1 张 Climax 做多限价单 |
| F-L-03-10 | 止损 12%，止盈 20%，持仓 6h |

#### F-L-04 追跌（DUMP）

| 需求项 | 描述 |
|--------|------|
| F-L-04-01 | 过去 48 根 5m K 线（=4h）跌幅 >= 10% |
| F-L-04-02 | 现价距窗口最低点反弹 <= 8%（未回弹过深） |
| F-L-04-03 | 若同标的 CHASE 已有持仓，跳过 DUMP 开仓 |
| F-L-04-04 | 开空仓，限价高于当前价 3%，受 24h 最高价约束 |
| F-L-04-05 | 止损 8%，止盈 20%，持仓 6h |

#### F-L-05 通用风控

| 需求项 | 描述 |
|--------|------|
| F-L-05-01 | 日内熔断：同标的当日止损 >= 2 次，当天暂停所有新开仓 |
| F-L-05-02 | 同标的任何策略有持仓时，拒绝重复开仓（含跨子策略） |
| F-L-05-03 | 超时平仓：持仓达到最大时长自动市价平仓 |
| F-L-05-04 | 限价单挂单超过 1h 未成交自动撤单 |
| F-L-05-05 | 品种黑名单 = 硬编码 BASE **∪** DB 表 `symbol_blacklist`（WHERE is_active=1）。三个策略每 5 分钟从 DB 刷新一次缓存。BASE 里是历史列表（DENT/XAN/SUPER/GUN/UAI/AAVE_USD/BTC_USD/XVG/TRU/DEGO/ZRO/RIVER/Q/CHIP/SPK/UB），DB 供运行时动态增删 |
| F-L-05-05a | UI：`/symbol_blacklist` 页面顶部"策略永久禁用"卡片，支持添加 / 解除；即时写 DB，策略 5 分钟内生效；无需改代码 / commit / 重启进程 |
| F-L-05-06 | 反向滑点熔断：限价被反向穿越且偏离 > 1.5% 时撤单不填，避免逆势进场（LONG 价跌太深，SHORT 价涨太高）|
| F-L-05-07 | **限价触发 30s 观察确认**（2026-04-24 新增，live/whale/bigmid 均生效）：价格穿过挂单价时不立即成交，记录首次触发时间；下一轮若价格仍在触发侧 且 ≥ 30s 才成交；若在观察期内回撤到另一侧则清除观察、继续挂单等下次触发。避免急跌/急涨瞬穿即成交（接飞刀） |

---

### 4.2 庄家对抗引擎（strategy_whale）

#### F-W-01 信号评分系统

| 需求项 | 描述 |
|--------|------|
| F-W-01-01 | 资金费率评分（1~3分）：极端正费率为做空信号，极端负费率为做多信号 |
| F-W-01-02 | 多空比评分（1~2分）：多头 >= 60% 为做空信号 |
| F-W-01-03 | OI 变化评分（1~2分）：4h OI 下降 >= 1% 为做空信号 |
| F-W-01-04 | 放量滞涨/滞跌评分（2~3分）：成交量 >= 1.8倍均量 且 价格变化 < 1.5% |
| F-W-01-05 | 隐性大单评分（1分）：taker_buy_ratio < 42% 为卖压 |
| F-W-01-06 | 触发器：大阴线 >= 2.5% 或跌破支撑 0.5%（做空必须满足） |
| F-W-01-07 | 总分 >= 5 且触发器满足，才允许开仓 |

#### F-W-02 仓位管理

| 需求项 | 描述 |
|--------|------|
| F-W-02-01 | 单侧最多同时持仓 3 个（MAX_POS_PER_SIDE = 3） |
| F-W-02-02 | 止损 10%（可配置），止盈 20%（硬止盈，可配置） |
| F-W-02-03 | 动态移动止盈（与 strategy_live 同）：peak 3-5% 回落 1% / 5-10% 回落 2% / ≥10% 回落 3% |
| F-W-02-04 | 持仓最大 6h（多/空均为 6h，可配置） |
| F-W-02-05 | 限价挂单：偏移 0.3%（可配置），受 24h 高低价约束 |
| F-W-02-06 | 限价单超过 2h 未成交自动撤单 |

#### F-W-03 冷却机制

| 需求项 | 描述 |
|--------|------|
| F-W-03-01 | 正常平仓后冷却 6h，同标的不再进场 |
| F-W-03-02 | 止损平仓后冷却 12h（惩罚性冷却，比正常翻倍） |

#### F-W-05 W 型双底子策略（做多、短持、不设 SL/TP）

**2026-04-24 时间尺度调整**：从 1h K 线 + 14 天窗口 改为 **15m K 线 + 3.5 天窗口**；所有 bar 数常量数值不变（`_H` 后缀保留为历史兼容），实际时间尺度按 1/4 缩短。目的是抓更短周期的 W 形，出单更频繁以匹配更短的持仓节奏。

| 需求项 | 描述 |
|--------|------|
| F-W-05-01 | 数据要求：至少 **336 根 15m K 线 = 3.5 天**（2026-04-24 从 14 天 1h 改为 15m） |
| F-W-05-02 | 形态识别：最近 **3.5 天**最低点 B1 → 颈线反弹 ≥ 5% → 再次探底 B2 (B1 ± **5%**，2026-04-24 从 3% 放宽) → 突破颈线 **+0.5%**（2026-04-24 从 +1% 放宽） |
| F-W-05-03 | B2 距颈线 C ≥ 4 根（= 1h，过滤假探底）；B1 → B2 时间间隔 **6h - 3.5 天**（24 - 336 根 15m） |
| F-W-05-04 | 方向：LONG；限价偏移 0.3%（同 whale） |
| F-W-05-05 | **不设 SL**（浮亏不自动止损）/ **不设 TP**（不自动止盈）|
| F-W-05-06 | 持仓上限 **1 天**（2026-04-24 从 3 天缩短以匹配短周期形态，timeout 兜底自动平）|
| F-W-05-07 | 全策略全局最多同时 3 笔 w-bottom 持仓 |
| F-W-05-08 | 同品种触发后冷却 3 天 |
| F-W-05-09 | state key：`(strategy='whale', symbol, stype='w-bottom')` |
| F-W-05-10 | source 前缀：`strategy_whale:w-bottom` |

#### F-W-04 品种筛选

| 需求项 | 描述 |
|--------|------|
| F-W-04-01 | 每 30 分钟刷新一次活跃品种列表 |
| F-W-04-02 | 要求：最近 30 分钟有数据 AND 24h 成交量 > $5M |
| F-W-04-03 | 按 24h 成交量降序取前 200 |
| F-W-04-04 | 黑名单同 strategy_live |

---

### 4.3 中大市值引擎（strategy_bigmid）

#### F-BM-01 品种池

| 需求项 | 描述 |
|--------|------|
| F-BM-01-01 | 从 `price_stats_24h.quote_volume_24h` 筛选 >= $100M 的 USDT 永续 |
| F-BM-01-02 | 分档：BIG (>= $500M) / MID ($100M ~ $500M) |
| F-BM-01-03 | 硬排除 BIGMID_EXCLUDES：XAU/XAG/CL/TSLA（股票/商品衍生品）+ PIEVERSE（数据不全） |
| F-BM-01-04 | `1000*/USDT` 前缀默认排除；白名单 MEME_1000_WHITELIST = {`1000PEPE/USDT`} 放行 |
| F-BM-01-05 | 每 15 分钟刷新品种池，分档在运行时动态判定（成交量变化时自动升降档） |

#### F-BM-02 BIG 档：Whale 多维评分（取代 CHASE/DUMP）

**背景**：72h 回测证实 BIG 档用 CHASE/DUMP 不可行
- SL=2.5%/TP=5%/24h → -57U，-0.77%/笔
- SL=1%/TP=2.5%/8h → -121U，6/8 触 SL
- 根因：主流币 1h 涨 3% 不是趋势信号，回踩 1% 是常态
- 改用 Whale 多维评分后：+70U / 胜率 75%

**评分维度**（阈值按 BIG 币近 7 天真实分布校准）

| 维度 | 分数 | 做空条件 | 做多条件 |
|------|------|----------|----------|
| Funding rate | +1~+3 | ≥ ±0.001% / ±0.003% / ±0.005% | 镜像 |
| LSR (long_account) | +1~+2 | ≥ 0.70 / 0.75 | short_account ≥ 0.50 / 0.55 |
| OI 4h 变化 | +1~+2 | ≤ -1% / -2.5% | ≥ +1% / +2.5% |
| 放量滞涨/滞跌 | +2~+3 | vol_ratio ≥ 1.5/2.0 且 3h 价格变化 < 1% | 镜像 |
| Taker buy ratio | +1 | < 0.45 | > 0.55 |
| **触发器**（必须） | N/A | 1h 阴线 ≥ 0.8% 或跌破 4h 低 0.15% | 1h 阳线 ≥ 0.8% 或突破 4h 高 0.15% |

**评分门槛**：`entry_score_min = 3`（2026-04-24 放宽自 4；首日 48h 只触发 1 笔，降门槛提高触发频率）

**触发器阈值**：`trigger_candle_pct = 0.005`（1h 实体 ≥ 0.5%，2026-04-24 放宽自 0.8%）

**双向评分选优**：同时计算 short/long 评分，都过门槛+触发器时，取高分方开仓；同分偏向 short。

#### F-BM-03 MID 档：CHASE/DUMP（趋势追踪）

| 参数 | MID |
|------|-----|
| 时间框架 | 15m |
| CHASE 回看 / 阈值 / 单 bar | 24 根 (6h) / 6% / 1.5% |
| CHASE 耗竭 | 3% |
| DUMP 回看 / 阈值 / 反弹上限 | 48 根 (12h) / 5% / 4% |

#### F-BM-04 风控分档

| 参数 | BIG (whale) | MID (trend) |
|------|-------------|-------------|
| SL | 1% | 5% |
| 硬止盈 | 2% | 10% |
| 移动止盈激活 | 1.2%（BIG 档保留单档） | 动态：peak ≥3%/5%/10% 分档 |
| 移动止盈回落 | 0.3%（BIG 档保留单档） | 动态：1%/2%/3%（对应上方三档） |
| 限价偏移 | **0 (市价入场)** | 1.5% |
| 反向滑点熔断 | 0.3% | 0.75% |
| 最大持仓时长 | 4h | 12h |
| 限价挂单超时 | 2h（仅 MID 有挂单）|

#### F-BM-05 状态机与冷却

| 需求项 | 描述 |
|--------|------|
| F-BM-05-01 | 状态机 key：`(strategy='bigmid', symbol, stype)`，stype ∈ {`whale`, `chase`, `dump`} |
| F-BM-05-02 | BIG 档使用 stype='whale'（不分方向）；MID 档按子策略 stype='chase' / 'dump' |
| F-BM-05-03 | 状态机记录 `tier` 字段，平仓后冷却 4h |
| F-BM-05-04 | 订单/仓位 source 前缀：`strategy_bigmid:whale-entry` (BIG) / `strategy_bigmid:chase-entry` / `strategy_bigmid:dump-entry` (MID) |
| F-BM-05-05 | 持仓监控/状态复位按 source 识别 stype（含 "whale-entry" → stype='whale'） |

#### F-BM-06 与其他引擎的协同

| 需求项 | 描述 |
|--------|------|
| F-BM-06-01 | 共用 account_id=2；`_has_any_open(sym)` 跨策略检查，同标的不重复开 |
| F-BM-06-02 | `_fill_pending_orders` / `_monitor_positions` / `_settle_closed_positions` 全部按 source LIKE 'strategy_bigmid:%' 过滤，不触碰 strategy_live/whale 的订单和仓位 |
| F-BM-06-03 | v2 规划：MID 档加 Climax / TopShort / BottomLong；BIG Whale 加 trail-tp 分档；考虑 BIG LSR 按币自适应（BTC p50≈0.45 vs DOGE≈0.73） |

---

## 5. 非功能需求

| 类别 | 需求 |
|------|------|
| 可靠性 | 任何信号异常应 log.error 记录，不静默失败 |
| 配置热更新 | 所有可调参数从 system_settings DB 读取，重启生效 |
| 进程隔离 | 两个引擎独立进程，互不影响 |
| 编码 | 所有日志和输出 UTF-8，stdout.reconfigure |
| 轮询间隔 | strategy_live: 60s；strategy_whale: 90s |
| 并发保护 | 限价单使用 FILLING 中间态，防止重复触发 |

---

## 6. 配置参数清单

| 参数键 | 默认值 | 说明 |
|--------|--------|------|
| live_sl_pct | 0.10 | 趋势跟踪引擎止损比例 |
| live_hard_tp_pct | 0.20 | 趋势跟踪引擎硬止盈比例 |
| live_limit_offset_pct | 0.03 | 趋势跟踪引擎限价单偏移 |
| live_hold_hours | 6 | 趋势跟踪引擎最大持仓时长(h) |
| reverse_slippage_limit | 0.015 | 反向滑点熔断阈值（固定常量，暂不进 DB）|
| whale_sl_pct | 0.10 | 庄家对抗引擎止损比例 |
| whale_hard_tp_pct | 0.20 | 庄家对抗引擎硬止盈比例 |
| whale_limit_offset_pct | 0.003 | 庄家对抗引擎限价单偏移 |
| whale_hold_hours | 6 | 庄家对抗引擎最大持仓时长(h) |
| max_positions | 50 | 全局最大持仓数量 |
| live_trading_enabled | false | 实盘下单总开关 |

---

### 4.X 模拟盘 → 实盘同步（PaperLimitSyncService）

每 10s 扫描模拟盘新成交的开仓单，满足条件者在实盘同步开相同仓位（TP/SL 按百分比折算基于实盘成交价）。同步状态写 `futures_orders.live_sync_status`（NULL / SYNCED / FAILED）+ `live_position_id`。

| 需求项 | 描述 |
|--------|------|
| F-PS-01 | 总开关 `system_settings.live_trading_enabled=1` 时才运行，否则每 tick 直接返回 |
| F-PS-02 | 扫描条件（2026-04-24 扩展 + 去重）：`status='FILLED' AND side IN ('OPEN_LONG','OPEN_SHORT') AND live_sync_status IS NULL AND fill_time >= NOW() - INTERVAL 2 HOUR`，**LIMIT 和 MARKET 都同步**（之前仅 LIMIT，BIG 档市价单漏掉） |
| F-PS-03 | **去重条件 1**：JOIN `futures_positions fp ON fp.id=fo.position_id` 且 `fp.status='open'`——paper 仓已关则不再同步（防历史兜底单被当新单开仓）|
| F-PS-04 | **去重条件 2**：`NOT EXISTS (SELECT 1 FROM futures_orders fo2 WHERE fo2.position_id=fo.position_id AND fo2.id<>fo.id AND fo2.live_sync_status='SYNCED')`——同 paper_pid 已有别的 SYNCED 记录则跳过（防 LIMIT+MARKET 同一次成交被双开）|
| F-PS-05 | 失败不重试：失败即写 `live_sync_status='FAILED'`，避免重复下单 |
| F-PS-06 | 数量：`margin_per_trade × max_leverage / price`（实盘 API key 字段）|
| F-PS-07 | TP/SL：基于 paper 的 avg_fill_price 折算百分比，按实盘实际成交价重算绝对价格，避免验证失败 |

---

## 5. v1.2 更新汇总（2026-04-24 ~ 2026-04-25）

### 5.1 新增策略

#### F-F3 strategy_f3 W 底小涨带量做多（独立进程）
- 抓"已大跌但还未反弹"的底部第一根带量小阳，是 W 底的"反弹前一刻"变种
- 入场条件：7 天最大跌幅 ≥ 20%，最近 24h 未续跌（≥ -5%），脱离 24h 最低，**24h 涨跌 ≤ +2%（核心）**，最后 1 根 15m 阳线 1-3%，量比 1.5-3.0x
- 仓位：SL 5% / TP 10% / 持仓 12h / 冷却 4h，全局最多 3 仓
- 限价偏移 0.5%，超时 3h
- 黑白名单优先级：F3 白名单 > F3 黑名单 > 全局 BASE > DB 动态。**白名单覆盖一切黑名单**（F3 形态对某些"被 trend 策略坑过"的币仍然有效，如 SPK）
- 默认黑名单 7 个：PENGU/EVAA/IR/DUSK/GPS/MYX/AAVE_USD（基于 7d 回测数据）
- 默认白名单 9 个：SPK/NEIRO/AVNT/ZBT/KERNEL/TREE/STRK/ENJ/TRIA（同源数据）

#### F-W-06 strategy_whale M 顶子策略（**代码完成但默认禁用**）
- W 底完全镜像，方向 SHORT
- 阈值复用 W 底所有常量
- 主循环调用注释掉。14 天回测严格阈值 0 命中，宽松版负期望。市场转顶时取消注释启用

### 5.2 入场守卫（strategy_live 全子策略生效）

#### F-L-EG-01 入场位置百分位过滤
- 计算当前价在过去 3 小时（12 根 15m）K 线区间的百分位
- 规则（一票否决）：
  - `pos > 100%`（破顶，已突破上沿）：任何方向都拒绝
  - `pos < 0%`（破底，已跌穿下沿）：任何方向都拒绝
  - `pos > 90%` 且方向 LONG：追高拒绝
  - `pos < 10%` 且方向 SHORT：踩底拒绝
- 应用于：chase / dump / topshort-classic / topshort-climax / bottomlong-climax (5 处)

#### F-L-EG-02 24h 涨跌对称过滤
| 子策略 | 下限 | 上限 |
|---|---|---|
| chase LONG | -12%（2026-04-25 由 -10 放宽 2pt） | +15% |
| dump SHORT | -15% | — |
| **topshort-* SHORT** | **-15%** 新加 | — |
| **bottomlong-* LONG** | — | **+15%** 新加 |
- 做空类：24h 已大跌则不再开空（避免接飞刀）
- 做多类：24h 已大涨则不再开多（避免追末班车）
- 修复 04-25 实例：API3 LONG bottomlong-climax 在 24h +49% 还做多

### 5.3 限价单成交时机（4 个策略文件统一）

#### F-LM-01 替换 30s 时间确认 → 5m K 线方向确认
- 价格首次触达限价 → 记录 first_seen
- 等下一根**完整收盘的 5m K 线**
  - SHORT 限价：阴线（close < open）→ 成交
  - LONG 限价：阳线（close > open）→ 成交
  - 平 K（close == open）：算逆向，不成交
- 不成交时：清除等待，限价单**保留**给下次触发
- 反向滑点 1.5%（live）/ 0.75%（MID）/ 0.3%（BIG）熔断保留
- 限价总超时 1h（→ 2h）/ 2h / 3h 保留作为最终兜底

#### F-LM-02 七上八下限价定价
- 触发改用区间阻力/支撑位，替代单一 cur ± offset
- SHORT：`lp = max(4h_high × 0.80, cur × (1 + offset))`，受 24h_high 压制
- LONG：`lp = min(4h_low × 1.30, cur × (1 - offset))`，受 24h_low 支撑
- 4h 数据缺失时回退到 ± offset 默认值
- 应用于全部 4 个策略文件 11 处调用

#### F-LM-03 限价超时调整
| 策略 | 之前 | 现在 |
|---|---|---|
| strategy_live | 1h | **2h** |
| strategy_whale | 2h | 2h（不变）|
| strategy_bigmid | 2h | 2h（不变）|
| strategy_f3 | 1h | **3h** |

### 5.4 引擎层修复

#### F-ENG-01 max_profit_pct 字段刷新
- `futures_trading_engine.py` 两处 mark-price 刷新原本不更新 `max_profit_pct/max_profit_price/max_profit_time`，导致这些字段永远为 0
- 修复：刷新逻辑 `max_profit_pct = GREATEST(COALESCE(...), %s)`，price/time 同步刷新
- 影响：前端 / 复盘报表能看到正确的峰值浮盈，但 trail-tp 触发不变（因为它走 strategy 内存的 peak_pnl_pct）

### 5.5 strategy_bigmid BIG 白名单扩充
- 8 个 → ~60 个（CMC 前 50 + 币安永续活跃合约）
- 涵盖 L1/L2/DeFi 蓝筹、T1 memes、AI 热点、新生代、老牌活跃、跨链
- 仍受 TIER_BIG_MIN_VOL=$500M 约束

### 5.6 UI

#### F-UI-01 alert/confirm 替换为自定义 modal
- `static/js/modal.js` 已有 `showAlert / showConfirm / showToast`
- 5 处原生调用全替换：`futures_trading.html` / `symbol_blacklist.html` / `binance_news.html` / `auth.js` / `app.js`
- 跨页 JS（auth.js / app.js）保留原生 alert 作 fallback
- prompt 全项目 0 处使用，不需要替换

---

## 6. v1.3 更新汇总（2026-04-30 ~ 2026-05-03）

### 6.1 入场总开关（strategy_live）

#### F-L-EG-03 chase / dump 入场关闭
- 7 天 paper 数据复盘：chase 高位追多 37 笔 -789U / dump 低位杀跌 24 笔 -288U，结构性方向反
- `system_settings.chase_entry_enabled` / `dump_entry_enabled`（默认 1，当前 0）= 0 时 tick 入场逻辑直接 return
- 已有持仓的 SL/TP/trail-tp 出场不受影响，只关新入场
- topshort（顶部空 +271U）/ f3-entry / Gemini 反转型策略接管
- 60s 动态生效（不重启进程）

#### F-L-EG-04 dump / topshort 信号 30min 等待期（默认 OFF）
- 信号触发 → state=SIG_WAIT 等 30min 观察价格走势
- `*_signal_wait_enabled` / `*_signal_wait_min(30)` / `*_signal_adverse_pct(0.02)`
- 30min 内若价格往不利方向走超 2%，取消等待回 IDLE；否则 30min 满期再开仓
- 防止抢顶/抢底立即被反弹/反砸

### 6.2 strategy_bigmid 改造为 Gemini AI 决策

#### F-BM-GEMINI-01 替换原 BIG/MID 双档逻辑
- 原 strategy_bigmid 整体废弃 BIG（whale-entry）+ MID（chase/dump），改造为单一 Gemini AI 决策入口
- 28 个硬编码 GEMINI_TOP30 大币种，每 6h 跑一轮，prompt 含 15d daily / 4d 1h / 8h 15m+1h / RSI
- stype='gemini'，主参数 `gemini_sl_pct(0.02) / gemini_tp_pct(0.03) / gemini_limit_offset_pct(0.002) / gemini_hold_hours(6)`
- 风控门控 `gemini_min_pnl_pct(0.01) / gemini_max_open_positions(5) / gemini_symbol_cooldown_hours(24)`
- 总开关 `gemini_strategy_enabled`，默认 OFF，需手动启用
- 改 google-genai SDK，已修 collation 死锁 + active_count 含 DONE 死锁两个老坑

### 6.3 REV4D 子策略（strategy_whale 新增，2026-05-02）

#### F-W-REV4D-01 4 天 4H 极值反转
- 抓 4 天 96 根 4H K 线极值（最高/最低）当前价反转到极值附近 0.5% 内
- 同时 24h 涨跌幅过滤：触底反弹 LONG 要求 24h ≤ -8%，触顶反弹 SHORT 要求 24h ≥ +8%
- 仓位：SL 2% / TP 5% / 持仓 24h / cooldown 48h
- `rev4d_enabled / rev4d_threshold_pct(0.005) / rev4d_sl_pct(0.02) / rev4d_tp_pct(0.05) / rev4d_hold_hours(24) / rev4d_cooldown_hours(48)`
- 默认 OFF，2026-05-02 上线

### 6.4 SWAN 子策略（strategy_whale 新增，2026-05-03）

#### F-W-SWAN-01 Gemini 红黑天鹅自动下限价单
- 数据源：远程 dimesion.gemini_swan_verdicts（每 2h 跑一次 3 轮聚合）
- red_swan → LONG（极端正向尾部 / 暴涨 / 空头挤压）
- black_swan → SHORT（极端负向尾部 / 急跌 / 多头踩踏）
- 仅消化 STRONG 一致性（3 轮中至少 2 轮同方向）+ avg_confidence ≥ 0.70
- 仓位：SL 2% / TP 5% / 持仓 12h / cooldown 12h（同 symbol 平仓后）
- 限价偏移 0.3%，5 仓上限 (`swan_max_open=5`)
- 24h 涨跌守卫：LONG 不追 24h > +30%，SHORT 不接 24h < -25% 飞刀
- `swan_strategy_enabled / swan_min_confidence(0.70) / swan_max_open(5) / swan_hold_minutes(720) / swan_cooldown_hours(12)`
- 默认 OFF
- 进度游标 `swan_last_run_id` 防止重复处理同一轮 verdict（60s reload 自动同步）

#### F-W-SWAN-02 红黑天鹅榜前端
- 桌面页 `/swan_board`（替换原币本位合约空壳页）+ 移动端 `/m/swan`
- 后台每 2h 自动跑一次（`gemini_swan_enabled=1`）+ "立即重跑"手动触发
- 双列卡片（红/黑）按 STRONG/MODERATE/WEAK 一致性等级展示，最高 conf 排序
- 移动端 4 tab 底部导航（设置/U本位/天鹅/实盘）

### 6.5 配置动态加载（2026-05-01）

#### F-CFG-RELOAD-01 4 个进程 60s 自动 reload
- strategy_live / whale / bigmid / f3 主循环每 60s 调 `_load_*_config()` 重读 system_settings
- 改 DB 后 60s 内自动生效，不再需要重启进程
- 少数仍需重启的场景：Gemini Client 实例 (`_init_gemini_client`)，硬编码 GEMINI_TOP30 列表

### 6.6 限价单撤单 state 同步修复（2026-05-03）

#### F-LM-CANCEL-SYNC-01 撤单同步回收 strategy_state
- **问题**：`_fill_pending_orders` 把 PENDING 限价单标 CANCELLED 时只更新 futures_orders，没碰 strategy_state
- **后果**：state 卡 PENDING 不释放，子策略 active_count 永远满槽，新候选全部 skip
- **触发实例**：5/3 SWAN 子策略 5 个槽位被 BABY/B/KNC + AXL/BR 五笔僵尸 PENDING 死占，run 9-10 两轮 STRONG 候选全被挡
- **根因叠加**：
  - strategy_whale / strategy_live 的 `_fill_pending_orders` 跨进程乱扫单（没按 order_source 过滤）
  - strategy_whale 的 `_check_pending_db` 写死 stype='whale'，子策略（swan/rev4d/longhold-w/m/w-bottom/m-top）无兜底
- **修复**：5 处 CANCELLED 写入点全部加 state 同步：
  - strategy_whale.py timeout 撤单（按 order_source 解析 stype）
  - strategy_live.py timeout / reverse_slippage 撤单（按 order_source 跨进程同步）
  - strategy_bigmid.py / strategy_f3.py timeout 撤单（防御性显式同步，原本有 _settle/_check 兜底）

### 6.7 数据查询 / 诊断脚本规范

#### F-DIAG-01 strategy_state × futures_orders 不能直接 JOIN
- 两表 order_id 列 collation 不同（utf8mb4_unicode_ci vs utf8mb4_general_ci），JOIN 抛 1267
- 修法：拆两步查询（先取 PENDING 列表 → 再 IN 反查 → Python 端 join）
- 历史踩坑：commit 0b09a17b 修过 strategy_bigmid `_settle_cancelled_pending`，2026-05-03 fix_swan_zombie_pending.py 又踩同一坑

#### F-DIAG-02 SWAN 全链路诊断脚本
- `scripts/diag/diag_swan_strategy_status.py`：6 段连查 system_settings / verdicts / state / orders
- `scripts/diag/fix_swan_zombie_pending.py`：兜底清理已存在的僵尸 PENDING（dry-run / --yes / 交互三档）
