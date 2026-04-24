# 设计文档 — 量化交易系统

版本：v1.1  
更新日期：2026-04-23  
覆盖范围：strategy_live（小币趋势跟踪）、strategy_whale（庄家对抗）、strategy_bigmid（中大市值引擎 MVP）

---

## 1. 系统架构

```
┌──────────────────────────────────────────────────────────┐
│                    FastAPI 服务 (port 9021)               │
│  /api/futures/open  /api/futures/close  /api/system/*    │
│  FuturesTradingEngine (纸仓) │ BinanceFuturesEngine (实盘) │
└────┬─────────────────┬──────────────────┬─────────────────┘
     │ HTTP            │ HTTP             │ HTTP
┌────┴───────┐   ┌─────┴──────┐   ┌───────┴──────────┐
│strategy_   │   │strategy_   │   │strategy_bigmid    │
│  live      │   │  whale     │   │  (MVP: CHASE/DUMP)│
│POLL=60s    │   │POLL=90s    │   │POLL=60s           │
│state='live'│   │='whale'    │   │='bigmid'          │
│source=     │   │source=     │   │source=            │
│ strategy_  │   │ strategy_  │   │ strategy_bigmid:  │
│ live:X     │   │ whale:X    │   │ {chase|dump}-entry│
└────┬───────┘   └─────┬──────┘   └───────┬──────────┘
     │                 │                   │
┌────┴─────────────────┴───────────────────┴──────────────┐
│                  MySQL (dimesion)                        │
│  kline_data / futures_positions / futures_orders         │
│  strategy_state / system_settings / price_stats_24h      │
│  funding_rates / long_short_ratio / open_interest        │
└──────────────────────────────────────────────────────────┘

三引擎共用 ACCOUNT_ID=2；状态机按 strategy 字段隔离；
订单/持仓按 source 前缀区分，互不干扰。
```

### 1.3 品种黑名单动态管理

```
有效黑名单 = 硬编码 BASE（模块顶部 set）∪ symbol_blacklist 表（is_active=1）
```

- BASE 是"系统永久级"（下架币、已知坏币），写死在代码
- `symbol_blacklist` 表（023 迁移）是"运行时动态"，UI 可增删
- 每个策略进程 5 分钟（`_DB_BLACKLIST_REFRESH_S=300`）从 DB 刷新缓存
- 品种池刷新时使用合并后的黑名单

API: `GET/POST/DELETE /api/symbol_blacklist`
UI: `/symbol_blacklist` 页面顶部卡片

### 1.1 进程模型

- 每个引擎是独立 Python 进程，定时轮询，无 web 框架。
- 与 FastAPI 服务通过本地 HTTP 通信（不共享内存）。
- 状态持久化在 `strategy_state` 表（字段 state_json JSONB）。

### 1.2 数据库关键表

| 表 | 用途 |
|----|------|
| `kline_data` | 5m/1h K 线，含 volume/open/high/low/close/open_time |
| `futures_positions` | 持仓记录，含 status/notes/close_time/realized_pnl |
| `futures_orders` | 限价单记录，含 status(PENDING/FILLING/FILLED/CANCELLED) |
| `strategy_state` | 各标的各策略的状态机 state_json |
| `system_settings` | 可配置参数 key-value 表 |
| `funding_rates` | 资金费率时序 |
| `long_short_ratio` | 多空比时序 |
| `open_interest` | 持仓量时序 |
| `aggr_trades` / `kline_data` | taker_buy_ratio（成交量中主动买入占比） |

---

## 2. 策略状态机

所有子策略共用同一套状态机（`strategy_state` 表）。

```
IDLE ──────────────────────────────────────────────────────▶ IDLE
  │                                                           ▲
  │ 信号触发                                                  │ 冷却到期
  ▼                                                           │
PENDING (限价单已挂，等待成交)                               DONE
  │                                                           ▲
  │ 成交 (fill_price 触发)                                    │
  ▼                                                           │
LONG / SHORT (持仓中)                                        │
  │                                                           │
  ├── SL 触发 (亏损 >= sl_pct)          ──── close → ────────┤
  ├── TP 触发 (盈利 >= hard_tp_pct)    ──── close → ────────┤
  ├── Trail TP 触发                     ──── close → ────────┤
  └── 超时 (hold_min 到期)              ──── close → ────────┘
```

状态字段（state_json 内）：

| 字段 | 类型 | 说明 |
|------|------|------|
| state | str | IDLE / PENDING / LONG / SHORT / DONE |
| pid | int | 持仓 ID |
| order_id | int | 挂单 ID |
| entry_p | float | 开仓价 |
| peak_pnl_pct | float | 历史最高盈利率 |
| entry_time | float | 开仓时间戳(s) |
| done_time | float | 完成时间戳(s，用于冷却计算) |
| last_reason | str | 最近平仓原因(stop_loss/hard-tp/trail-tp/timeout) |

---

## 3. 趋势跟踪引擎（strategy_live）详细设计

### 3.1 初始化与配置加载

```python
# 启动时调用 _load_live_config()
# 从 system_settings 读取以下键：
LIVE_SL_PCT           = 0.10    # live_sl_pct
LIVE_HARD_TP_PCT      = 0.20    # live_hard_tp_pct
LIVE_LIMIT_OFFSET_PCT = 0.03    # live_limit_offset_pct
LIVE_HOLD_H           = 6       # live_hold_hours

# 加载后同步覆盖所有子策略常量：
CHASE_SL_PCT = TOP_SL_PCT = BOTLONG_SL_PCT = DUMP_SL_PCT = LIVE_SL_PCT
HARD_TP_PCT    = LIVE_HARD_TP_PCT
LONG_HOLD_MIN = SHORT_HOLD_MIN = LIVE_HOLD_H * 60  # -> 360 min
```

### 3.2 子策略一：CHASE（追涨）

#### 3.2.1 数据输入

- 数据源：`kline_data` WHERE `timeframe='5m'` AND `symbol=sym`
- 取最近 `CHASE_PUMP_BARS + 2 = 26` 根，使用 `completed`（排除未收盘最新一根）

#### 3.2.2 信号判断流程

```
Step 1: 涨幅检测
  window_bars = completed[-24:]  # 最近24根5m K线
  pump_pct = (close[-1] - open[window_bars[0]]) / open[window_bars[0]]
  IF pump_pct < CHASE_PUMP_PCT(0.12): return  # 不足12%，跳过

Step 2: 耗竭过滤
  recent_high = max(bar.high for bar in window_bars)
  dd_from_peak = (recent_high - close[-1]) / recent_high
  IF dd_from_peak > CHASE_EXHAUST_MAX_DD(0.06): return  # 从高点回撤>6%，顶部耗竭

Step 3: 急拉验证
  leader_gain = max((bar.close - bar.open)/bar.open for bar in window_bars)
  IF leader_gain < CHASE_LEADER_BAR_MIN_PCT(0.03): return  # 无单bar涨3%，慢速爬升

Step 4: 方向冲突检查
  ts_row = get_or_create(conn, 'live', sym, 'topshort')
  IF ts_row['state'] not in ('IDLE','DONE'): return  # 顶空持仓中，避免对冲

Step 5: 状态检查
  s = chase_row['state']
  IF s != 'IDLE': return  # 非空闲状态

Step 6: 开仓
  lp = _calc_limit_price('LONG', price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT)
  open_order(sym, 'LONG', price, HARD_TP_PCT, CHASE_SL_PCT, CHASE_MAX_HOLD, 'chase-entry', lp)
  update_state(state='PENDING')
```

#### 3.2.3 限价计算

```python
def _calc_limit_price(side, cur_price, high_24h, low_24h, pct):
    if side == 'LONG':
        lp = cur_price * (1 - pct)          # 低挂 3%
        lp = max(lp, low_24h) if low_24h else lp   # 不低于24h最低
    else:  # SHORT
        lp = cur_price * (1 + pct)          # 高挂 3%
        lp = min(lp, high_24h) if high_24h else lp  # 不高于24h最高
    return round(lp, 8)
```

#### 3.2.4 冷却与重置

- 成功开仓 → state = PENDING
- 成交 → state = LONG
- 平仓 → state = DONE，done_time = now_s()
- 下次循环检查：`now_s() - done_time >= CHASE_COOLDOWN(14400s)` → state = IDLE

---

### 3.3 子策略二：TOPSHORT（顶部做空）

#### 3.3.1 经典顶空（topshort_classic_tick）

**数据要求：** 1h K 线，最少 `TOP_LOOKBACK_H(48) + TOP_NO_NEW_H(6) = 54` 根。

```
Step 1: 历史数据充分性
  IF count(1h bars) < TOPSHORT_MIN_HISTORY_MS(12天) / 3600s: return

Step 2: 扫描48h内峰值
  FOR i in range(len(bars) - TOP_NO_NEW_H, len(bars)):
      pump_pct = (bars[i].high - min_low_before_i) / min_low_before_i
      IF pump_pct < TOP_PUMP_THRESH(0.80): continue  # 涨幅不足80%，跳过

Step 3: 6h无新高确认
  peak_high = bars[i].high
  IF any(bar.high > peak_high for bar in bars[i+1:i+7]): continue  # 6h内有新高

Step 4: 现价验证
  IF cur_price <= 48h_min_low: return   # 已跌穿
  IF (peak_high - cur_price)/peak_high > 0.50: return  # 已跌超50%

Step 5: 开仓
  lp = _calc_limit_price('SHORT', price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT)
  open_order(sym, 'SHORT', price, HARD_TP_PCT, TOP_SL_PCT, TOP_HOLD_H*60, 'topshort', lp)
```

#### 3.3.2 Climax 顶空（topshort_climax_tick）

**领袖K识别（大阳实体模式）：**

```python
# 条件全部为 AND 关系
body_pct   = (close - open) / open                  # 实体占open
range_pct  = (high - low) / open                    # 振幅占open
body_ratio = (close - open) / (high - low)          # 实体占振幅

leader_candle = body_pct   >= TOPCLI_MIN_BODY_VS_O(0.025)   # 实体>=2.5%
             AND range_pct  >= TOPCLI_MIN_RANGE_FULL_PCT(0.045) # 振幅>=4.5%
             AND body_ratio >= TOPCLI_MIN_BODY_OF_RANGE(0.42)   # 实体占振幅>=42%
             AND volume     >= avg_vol_20 * TOPCLI_VOL_MULT(2.0) # 放量2倍
```

**筋骨验证（确保是最强K）：**

```python
# 取最近24根1h K线（TOPCLI_LEADER_LOOKBACK=24）
# 领袖K必须是这24根中振幅最大的阳线
# 领袖K索引 <= n-1-POST_LEADER_WAIT_BARS(2)：即后面还有>=2根1h K未超越它
```

**走弱确认（当前价位验证）：**

```python
dd_from_peak = (leader_high - cur_price) / leader_high
# 必须满足：TOPCLI_PULLBACK_FR(0.012) <= dd_from_peak <= TOPCLI_MAX_DD_FR(0.48)
# 即从高点回落 1.2% ~ 48% 区间内
```

**信号时效：**

```python
leader_close_ts = leader_bar.open_time + 3600000  # 领袖K收盘时刻
age_ms = now_ms() - leader_close_ts
# 开仓窗口：age_ms <= TOPCLI_SIGNAL_AGE_MS(22h=79200000ms)
# 撤单窗口：age_ms > TOPCLI_MAX_OPEN_AGE_MS(26h=93600000ms) → 撤单
```

---

### 3.4 子策略三：BOTTOMLONG（底部做多）

经典顶空的完全镜像逻辑，方向取反：

| TOPSHORT | BOTTOMLONG |
|----------|------------|
| 大阳实体 + 巨量 | 大阴实体 + 巨量 |
| 上影线变体 | 下影线变体 |
| body = (C-O)/O | body = (O-C)/O |
| pump_to_high = (H-O)/O | drop_to_low = (O-L)/O |
| dd = (peak-cur)/peak | bounce = (cur-low)/low |
| 开 SHORT | 开 LONG |
| SL 12% | SL 12% |

---

### 3.5 子策略四：DUMP（追跌）

```
Step 1: 跌幅检测
  window_bars = completed[-48:]  # 最近48根5m K线（=4h）
  dump_pct = (open[window_bars[0]] - close[-1]) / open[window_bars[0]]
  IF dump_pct < DUMP_PCT(0.10): return  # 不足10%

Step 2: 反弹过滤
  min_low = min(bar.low for bar in window_bars)
  bounce = (close[-1] - min_low) / min_low
  IF bounce > 0.08: return  # 已反弹超8%，不追

Step 3: 冲突检查（同CHASE）
  IF _has_any_open(sym): return

Step 4: 开仓
  lp = _calc_limit_price('SHORT', price, h24, l24, pct=LIVE_LIMIT_OFFSET_PCT)
  open_order(sym, 'SHORT', price, HARD_TP_PCT, DUMP_SL_PCT, DUMP_MAX_HOLD, 'dump-entry', lp)
```

---

### 3.6 出场逻辑总览（strategy_live / strategy_whale / bigmid MID 共用）

**动机**：原单档 trail（peak ≥ 12% 回落 2%）启动太晚；硬 SL 10% 太宽，单笔最大亏损相当于 50% margin（5x 杠杆下）。加**早期止损**和**保本止损**两条兜底，配合动态 trail 形成完整出场链。

**执行顺序（仅在 DISABLE_SL_TP_HOLD=OFF 时）**：

| 优先级 | 规则 | 触发条件 | 关闭原因 |
|--------|------|----------|----------|
| 1 | hard-tp | `pnl ≥ 20%` | `hard-tp` |
| 2 | trail-tp（动态分档） | peak 分档对应回落阈值触发（见下表） | `trail-tp` |
| 3 | breakeven-sl | `peak ≥ 3%` 且 `pnl ≤ -0.5%` | `breakeven-sl` |
| 4 | early-sl | `pnl ≤ -3%` | `early-sl` |
| 5 | stop_loss（paper engine 兜底） | `pnl ≤ -10%` | `stop_loss` |

**动态 trail 分档（TRAIL_TP_TIERS）**：

| peak 区间 | 回落阈值 | 设计意图 |
|-----------|----------|----------|
| `[3%, 5%)` | 1% | 小赚紧盯 |
| `[5%, 10%)` | 2% | 中赚放松 |
| `≥ 10%` | 3% | 大赚奔跑 |
| `< 3%` | ∞（不触发） | |

**早期止损 / 保本止损常量**：
- `EARLY_SL_PCT = 0.03`         单笔价格反向 3% 立即平，单笔最大亏损降到 15% margin
- `BREAKEVEN_AFTER_PEAK_PCT = 0.015` peak 达 1.5% 后进入"赚过钱"状态（2026-04-24 从 3% 降低：补 peak 1-3% 保护盲区）
- `BREAKEVEN_SL_PCT = -0.005`   赚过钱的单若回吐到 -0.5% 立即平（防盈利单翻亏）

**伪代码**：

```python
def _check_exit(pid, pnl_pct, peak_pnl_pct):
    if DISABLE_SL_TP_HOLD: return False    # 裸奔模式：任由自生自灭
    new_peak = max(pnl_pct, peak_pnl_pct)
    update_state(peak_pnl_pct=new_peak)

    if pnl_pct >= HARD_TP_PCT:                                    return close("hard-tp")
    pullback = _dynamic_trail_pullback(new_peak)
    if (new_peak - pnl_pct) >= pullback:                          return close("trail-tp")
    if new_peak >= BREAKEVEN_AFTER_PEAK_PCT and pnl_pct <= BREAKEVEN_SL_PCT:
                                                                  return close("breakeven-sl")
    if pnl_pct <= -EARLY_SL_PCT:                                  return close("early-sl")
    return False   # 落到 SL 兜底
```

**BIG 档（strategy_bigmid TIER_PARAMS["BIG"]）不适用**：
- TP 只有 2%，peak 基本到不了 3%（动态 trail + 保本都不会触发）
- SL 只有 1%，early-sl 3% 根本不会先触发
- 保留原有单档 `trail_tp_start=1.2%, trail_tp_pullback=0.3%` 作为兜底

### 3.6a 出场规则的双路执行：策略 60s 轮询 + Monitor 1s 轮询

为了抓住小币快速穿越，上述 5 条规则同时由 **两处** 执行，先触发者胜：

| 执行者 | 频率 | 数据源 | 职责 |
|--------|------|--------|------|
| 策略进程 `_trail_tp_check` | 60s（跟随策略主循环） | strategy_state.peak_pnl_pct | 慢路径，兼容历史 |
| **`PositionSLTPMonitor`** | **1s** | 进程内 `_peak_pnl_map` 内存维护 | 快路径，抓瞬时 |

Monitor 行为：
- 每 1s 扫 `futures_positions status='open' 且 sl/tp 非空`
- 用 `/api/futures/price` 端点的 L2 内存字典或 Binance fallback 取最新价
- 算 `pnl_pct = (price - entry) / entry`，按 side 取符号
- 内存 `_peak_pnl_map[pid]` 更新最大值
- 按优先级判定 trail-tp → breakeven-sl → early-sl → 硬 SL/TP
- 触发后 HTTP 调 `/api/futures/close/{pid}` 平仓
- 受 `disable_sl_tp_hold` 开关控制：ON 时跳过新三条规则，仅硬 SL/TP 兜底（但这种仓位通常没设 SL/TP，所以整体"自生自灭"）

Monitor 的 peak 是**进程内内存**，FastAPI 重启时丢失；对持仓 < 1-2h 的仓位影响小，策略端的 60s 轮询会在 peak 重建后接上。

---

### 3.7 日内熔断（open_order 入口）

```python
def open_order(sym, direction, ...):
    # 检查1：已有持仓
    if _has_any_open(sym): return None, None, False

    # 检查2：日内熔断
    daily_sl = _symbol_daily_sl_count(sym)
    # 查询：futures_positions WHERE account_id=ACCOUNT_ID AND symbol=sym
    #        AND status='closed' AND close_time >= CURDATE()
    #        AND notes='stop_loss'
    if daily_sl >= SYMBOL_MAX_DAILY_SL(2):
        return None, None, False

    # 正常开仓流程...
```

---

### 3.8 限价单填充机制（_fill_pending_orders）

```python
# 每主循环执行
# 1. 查所有 PENDING LIMIT 订单
# 2. 超时检查：
#    age_s = (now - created_at).total_seconds()
#    IF age_s > LIMIT_PENDING_MAX_S(3600): 标记 CANCELLED
# 3. 检查是否满足触发条件：
#    LONG: cur_price <= limit_price
#    SHORT: cur_price >= limit_price
# 4. 反向滑点熔断（见 3.8.1）：
#    LONG:  reverse_slip = (limit_p - cur_p) / limit_p
#    SHORT: reverse_slip = (cur_p - limit_p) / limit_p
#    IF reverse_slip > REVERSE_SLIPPAGE_LIMIT(0.015):
#        UPDATE status='CANCELLED', cancellation_reason='reverse_slippage_XXXX'
#        continue  # 跳过本单，不进入 FILLING
# 5. 触发且未熔断时：
#    a. UPDATE status='FILLING'（乐观锁防并发）
#    b. 按成交价重算 SL/TP（偏离 > 0.1% 时，见 3.8.2）
#    c. POST /api/futures/open with fill_price=cur_price
#    d. UPDATE status='FILLED', position_id=pid
```

#### 3.8.1 反向滑点熔断

**动机：** 信号触发时价格合理，但挂单挂到成交的窗口内价格可能向不利方向跑 1-10%。原先只做 SL/TP 重算（被动补救），命中率仍然差——需要主动拒绝进场。

**判断：**

| 方向 | 意义 | 触发后反向偏离计算 | 阈值命中 → 动作 |
|------|------|---------------------|------------------|
| LONG | 挂低价等买 | `(limit_p - cur_p) / limit_p` | `> 0.015` → CANCELLED |
| SHORT | 挂高价等卖 | `(cur_p - limit_p) / limit_p` | `> 0.015` → CANCELLED |

**实例：** CHIP 4/23 05:19 SHORT dump-entry，limit=0.09579，cur=0.09846，反向偏离 2.79%（> 1.5%）→ 应撤单，避免 80 分钟后 -260 U 止损。

**阈值取值理由：** strategy_live LIMIT_OFFSET_PCT = 3%，选其一半 1.5% 作为反向滑点阈值；太紧会误伤正常滑点（~0.5%），太松无法拦住反向动量单。

#### 3.8.2 SL/TP 基于成交价重算

保留原有机制：若 `abs(cur_p - limit_p) / limit_p > 0.001`，按原始 SL/TP 比例以 `cur_p` 为锚重算，防止限价被小幅穿越时 SL 幅度被压缩。

---

## 4. 庄家对抗引擎（strategy_whale）详细设计

### 4.1 初始化与配置加载

```python
# 启动时调用 _load_whale_config()
WHALE_SL_PCT           = 0.10    # whale_sl_pct
WHALE_HARD_TP_PCT      = 0.20    # whale_hard_tp_pct
WHALE_LIMIT_OFFSET_PCT = 0.003   # whale_limit_offset_pct
WHALE_HOLD_H           = 6       # whale_hold_hours

SL_PCT      = WHALE_SL_PCT
HARD_TP_PCT = WHALE_HARD_TP_PCT
SHORT_HOLD_H = LONG_HOLD_H = WHALE_HOLD_H
```

### 4.2 品种筛选

```python
# 每 30 分钟从 kline_data 取最新活跃品种
SELECT DISTINCT symbol FROM kline_data
WHERE timeframe='1h'
  AND open_time >= UNIX_TIMESTAMP(NOW() - INTERVAL 30 MINUTE) * 1000
  AND volume_usdt > 5000000   # 24h成交量 > $5M
ORDER BY volume_usdt DESC
LIMIT 200
```

### 4.3 信号评分计算（score_signal）

```python
def score_signal(sym, direction) -> (int, bool):
    score = 0
    triggered = False

    # ── 数据采集 ──────────────────────────────────
    # 取最近 3 根 1h K线（用于计算近期状态）
    recent3 = SELECT FROM kline_data WHERE timeframe='1h' ORDER BY open_time DESC LIMIT 3
    # 取最近 20 根 1h K线（用于计算均量）
    vol20   = SELECT avg(volume) FROM kline_data ... LIMIT 20
    # 取最新资金费率
    fr      = SELECT funding_rate FROM funding_rates WHERE symbol=sym ORDER BY ts DESC LIMIT 1
    # 取最新多空比
    lsr     = SELECT long_pct FROM long_short_ratio WHERE symbol=sym ORDER BY ts DESC LIMIT 1
    # 取最近 4h OI 变化
    oi_chg  = (latest_oi - oi_4h_ago) / oi_4h_ago

    # ── 评分逻辑（以做空方向为例）─────────────────
    # 1. 资金费率评分
    if fr >= FR_EXTREME_HIGH(0.0005): score += 3
    elif fr >= FR_HIGH(0.0003):       score += 2
    elif fr >= FR_MILD_HIGH(0.0001):  score += 1

    # 2. 多空比评分
    long_pct = lsr['long_pct']
    if long_pct >= LS_LONG_EXTREME(0.65): score += 2
    elif long_pct >= LS_LONG_HIGH(0.60):  score += 1

    # 3. OI变化评分
    if oi_chg <= OI_DROP_STRONG(-0.03): score += 2
    elif oi_chg <= OI_DROP_MILD(-0.01): score += 1

    # 4. 放量滞涨评分
    vol_ratio = avg(recent3.volume) / vol20
    price_chg = (recent3[-1].close - recent3[0].open) / recent3[0].open
    if vol_ratio >= VOL_RATIO_STRONG(2.5) and abs(price_chg) < STALE_PRICE_PCT(0.015):
        score += 3
    elif vol_ratio >= VOL_RATIO_MILD(1.8) and abs(price_chg) < STALE_PRICE_PCT(0.015):
        score += 2

    # 5. 隐性卖压评分
    avg_taker = avg(recent3.taker_buy_ratio)
    if avg_taker < TAKER_SELL_THRESH(0.42): score += 1

    # 6. 触发器（必须）
    cur_price = recent3[-1].close
    big_candle = (recent3[-1].open - cur_price) / recent3[-1].open >= TRIGGER_CANDLE_PCT(0.025)
    support_break = cur_price < min(recent3[-4:-1].low) * (1 - TRIGGER_BREAKOUT(0.005))
    triggered = big_candle or support_break

    return score, triggered
```

### 4.4 开仓决策（whale_tick）

```python
def whale_tick(sym, conn):
    ss = get_or_create(conn, 'whale', sym, 'whale', {'state': 'IDLE'})

    # 状态检查
    if ss['state'] in ('SHORT', 'LONG', 'PENDING'):
        return _monitor_position(ss, sym)   # 监控已有仓位

    # 冷却检查
    anchor = ss.get('done_time', 0)
    cd = COOLDOWN_SL_S(43200) if ss.get('last_reason') == 'stop_loss' else COOLDOWN_S(21600)
    if now_s() - anchor < cd: return

    # 信号评分
    score, triggered = score_signal(sym, 'short')
    if score < ENTRY_SCORE_MIN(5) or not triggered: return

    # 并发限制
    if count_open_positions('SHORT') >= MAX_POS_PER_SIDE(3): return

    # 开仓
    price = get_price(sym)
    h24, l24 = _get_24h_stats(cur, sym)
    lp = _calc_limit_price('SHORT', price, h24, l24)
    # lp = price * (1 + WHALE_LIMIT_OFFSET_PCT(0.003))  受h24约束
    pid, oid, pending = open_order(sym, 'SHORT', price, HARD_TP_PCT, SL_PCT, SHORT_HOLD_H*60, 'whale-short', lp)
    update_state(state='SHORT', pid=pid, entry_p=lp, peak_pnl_pct=0.0)
```

### 4.5 移动止盈（与 strategy_live 相同，动态分档）

见 3.6。strategy_whale 的 `_trail_tp_check` 调用同一个 `_dynamic_trail_pullback(peak)` 分档表。

### 4.6 冷却机制

| 平仓原因 | 冷却时长 | 对应常量 |
|----------|----------|----------|
| stop_loss | 12 小时 | COOLDOWN_SL_S = 43200 |
| 其他（tp/timeout） | 6 小时 | COOLDOWN_S = 21600 |

---

## 4a. 中大市值引擎（strategy_bigmid）详细设计

### 4a.1 设计动机

现有 strategy_live 的信号阈值全部按小币 5m 波动（p50 振幅 2-3%，p99 振幅 10%+）设计，在主流大币（BTC/ETH/SOL 5m 振幅 p50 0.15-0.35%、p99 < 1.5%）上**永远无法触发**。近 7 天大币列表（BTC/ETH/SOL/DOGE/XRP/BNB 等）策略开仓数 = 0。

strategy_bigmid 不修改 strategy_live，改用独立进程 + 按档位缩放的阈值表，让大币也能被捕捉。

### 4a.2 分档规则

```python
TIER_BIG_MIN_VOL = 500_000_000   # >= $500M/24h
TIER_MID_MIN_VOL = 100_000_000   # >= $100M/24h

def get_tier(sym, vol_map):
    v = vol_map.get(sym, 0)
    if v >= TIER_BIG_MIN_VOL: return "BIG"
    if v >= TIER_MID_MIN_VOL: return "MID"
    return None   # 跌出池

# 排除
BIGMID_EXCLUDES = {"XAU/USDT","XAG/USDT","CL/USDT","TSLA/USDT","PIEVERSE/USDT"}
MEME_1000_WHITELIST = {"1000PEPE/USDT"}
```

品种池每 15 分钟从 `price_stats_24h` 刷新；成交量波动可致品种升降档（如 HYPE 从 MID 升 BIG）。

### 4a.3 分档策略差异

| 参数 | BIG (whale) | MID (trend) | 说明 |
|------|-------------|-------------|------|
| 策略类型 kind | whale | trend | 路由按 kind 分发 |
| tf | 1h | 15m | 时间框架 |
| 入场逻辑 | 多维评分 + 触发器 | CHASE/DUMP 单指标 | |
| 入场评分门槛 | **3 分**（2026-04-24 放宽自 4 分） | n/a | |
| sl_pct | 1% | 5% | |
| hard_tp_pct | 2% | 10% | |
| trail_tp_start / pullback | 1.2% / 0.3% | 6% / 1% | |
| limit_offset_pct | **0 (市价)** | 1.5% | BIG 限价单在主流币几乎填不到，用市价 |
| reverse_slippage | 0.3% | 0.75% | MID 档挂单才有意义；BIG 市价无穿越 |
| hold_min | 4h | 12h | |

### 4a.4 BIG Whale 评分阈值表（按 7 天真实分布校准）

```python
BIG Whale 参数（TIER_PARAMS["BIG"]）:
  funding rate:
    fr_extreme_high  0.00005   # +3 做空（BTC 7 天 p95 ≈ 0.003%）
    fr_high          0.00003   # +2
    fr_mild_high     0.00001   # +1
    fr_extreme_low  -0.00005   # +3 做多（镜像）
    fr_low          -0.00003   # +2
    fr_mild_low     -0.00001   # +1

  LSR (long_account):
    ls_long_extreme  0.75  # +2 做空
    ls_long_high     0.70  # +1
    ls_short_extreme 0.55  # +2 做多
    ls_short_high    0.50  # +1

  OI 4h change:
    oi_drop_strong  -0.025  # +2 做空
    oi_drop_mild    -0.010  # +1
    oi_rise_strong   0.025  # +2 做多
    oi_rise_mild     0.010  # +1

  放量滞涨/滞跌 (bars[-3:] 对比前 24 根均量):
    vol_ratio_strong 2.0  # +3（diverged 时）
    vol_ratio_mild   1.5  # +2
    stale_price_pct  0.010  # 3h 价格变化 < 1% 算滞涨/滞跌

  Taker:
    taker_sell_thresh 0.45  # +1 做空
    taker_buy_thresh  0.55  # +1 做多

  触发器（必须满足）:
    trigger_candle_pct 0.005  # 1h 实体 ≥ 0.5%（2026-04-24 放宽自 0.8%）
    trigger_breakout   0.0015 # 跌破/突破 4h 高低点 0.15%

  入场门槛 entry_score_min = 3（2026-04-24 放宽自 4）
```

**2026-04-24 放宽记录**：首日上线后 48h 只触发 1 笔（主流币日内 1h 实体极少超 0.8%）。
放宽到 0.5%（覆盖 BTC/SOL/BNB/DOGE/ADA/SUI 的常见日内动能）+ 门槛降到 3 分，
预期每日 3-5 笔 BIG 信号。代价是信号质量下降，需观察胜率变化。

### 4a.5 big_whale_tick 信号流

```
big_whale_tick(sym):
  state = get_or_create('bigmid', sym, 'whale')
  IF state != IDLE or cooldown not elapsed: return
  bars_1h = last 24 completed 1h bars
  s_short, trig_short = compute_whale_score(short, bars_1h, cur_price)
  s_long,  trig_long  = compute_whale_score(long,  bars_1h, cur_price)
  candidates = []
  IF s_short >= 4 AND trig_short: candidates.append(('SHORT', s_short))
  IF s_long  >= 4 AND trig_long:  candidates.append(('LONG',  s_long))
  IF not candidates: return
  direction, sc = max(candidates, key=score)  # 同分偏 short
  open_order(sym, direction, price, 'BIG', 'whale-entry', limit_p=None)  # 市价
  update_state(state='PENDING')
```

### 4a.6 MID 档 CHASE / DUMP 信号流

```
CHASE: bars_15m[-24:]; pump >= 6% + leader >= 1.5% + dd <= 3%
DUMP:  bars_15m[-48:]; drop >= 5% + bounce <= 4%
挂限价单 cur * (1 ± 1.5%)，2h 超时撤单，反向滑点 0.75% 熔断
```

### 4a.5 限价单填充（复用反向滑点熔断，但按 tier 取阈值）

```python
for o in pending_orders WHERE source LIKE 'strategy_bigmid:%':
    tier = lookup_tier(o.symbol)     # 挂单期间 tier 若被挤出池 → CANCELLED('tier_downgrade')
    if triggered:
        rev_slip = (limit_p - cur_p)/limit_p if LONG else (cur_p - limit_p)/limit_p
        if rev_slip > TIER_PARAMS[tier]['reverse_slippage']:
            CANCELLED(reason=f'reverse_slippage_{rev_slip:.4f}')
            continue
        # optimistic lock FILLING + SL/TP 按 cur_p 重算
        POST /api/futures/open
```

### 4a.6 持仓监控

`_monitor_positions`: 只扫 `source LIKE 'strategy_bigmid:%'` 的 open 持仓：
1. 硬止盈 pnl >= hard_tp_pct → close('hard-tp')
2. 止损 pnl <= -sl_pct → close('stop_loss')
3. 移动止盈 peak >= trail_tp_start 且 (peak-pnl) >= trail_tp_pullback → close('trail-tp')
4. 超时 timeout_at <= NOW()：由 FuturesTradingEngine 的后台任务处理（未显式在 bigmid 进程里实现）

`_settle_closed_positions`: 把 `futures_positions.status='closed'` 且状态机仍在 LONG/SHORT/PENDING 的条目复位为 DONE，启动 4h 冷却。

### 4a.7 隔离策略

| 维度 | 实现 |
|------|------|
| 状态机 | `strategy_state` 表 strategy='bigmid' |
| 订单过滤 | `futures_orders.order_source LIKE 'strategy_bigmid:%%'` |
| 仓位过滤 | `futures_positions.source LIKE 'strategy_bigmid:%%'` |
| 账户共享 | ACCOUNT_ID=2 与 strategy_live/whale 共用；`_has_any_open` 跨策略，同标的不重复开 |
| 日志 | `strategy_bigmid.log` 独立文件 |

### 4a.8 72h 回测记录

| 方案 | 成交 | 胜率 | 累计 PnL |
|------|------|------|----------|
| BIG CHASE/DUMP SL=2.5%/TP=5%/24h | 3 | 33% | **-57 U** |
| BIG CHASE/DUMP SL=1%/TP=2.5%/8h | 8 | 12% | **-121 U** |
| **BIG Whale SL=1%/TP=2%/4h** | **8** | **75%** | **+70 U** |
| MID CHASE/DUMP | 4 | 50% | +10 U |

根因：主流币 1h 涨幅 3% 不是趋势信号，回踩 1% 是常态。单一 CHASE/DUMP 指标在大币无效，必须多维共振（funding + OI + 放量 + 触发器）才能过滤噪声。

### 4a.9 v2 路线图（未实现）

- MID 档加 Climax / TopShort / BottomLong 覆盖
- BIG Whale 按币自适应 LSR 阈值（BTC p50≈0.45 vs DOGE p50≈0.73，统一阈值漏信号）
- 实盘灰度：paper 验证 7 天后开小额实盘，对比 paper/live PnL 相关性

---

## 5. 限价单通用设计

### 5.1 挂单偏移对比

| 引擎 | 方向 | 偏移 | 计算 |
|------|------|------|------|
| strategy_live | LONG | -3% | cur * 0.97，不低于 low_24h |
| strategy_live | SHORT | +3% | cur * 1.03，不高于 high_24h |
| strategy_whale | SHORT | +0.3% | cur * 1.003，不高于 high_24h |
| strategy_whale | LONG | -0.3% | cur * 0.997，不低于 low_24h |

### 5.2 超时取消

| 引擎 | 超时阈值 | 原因 |
|------|----------|------|
| strategy_live | 1h (3600s) | 慢速爬升信号衰减快 |
| strategy_whale | 2h (7200s) | 庄家行为持续时间较长 |

---

## 6. API 调用规范

### 6.1 开仓

```
POST /api/futures/open
{
  "account_id": 2,
  "symbol": "BTC/USDT",
  "position_side": "LONG",  # 或 "SHORT"
  "quantity": qty,           # MARGIN * LEVERAGE / price_ref
  "leverage": 5,
  "stop_loss_price": price_ref * (1 - sl_pct),
  "take_profit_price": price_ref * (1 + tp_pct),
  "max_hold_minutes": 360,
  "source": "strategy_live:chase-entry",
  "limit_price": lp           # 可选，有则挂限价单
}
```

### 6.2 平仓

```
POST /api/futures/close/{position_id}
{
  "reason": "stop_loss" | "hard-tp" | "trail-tp" | "timeout",
  "close_price": cur_price    # 可选，有则用此价格
}
```

---

## 7. 可配置参数与默认值

| 参数 | 默认值 | 范围 | 说明 |
|------|--------|------|------|
| CHASE_PUMP_BARS | 24 | 固定 | 追涨回看窗口（5m K 线数） |
| CHASE_PUMP_PCT | 0.12 | 固定 | 追涨触发涨幅 |
| CHASE_EXHAUST_MAX_DD | 0.06 | 固定 | 耗竭判断：高点回撤阈值 |
| CHASE_LEADER_BAR_MIN_PCT | 0.03 | 固定 | 急拉验证：单 bar 涨幅门槛 |
| LIVE_SL_PCT | 0.10 | 可配 | 止损（覆盖所有子策略） |
| LIVE_HARD_TP_PCT | 0.20 | 可配 | 硬止盈 |
| LIVE_LIMIT_OFFSET_PCT | 0.03 | 可配 | 限价偏移 |
| LIVE_HOLD_H | 6 | 可配 | 最大持仓时长(h) |
| TRAIL_TP_START | 0.12 | 固定 | 移动止盈激活阈值 |
| TRAIL_TP_PULLBACK | 0.02 | 固定 | 移动止盈回落触发 |
| SYMBOL_MAX_DAILY_SL | 2 | 固定 | 日内止损熔断次数 |
| POST_CLOSE_COOLDOWN_S | 14400 | 固定 | 平仓后冷却(4h) |
| REVERSE_SLIPPAGE_LIMIT | 0.015 | 固定 | 反向滑点熔断阈值（_fill_pending_orders）|
| LIMIT_PENDING_MAX_S | 3600 | 固定 | LIMIT 超时撤单阈值(s) |
| TOPCLI_VOL_MULT | 2.0 | 固定 | Climax 放量倍数 |
| TOPCLI_POST_LEADER_WAIT_BARS | 2 | 固定 | 领袖 K 后等待根数 |
| ENTRY_SCORE_MIN | 5 | 固定 | Whale 评分入场门槛 |
| WHALE_SL_PCT | 0.10 | 可配 | Whale 止损 |
| WHALE_HARD_TP_PCT | 0.20 | 可配 | Whale 硬止盈 |
| WHALE_LIMIT_OFFSET_PCT | 0.003 | 可配 | Whale 限价偏移 |
| WHALE_HOLD_H | 6 | 可配 | Whale 最大持仓时长(h) |
| COOLDOWN_S | 21600 | 固定 | Whale 普通冷却(6h) |
| COOLDOWN_SL_S | 43200 | 固定 | Whale 止损后冷却(12h) |
| MAX_POS_PER_SIDE | 3 | 固定 | Whale 单侧最大持仓数 |
