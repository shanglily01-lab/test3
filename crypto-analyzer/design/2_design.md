# 设计文档 — 量化交易系统

版本：v1.0  
更新日期：2026-04-23  
覆盖范围：strategy_live（趋势跟踪引擎）、strategy_whale（庄家对抗引擎）

---

## 1. 系统架构

```
┌──────────────────────────────────────────────────────────┐
│                    FastAPI 服务 (port 9021)               │
│  /api/futures/open  /api/futures/close  /api/system/*    │
│  FuturesTradingEngine (纸仓) │ BinanceFuturesEngine (实盘) │
└────────────┬─────────────────────────┬────────────────────┘
             │ HTTP                    │ HTTP
    ┌────────┴────────┐      ┌─────────┴──────────┐
    │ strategy_live   │      │ strategy_whale      │
    │ ACCOUNT_ID=2    │      │ ACCOUNT_ID=2        │
    │ POLL=60s        │      │ POLL=90s            │
    └────────┬────────┘      └─────────┬──────────┘
             │                         │
    ┌────────┴─────────────────────────┴──────────┐
    │              MySQL (dimesion)                │
    │  kline_data / futures_positions              │
    │  futures_orders / strategy_state            │
    │  system_settings / funding_rates / ...       │
    └──────────────────────────────────────────────┘
```

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

### 3.6 移动止盈（三个策略共用）

```python
def _check_trail_tp(pid, pnl_pct, peak_pnl_pct):
    new_peak = max(pnl_pct, peak_pnl_pct)
    update_state(peak_pnl_pct=new_peak)

    if pnl_pct >= HARD_TP_PCT(0.20):
        close_order(pid, "hard-tp")    # 硬止盈 20%

    if new_peak >= TRAIL_TP_START(0.12):
        if (new_peak - pnl_pct) >= TRAIL_TP_PULLBACK(0.02):
            close_order(pid, "trail-tp")  # 移动止盈：峰值回落2%
```

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

### 4.5 移动止盈（与 strategy_live 相同）

```python
def _monitor_pnl(pid, pnl_pct, peak):
    new_peak = max(pnl_pct, peak)
    if pnl_pct >= HARD_TP_PCT(0.20):
        _close_pos(pid, "hard-tp")
    elif new_peak >= TRAIL_TP_START(0.12) and (new_peak - pnl_pct) >= TRAIL_TP_PULLBACK(0.02):
        _close_pos(pid, "trail-tp")
```

### 4.6 冷却机制

| 平仓原因 | 冷却时长 | 对应常量 |
|----------|----------|----------|
| stop_loss | 12 小时 | COOLDOWN_SL_S = 43200 |
| 其他（tp/timeout） | 6 小时 | COOLDOWN_S = 21600 |

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
