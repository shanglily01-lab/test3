# 策略优化指导文件

> 适用范围：d:\test3\crypto-analyzer（测试环境）
> 生产环境 d:\test2 **不允许任何修改**
> 最后更新：2026-04-17

---

## 一、系统架构总览

### 1.1 策略分层

```
dimension_trader.py
├── D系列（3个）         全量标的 SHORT  仅强空 score <= -0.85
│   ├── D1a-Big4StrengthShort
│   ├── D4b-Big4Only
│   └── D3-AltLag
│
├── E系列（98个）        全量/精选标的
│   ├── E1-E15           SHORT  仅强空 score <= -0.85
│   └── E16-E98          LONG   中性以上 score >= 0.0
│       ├── DB家族        DecelBounce（4h上行+1h回调反转）
│       ├── FluxAccel家族  flux加速（趋势延续）
│       ├── OvrSold家族   超卖反弹
│       └── BTCLead家族   BTC领涨跟随
│
└── A系列（174个总）     全量 ALT99 标的
    ├── Batch1（9个）    反转LONG  轻空以上 score >= -0.5
    │   ├── A1-SellCapLong
    │   └── A9-SpatDivLong
    ├── Batch2（39个）   SHORT+LONG  多个门槛
    │   ├── PM/FM SHORT  轻空以上 score <= -0.5
    │   └── PM/FM LONG   中性以上 score >= 0.0
    ├── Batch3（14个）   反转LONG  轻空以上 score >= -0.5
    │   ├── OFD_L系列
    │   └── VolMom_L系列
    ├── Batch4（4个）    反转LONG  轻空以上 score >= -0.5
    │   └── CC_L系列（CloseConsistency）
    └── Batch5（7个）    反转LONG  轻空以上 score >= -0.5
        └── PVel_L系列（PriceVelocityExhaustion）
```

### 1.2 Big4 Regime Filter（市场环境过滤）

基于 `big4_trend_history` 表近6H信号分布，每30分钟刷新。
**2026-04-17 简化为二值门槛**：只区分"是否强多/强空"，其余一律允许双向：

| Regime | score | allow_long | allow_short | 适用场景 |
|--------|-------|-----------|------------|---------|
| 强多   | +1.0  | True      | **False**  | bull >= 85%（屏蔽空） |
| 轻多   | +0.5  | True      | True       | bull 60-85% |
| 中性   | 0.0   | True      | True       | 方向不明 |
| 轻空   | -0.5  | True      | True       | bear 60-85% |
| 强空   | -1.0  | **False** | True       | bear >= 85%（屏蔽多） |

阈值公式（`dimension_trader.py _load_settings_from_db`）：

```python
_ALLOW_LONG   = score >= -0.5   # 非强空都允许做多
_ALLOW_SHORT  = score <=  0.5   # 非强多都允许做空
```

**策略入场门槛（已简化，不再分级）：**

| 策略方向 | 门槛 | 说明 |
|----------|------|------|
| 所有 LONG 策略  | `_ALLOW_LONG == True`  | 大盘不是强空就开 |
| 所有 SHORT 策略 | `_ALLOW_SHORT == True` | 大盘不是强多就开 |

> 早期（2026-04-16 之前）用的分级阈值（趋势SHORT `-0.85`，Alien SHORT `-0.5`，
> 反转LONG `-0.5`，一般LONG `0.0`）已**全部废弃**，由 big4 二值直接决定。

### 1.3 当前实盘表现（7日，2026-04-16）

| 策略家族 | 笔数 | 7日盈亏 | 胜率 | 状态 |
|----------|------|---------|------|------|
| Batch4 CC_L | 50 | +1743U | 66% | 优秀 |
| E16-E98 LONG | 21 | +789U | 43%* | 良好（EV高） |
| Batch1 A1/A9 | 34 | +147U | 53% | 一般（A9待观察） |
| Batch3 VolMom | 14 | +36U | 57% | 尚可 |
| Batch3 OFD | 1 | +15U | 100% | 样本太少 |
| E1-E15 SHORT | 200 | -3738U | 28% | 已被Regime屏蔽 |
| D系列 SHORT | 10 | -294U | 20% | 已被Regime屏蔽 |

*E16-E98 胜率低但单笔盈利高（DB家族大涨），期望值仍为正。

---

## 二、策略评估标准（准入门槛）

### 2.1 回测验证阶段（auto_explore_alien3.py）

| 指标 | 最低标准 | 优秀标准 |
|------|---------|---------|
| 测试集通过率 | 14/24 标的 | 18/24 标的 |
| 测试集胜率 | >= 57% | >= 62% |
| 训练/测试胜率差 | <= 4% | <= 2% |
| 测试集样本 | >= 50笔 | >= 100笔 |
| EV（期望值） | > 0 | > +0.3% |

### 2.2 参数优化阶段（signal_analysis.py）

| 指标 | 最低标准 | 优秀标准 |
|------|---------|---------|
| 历史信号数 | >= 80笔 | >= 150笔 |
| 最优hold时历史胜率 | >= 58% | >= 63% |
| SL净效益 | > 35% | > 44% |
| 最优SL范围 | 0.8% ~ 2.0% | 1.0% ~ 1.5% |

### 2.3 实盘监控阶段（上线后）

| 时间窗口 | 危险信号 | 处理动作 |
|---------|---------|---------|
| 前3天 | 亏损 > -100U 或胜率 < 40% | 暂停观察，检查信号逻辑 |
| 7天 | 亏损 > -200U 或胜率 < 45% | 考虑停用或调整参数 |
| 30天 | 连续亏损 > -500U | 停用并分析根因 |
| 任意时段 | 单策略单日亏损 > -150U | 立即检查，可能遇到极端行情 |

---

## 三、优化工作流程（标准四阶段）

```
Phase 1: 信号探索          Phase 2: 参数优化
auto_explore_alien3.py  →  signal_analysis.py
（新信号原语验证）           （SL/TP/hold最优化）
        ↓                         ↓
通过率 >= 14/24           参数写入 strategy_params DB
        ↓                         ↓
Phase 3: 代码部署          Phase 4: 实盘监控
dimension_trader.py     →  futures_positions 表
（加入 BATCH 列表）         （7日/30日P&L追踪）
        ↓
重启 dimension_trader
```

---

## 四、Phase 1：信号探索规范

### 4.1 auto_explore_alien3.py 使用

```bash
cd d:/test3/crypto-analyzer

# 标准探索：3阶段验证（train/test/stability）
.venv/Scripts/python.exe auto_explore_alien3.py --signal NewSignalName

# 查看当前探索状态
tail -f logs/alien3_20260416_xxxx.log
```

**探索前必做：**
1. `Grep` 检查同名或相似信号是否已存在
2. 确认原语函数在 `dimension_trader.py` 中已实现
3. 检查信号是否与现有信号重叠度过高（避免同质化）

### 4.2 信号原语设计原则

| 原则 | 说明 | 反例 |
|------|------|------|
| 单一职责 | 每个原语只测量一种市场特征 | 混合价格+成交量+时间的复合原语 |
| 可参数化 | 关键门槛不硬编码，支持调参 | 直接写死 `if close < 0.43` |
| 有方向性 | 原语输出应与信号方向有明确逻辑 | 买压高 -> LONG 信号 |
| 避免前视偏差 | 只用当前K线及历史数据 | 使用未来K线数据 |
| 小样本稳健 | 至少 n >= 3 根K线才计算 | n=1 的极端值 |

**现有原语（可复用，不要重复造轮子）：**

```python
# 基础指标（dimension_trader.py 600-680行区域）
gradient(cs, n)           # 近n根价格梯度，正=上行，负=下行
amplitude(cs, n)          # 近n根平均振幅/收盘价（波动率代理）
flux(cs, n)               # 近n根买压比例（buy_vol/vol），>0.5=买方主导
sl_tp_from(cs)            # 根据近期振幅自动推算 SL/TP

# Alien 原语（680-750行区域）
_sell_saturation(cs, n)   # 近n根平均卖压比例（sell_vol/vol）
_spatial_close(cs, n)     # 近n根平均收盘位置（(close-low)/(high-low)）
_momentum_ratio(cs, n)    # 成交量加速度（近N根 vs 前N根）
_price_memory(cs, n)      # 价格在近期区间的位置（0=底部, 1=顶部）
_close_consistency(cs, n, lo) # 近n根收盘偏低比例（lo门槛）
_price_velocity(cs, n, amp_n, pv_th) # 价格下跌速度归一化
```

### 4.3 通过率不足时的处理

| 通过率 | 建议处理 |
|--------|---------|
| 18-24/24 | 直接进 Phase 2 参数优化 |
| 14-17/24 | 进 Phase 2，但标注为"观察级"，实盘样本达30笔再评估 |
| 10-13/24 | 调整参数重跑，或寻找更好的标的子集 |
| < 10/24 | 放弃该信号，记录失败原因 |

---

## 五、Phase 2：参数优化规范

### 5.1 signal_analysis.py 使用

```bash
cd d:/test3/crypto-analyzer

# 运行指定批次（推荐：每次最多跑2批，避免DB锁）
.venv/Scripts/python.exe signal_analysis.py --batch 34

# 只看结果不更新参数
.venv/Scripts/python.exe signal_analysis.py --batch 34 --no-update

# 单独跑某个策略
.venv/Scripts/python.exe signal_analysis.py --strategies CC_L1 CC_L2

# 历史数据窗口（默认 2024-07-01 ~ 2025-03-31，约9个月）
.venv/Scripts/python.exe signal_analysis.py --batch 34 --start 2024-01-01 --end 2025-06-01
```

### 5.2 BATCHES 编号规则

```python
# signal_analysis.py 中的 BATCHES 字典
BATCHES = {
    1-8:   早期批次（E系列基础验证）
    9-20:  SHORT 优化（E1-E15）
    21-33: LONG 优化（E16-E98 + VolMom）
    34:    CC_L（CloseConsistency LONG，Batch4）
    35:    PVel_L（PriceVelocity LONG，Batch5）
    36+:   新策略预留
}
```

新策略加入规则：
1. 在 `ALL_STRATEGIES` 字典中注册（含 fn/dir/mode/name/test_wr）
2. 在 `BATCHES` 末尾追加新批次编号
3. 批次内策略数：3-8个（不超过10个，避免运行时间过长）

### 5.3 参数选择原则

优先选 **SL净效益最高** 的参数，而非纯胜率：

```
SL净效益 = 赢单被截% 对 输单被截% 的净差
SL净效益 > 40% 为良好
SL净效益 > 45% 为优秀
```

SL/TP 上下限参考：

| 策略类型 | SL范围 | TP范围 | hold范围 |
|---------|--------|--------|---------|
| 趋势SHORT（E1-E15） | 0.8-1.5% | 1.5-3.0% | 6-12h |
| 反转LONG（CC/PVel/OFD） | 1.0-1.5% | 2.0-3.0% | 6-8h |
| 趋势LONG（E-DB/FluxAccel） | 1.0-2.0% | 2.0-6.0% | 8-24h |
| 超卖反弹（OvrSold） | 0.8-1.2% | 1.5-2.5% | 4-8h |

> **2026-04-17 更新**：应用户指令，**Phase 2 参数优化阶段全部跳过**。
> 所有新策略与 DB 已有策略都统一为 `SL=2%, TP=3%, hold=3h`（见第十三节）。
> `signal_analysis.py` 仅用于历史分析，不再用于生产部署参数。

---

## 六、Phase 3：代码部署规范

### 6.1 dimension_trader.py 部署流程

**步骤1：添加信号原语函数**（如需新原语）

```python
# 约 620-750 行附近，按类型分组
def _new_primitive(cs: list, n: int, threshold: float) -> float:
    """简短说明：输入 -> 输出含义，范围"""
    if len(cs) < n: return 0.5   # 数据不足时返回中性值
    # ... 计算逻辑
    return result
```

**步骤2：添加信号函数**

```python
def _sig_new_long(cs1h: list, cs4h: list, param1: int, param2: float) -> bool:
    """策略名称: 逻辑描述 test=XX%"""
    if len(cs1h) < param1 + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False   # 4h确认方向
    if _new_primitive(cs1h, param1) >= param2: return False
    return True
```

**步骤3：添加到策略列表（全局变量区，约 480-560 行）**

```python
# 反转LONG 列表格式：(param1, param2, "策略名")
_NEW_LONG_LIST = [
    (5, 0.20, "New_L_n5_p20"),
    (3, 0.25, "New_L_n3_p25"),
]
```

**步骤4：添加到 compute_signal() 调用（约 2183-2213 行）**

```python
# 在对应 Regime 块内
for _p1, _p2, _nm in _NEW_LONG_LIST:
    if _sig_new_long(cs1h, cs4h_self, _p1, _p2):
        sl_pct, tp_pct = sl_tp_from(cs1h)
        return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
```

**步骤5：添加默认参数（约 150-337 行）**

```python
_STRATEGY_PARAMS_DEFAULT = {
    # ...
    "New_L_n5_p20": {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "New_L_n3_p25": {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
}
```

**步骤6：在 signal_analysis.py 中注册**

```python
ALL_STRATEGIES["New1"] = {"fn": _partial(_sig_new_long, param1=5, param2=0.20),
                           "dir": "LONG", "mode": "self4h",
                           "name": "New_L_n5_p20", "test_wr": 59.0}
BATCHES[36] = ["New1", "New2", ...]
```

### 6.2 重启规范

```bash
cd d:/test3/crypto-analyzer

# 标准重启（推荐）
kill $(cat dimension_trader.pid 2>/dev/null) 2>/dev/null
rm -f dimension_trader.pid
nohup .venv/Scripts/python.exe -u dimension_trader.py >> logs/dimension_trader.log 2>&1 &
echo "PID=$!"

# 确认启动成功（看到 BIG4-REGIME 日志）
grep "BIG4-REGIME\|started" logs/dimension_trader.log | tail -3
```

**必须重启的场景：**
- 修改信号函数逻辑（sig_xxx）
- 修改策略列表（_XX_LIST）
- 修改全局参数（_REGIME_SCORE 分层门槛）
- 修改 compute_signal() 的 Regime 判断结构

**不需要重启的场景（DB自动刷新）：**
- 修改 SL/TP/hold_h 参数（通过 strategy_params 表，30分钟刷新）
- 修改 allow_long/allow_short DB 设置（1小时内生效）

### 6.3 watchdog 覆盖范围

| 进程 | watchdog管理 | 崩溃处理 |
|------|------------|---------|
| fast_collector_service.py | 是，自动重启 | 自动 |
| smart_trader_service.py | 是，自动重启 | 自动 |
| dimension_trader.py | **否** | 手动重启 |
| alien_trader.py | **否** | 手动重启 |
| app/main.py (API) | **否** | 手动重启 |

---

## 七、Phase 4：实盘监控规范

### 7.1 日常查询

```sql
-- 7日各策略P&L
SELECT source,
       COUNT(*) as n,
       ROUND(SUM(realized_pnl), 2) as total_pnl,
       ROUND(AVG(realized_pnl), 2) as avg_pnl,
       ROUND(100.0 * SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) / COUNT(*), 1) as win_rate
FROM futures_positions
WHERE account_id=2 AND status='closed'
  AND close_time >= NOW() - INTERVAL 7 DAY
GROUP BY source
HAVING n >= 3
ORDER BY total_pnl DESC;
```

### 7.2 策略停用决策树

```
7日亏损 > -200U 且 胜率 < 45%?
├── YES: 立即停用（从 dimension_trader.py 移除或加 Regime 限制）
│         记录停用原因到本文档"历史决策"章节
└── NO:
    ├── 7日笔数 > 20 且 avg_pnl < +5U?
    │   └── YES: 优化（收紧信号条件，减少无效开仓）
    └── 否: 继续观察（至少30笔实盘样本再评估）
```

### 7.3 策略停用方法

**方法A：提高 Regime 门槛**（推荐，保留信号逻辑）
```python
# 反转LONG 从 score >= -0.5 改为 score >= 0.0
# 在 compute_signal() 里的对应 if 块修改门槛
```

**方法B：从策略列表移除**（彻底停用）
```python
# 从 _CC_LONG_LIST 等列表中删除对应条目
# 从 _STRATEGY_PARAMS_DEFAULT 中删除对应键
```

**方法C：调整参数收紧信号**（减少触发）
```python
# 修改 sig_A9() 等函数中的数值门槛
# 修改后必须重启
```

---

## 八、Big4 Regime 维护规范

### 8.1 Regime 核心参数（dimension_trader.py 约 343-350 行）

```python
_ALLOW_LONG:   bool  = True    # 初始默认值
_ALLOW_SHORT:  bool  = False   # 初始默认值
_REGIME_SCORE: float = 0.0     # 五级分数

_BIG4_LOOKBACK_HOURS  = 6    # 近N小时窗口（太短=噪声，太长=滞后）
_BIG4_MIN_DIR_RECORDS = 5    # 有方向信号最少条数（不足则中性）
```

### 8.2 Regime 调整场景

| 场景 | 建议调整 |
|------|---------|
| 熊市中 CC_L/PVel_L 频繁止损 | 反转LONG 门槛从 >= -0.5 改为 >= 0.0 |
| 平静市场 SHORT 策略持续亏损 | 强空门槛从 <= -0.85 改为 <= -1.0（更严格） |
| 牛市中所有策略胜率飙升 | 可考虑降低一般LONG门槛至 >= -0.5 |
| 大波动市场（>10%单日波动）| 临时改 allow_long=0 allow_short=0（系统停止） |

### 8.3 紧急停止（通过DB，无需重启）

```sql
-- 停止所有新开仓
UPDATE system_settings SET setting_value='0' WHERE setting_key='allow_long';
UPDATE system_settings SET setting_value='0' WHERE setting_key='allow_short';

-- 30分钟内生效（等待 _load_settings_from_db 刷新）
-- 或重启 dimension_trader 立即生效
```

---

## 九、新信号原语开发指引

### 9.1 已验证有效的信号类型（可参考）

| 信号类型 | 代表策略 | 核心逻辑 | 7日表现 |
|---------|---------|---------|---------|
| 多时间框架背离 | E2, E16-E30 DB | 4H趋势 + 1H逆势 | +++ |
| Flux加速 | E79, E98 FluxAccel | 买压持续加速 | ++++ |
| 收盘一致性 | CC_L Batch4 | 多根收盘偏低 = 超卖 | ++++ |
| 卖方投降 | A1-SellCapLong | 卖压骤升后回落 | ++ |
| 空间背离 | A9-SpatDivLong | 4H上行但K线收低 | + |
| 价格速度 | PVel_L Batch5 | 下跌速度异常 = 超卖 | 待观察 |

### 9.2 已证明无效的信号类型（避免重复尝试）

| 信号类型 | 失败原因 |
|---------|---------|
| AmplitudeSkewSignal | 0/24通过，波动不对称无预测力 |
| EntropyVelocityBreak | 0/36通过，熵值变化噪声过大 |
| 纯技术指标（RSI/MACD等） | 同质化严重，在现有信号中无增量信息 |
| 基于新闻/情绪 | 数据质量不稳定，信号延迟 |

### 9.3 最有潜力的探索方向（待开发）

按优先级排序：

1. **OrderBook 不平衡** - 买卖盘深度比（需要orderbook数据接入）
2. **跨标的相关性** - 板块联动信号（ETH领涨 -> 其他DeFi）
3. **成交量时段分布** - 不同时段的量的形态（亚盘/欧盘/美盘）
4. **历史价格区间** - 支撑阻力位接近度
5. **Funding Rate 极端** - 资金费率超过阈值时的反转

---

## 十、禁忌清单

### 严格禁止

- **修改 d:\test2 任何文件**（生产环境，勿碰）
- **启用 live_trading_enabled=1**（DB 设置，测试用假钱）
- **删除 futures_positions 中 realized_pnl > 0 的历史记录**（历史数据不可篡改）
- **跳过 Phase 1 直接部署**（未经回测的信号不允许上实盘）
- **单批次部署超过 10 个新策略**（无法判断是哪个导致问题）
- **修改 auto_explore_alien3.py 的通过门槛**（勿下调 14/24 的验证标准）

### 注意事项

- 修改完代码必须重启对应进程（参见 6.2 节）
- 禁止同时运行多个 dimension_trader 实例（会重复开仓）
- signal_analysis.py 同时运行不超过 2 批（避免 DB 连接过多）
- 策略总数超过 200 个时，考虑按信号质量清理尾部策略

---

## 十一、工具快速索引

| 工具 | 路径 | 用途 |
|------|------|------|
| 主交易程序 | `dimension_trader.py` | 信号计算+开仓执行 |
| 参数优化 | `signal_analysis.py` | 历史回测最优SL/TP/hold |
| 信号探索 | `auto_explore_alien3.py` | 新信号原语验证（24标的3阶段） |
| 策略发现 | `discovery_trader.py` | 纯探索模式（不开仓）|
| K线采集 | `fast_collector_service.py` | 1m/5m/1h/4h/1d数据入库 |
| API服务 | `app/main.py` | REST API port:9021 |
| 参数表 | DB: `strategy_params` | SL/TP/hold，30min热加载 |
| 环境设置 | DB: `system_settings` | allow_long/allow_short 等 |
| 持仓记录 | DB: `futures_positions` | 全部历史持仓（account_id=2） |
| Regime数据 | DB: `big4_trend_history` | Big4每小时趋势信号 |

---

## 十二、历史优化决策记录

| 日期 | 决策 | 原因 | 效果 |
|------|------|------|------|
| 2026-04-16 | 引入 Big4 Regime 5级评分 | E1-E15 SHORT 在牛市 7日亏-3738U | 牛市自动屏蔽 SHORT |
| 2026-04-16 | A9-SpatDivLong 收紧门槛 | 30笔/7天但avg仅+6.59U | gradient >0.002, spatial <0.38 |
| 2026-04-16 | E2-LONG 归入 score>=0.0 块 | 确认E2是趋势跟随型，不是反转型 | 正确分层 |
| 2026-04-16 | Batch4 CC_L 参数优化 | 上线无最优参数 | 4策略最优SL=1.2%, hold=8h |
| 2026-04-16 | Batch5 PVel_L 参数优化 | 上线无最优参数 | 7策略最优SL=1.5%, hold=8h |
| 2026-04-16 | XVG/USDT 从 ALT99 移除 | 币安已下架 | 避免API报错 |
| 2026-04-16 | smart_exit_optimizer.py 修复 | dimension_trader 持仓超8h | 加入跳过列表 |
| 2026-04-16 | 清理一级过时策略（8个） | 5日实盘数据确认信号失效（胜率11-30%/重复开单虚增/0%胜率即止损） | 删除 E7/E11/E13/E15/A6/sig_E47/VolMom_L_n3_l4_v40/E27-DB-h3 |
| **2026-04-17** | **Regime 二值化**（强多/强空屏蔽单边） | 分级阈值过严，轻多/中性时 SHORT 几乎不发车 | `_ALLOW_LONG = score>=-0.5`，`_ALLOW_SHORT = score<=0.5` |
| **2026-04-17** | **策略参数全部统一为 2%SL/3%TP/3h** | 用户指令：废弃 Phase 2 参数优化 | DB 252 条全部覆盖 + `_UNIFIED_DEFAULT` 硬编码保底 |
| **2026-04-17** | **alien 探索脚本增加去重** | 重复验证已部署策略浪费时间 | `explored_filter.py` + `--force` 开关，见第十三节 |

---

## 十三、已探索策略去重机制（2026-04-17）

### 13.1 背景

`auto_explore_alien*.py` 每次运行会生成数百个候选策略名，若与之前跑过的重名，
进入四阶段验证会**重复消耗 5-30 分钟/主题**而无新结果。

### 13.2 机制

**权威记录**：`strategy_params` 表的 `strategy_name` 列。凡是表里有的名字，视为
"已通过验证并部署"，默认不再重跑。

**实现**：共享模块 `explored_filter.py`：

```python
from explored_filter import load_deployed_names, filter_new_strategies

deployed = load_deployed_names()       # 一次性从 DB 拉取全部 strategy_name
strategies = filter_new_strategies(theme_fn(), deployed)  # 过滤后再验证
```

5 个 `auto_explore_alien*.py` 脚本在 `run_exploration` 主循环里全部接入。

### 13.3 使用方法

```bash
cd d:/test3/crypto-analyzer

# 正常跑：自动跳过 DB 里已有的策略名
.venv/Scripts/python.exe auto_explore_alien5.py

# 只跑某个主题 + 去重
.venv/Scripts/python.exe auto_explore_alien5.py --theme TakerSuspend

# 强制重跑（信号函数改过逻辑需重新验证时）
.venv/Scripts/python.exe auto_explore_alien5.py --force
```

**日志标识：**

```
主题: TakerSuspend (12 待跑 / 18 总候选，跳过 6 个已部署)
[OrderFlow] 全部已在 strategy_params，跳过。--force 可强制重跑。
```

### 13.4 DB 当前已部署快照

| source | 数量 | 代表策略 |
|--------|------|---------|
| signal_analysis      | 172 | 早期 E/A 系列参数优化 |
| auto_explore_alien2  |  39 | MomDecay/PriceMem/CC |
| auto_explore_alien3  |  27 | CC_L/PVel_L |
| auto_explore_alien   |   9 | SellCap_L/BuyExh_S |
| auto_explore_alien5  |   5 | TakerSus_L 系列 |
| **合计**             | **252** | 全部参数 = 2%SL/3%TP/3h |

### 13.5 注意事项

- `auto_explore_alien4.py` 历史跑过但 DB 中 0 条 — 因为候选名与 alien2/alien3 重
  合，`_strategy_params_exist` 在 deploy 时就全跳过了。下次再跑 `alien4` 本机制
  会在**验证前**就跳过，彻底节省时间。
- **新加信号函数逻辑**（如修改 `_taker_suspension` 阈值）后，务必 `--force` 一次
  让回测结果刷新。
- **新加策略名**（不重名）不受影响，依旧正常走完四阶段。
