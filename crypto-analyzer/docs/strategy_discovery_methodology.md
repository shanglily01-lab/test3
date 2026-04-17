# 策略发现方法论

> 从信号假设到实盘部署的完整流程  
> 更新时间：2026-04-16

---

## 一、总体流程图

```
[路径 A] Gemini AI 生成假设   [路径 B] 参数网格枚举    [路径 C] 批次化原语探索
strategy_explorer.py           auto_explore.py           auto_explore_alienN.py
         |                            |                          |
         v                            v                          v
   [S1] Big4 训练集快速筛           同左                    同左（86alts）
   [S2] 10随机山寨 训练集           同左                    同左
   [S3] 全量山寨 训练集             同左                    同左
   [S4] 全量30%走时测试集           同左                    同左（唯一判定性阶段）
         |                            |                          |
         +----------------+-----------+-------------+-----------+
                          |                         |
                   通过 Stage 4                去冗余筛选 (Phase 2)
                          |                         |
                          v                         v
              [Step 5] 参数优化（signal_analysis.py）
                  - 最优持仓时长（hold_h）
                  - 最优止损（SL）
                  - 最优止盈（TP）
                  - 写入 strategy_params DB 表
                          |
                          v
              [Step 6] 信号函数写入 dimension_trader.py
                  - 手写（路径A）或 deploy_strategies.py（路径B）
                  - dispatch 模式（路径C，helper函数+LIST）
                          |
                          v
              [Step 7] 重启 dimension_trader.py
```

---

## 二、三条发现路径

### 路径 A：Gemini AI 探索（strategy_explorer.py）

**适合发现：** 全新逻辑，人工难以穷举的非线性关系

Gemini 根据系统提示（已知策略上下文 + 灵感方向）生成信号代码，每轮 5 个假设：

```bash
# 单轮，生成 5 个策略（默认 SHORT+LONG）
.venv/Scripts/python.exe strategy_explorer.py

# 多轮，一次跑 15 个假设
.venv/Scripts/python.exe strategy_explorer.py --rounds 3

# 只生成 LONG 策略
.venv/Scripts/python.exe strategy_explorer.py --long-only

# 不调用 Gemini，用内置样本跑流程（调试用）
.venv/Scripts/python.exe strategy_explorer.py --no-gemini
```

结果保存到 `explorer_results/explorer_YYYYMMDD_HHMM.json`。  
通过 Stage 4 的策略需手动将信号函数添加到 `dimension_trader.py`。

**Gemini 提示工程要点：**
- 历史测试过的策略（近30条）注入到提示，避免重复
- "近乎通过"策略（训练>=60% 但测试54-59%）提供代码让 Gemini 生成变体
- 温度设为 0.95，鼓励探索
- 三种允许的信号函数模式：`1h`（纯1h）、`mtf_self`（自身4h）、`mtf_btc`（BTC 4h引导）

---

### 路径 B：参数网格枚举（auto_explore.py）

**适合发现：** 已有策略模板的参数空间，系统化穷举最优组合

当前支持的主题族群：

| 主题 | 模板 | 枚举参数 |
|------|------|---------|
| DecelBounce_Extended | DB 家族 | h_n, hist_th, f_min, amp1_max |
| OversoldDeepBounce | OvrSold 家族 | h_n, depth_th, f_min |
| FluxAcceleration | FluxAccel 家族 | hist_n, mac_n, f_abs |
| BTCLead | BTCLead 家族 | btc_th, alt_hist_n, f_min |

```bash
# 运行所有主题
.venv/Scripts/python.exe auto_explore.py

# 只跑单个主题
.venv/Scripts/python.exe auto_explore.py --theme DecelBounce_Extended

# 列出所有主题和候选数量
.venv/Scripts/python.exe auto_explore.py --list-themes
```

通过策略输出到 `logs/explore_YYYYMMDD_HHMM_passed.csv`，  
然后用 `deploy_strategies.py` 自动写入 `dimension_trader.py`：

```bash
.venv/Scripts/python.exe deploy_strategies.py               # 读最新 CSV
.venv/Scripts/python.exe deploy_strategies.py --dry-run     # 预览，不修改文件
```

---

### 路径 C：批次化原语探索（auto_explore_alienN.py）

**适合发现：** 基于物理/统计原语的新信号维度，系统化探索"非人类思维"的量化信号。

**核心思想：** 不由人写规则，而是定义物理原语（买压速率、动量记忆、流量极化等），让 AI 在参数空间内系统搜索统计显著的信号。

每批次包含 8 个原语，每个原语探索 8-24 个参数变体（LONG/SHORT 各计）。

```bash
# 启动探索（以 alien3 为例）
nohup .venv/Scripts/python -u auto_explore_alien3.py \
  >> logs/alien3_explore_run.log 2>&1 &

# 监控进度
tail -f logs/alien3_explore_run.log | grep -E "--- S|PASS|FAIL|淘汰|通过"
```

S4 是唯一判定性阶段（86alts 测试集，WR ≥ 57% 通过）。S1-S3 只做噪音过滤。

通过策略集成到 `dimension_trader.py` 使用 dispatch 模式：

```python
# helper 函数 + dispatch 列表
def _sig_pm_short(cs1h, cs4h, mem_n, mem_hi): ...
_PM_SHORT_LIST = [(20, 0.75, "A10-PriceMem-S-n20-hi75"), ...]
for _mn, _mh, _nm in _PM_SHORT_LIST:
    if _sig_pm_short(cs1h, cs4h_self, _mn, _mh):
        return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
```

**完整流程见：** `docs/alien_exploration_sop.md`（6 阶段 SOP：原语设计 → 漏斗探索 → 去冗余 → 参数优化 → 集成 → 实盘验证）

| 批次 | 脚本 | 状态 | 策略编号 |
|------|------|------|---------|
| Batch 1 | auto_explore_alien.py | 完成+集成 | A1–A9（9个）|
| Batch 2 | auto_explore_alien2.py | 完成+集成 | A10–A48（39个）|
| Batch 3 | auto_explore_alien3.py | 进行中 | A49+（OFD/VolMom 已集成，其余待完成）|

---

## 三、四阶段验证（Stage 1-4）

所有策略必须通过同一个漏斗，数据按时间序列切分（前 70% 训练 / 后 30% 测试）：

| 阶段 | 数据集 | 标的 | 通过门槛 |
|------|--------|------|---------|
| Stage 1 | 训练集 70% | Big4（BTC/ETH/BNB/SOL） | n≥5，WR≥57% |
| Stage 2 | 训练集 70% | 随机10个山寨 | n≥15，WR≥58%（LONG宽松55%） |
| Stage 3 | 训练集 70% | 全量99个山寨 | n≥30，WR≥60%（LONG宽松57%） |
| Stage 4 | 测试集 30% | 全量99个山寨 | WR≥60%（PASS），55-59%（border） |

**当前数据区间：** 2024-01-01 ~ 2026-04-15（835 天 1h K线，835天 4h K线）  
**走时切分点：** 约 2025-07 之前为训练集，之后为测试集

### 回测机制

每个信号触发后追踪接下来 `HOLD_BARS=3` 根 1h K线的价格走势：
- 期间触碰 SL → 以 SL% 亏损计算
- 期间触碰 TP → 以 TP% 盈利计算
- 未触碰 → 用第 3 根 K线的收盘价计算实际盈亏

SL/TP 动态按最近 6 根 1h 振幅计算（SL_MULT=1.5×振幅，TP_MULT=2.5×振幅），  
振幅下限 0.5%，上限 2.0%。

---

## 四、参数优化（signal_analysis.py）——必做步骤

**这是新策略部署前的必做步骤，不能跳过。**

验证阶段的 SL/TP 使用动态振幅估算，但实盘需要固定参数。  
`signal_analysis.py` 对每个策略扫描历史信号，分析最优：

- **hold_h**：哪个持仓时长 EV（期望值）最高
- **SL**：哪个止损能最大化"切输单 - 切赢单"的净效益
- **TP**：50th 百分位 MFE（最大有利偏移），确保一半信号能够触及

### 分析区间

```
DEFAULT_START = "2024-07-01"
DEFAULT_END   = "2025-03-31"
```

分析标的为25个代表性标的（ETH/BNB/SOL/XRP等），覆盖大中小市值。

### 运行方式

```bash
# 对单个策略运行（最常用，新策略发现后立即跑）
.venv/Scripts/python.exe signal_analysis.py --strategies E29

# 对多个策略同时跑
.venv/Scripts/python.exe signal_analysis.py --strategies E28 E29 E30

# 按批次（每批5个，约5-10分钟）
.venv/Scripts/python.exe signal_analysis.py --batch 1

# 只看报告不写回参数
.venv/Scripts/python.exe signal_analysis.py --strategies E29 --no-update
```

### 输出结果

分析完成后自动写入两处：

1. **DB `strategy_params` 表**（主路径，dimension_trader 每小时刷新）：
```sql
SELECT strategy_name, sl_pct, tp_pct, hold_h, signal_count, backtest_wr
FROM strategy_params ORDER BY strategy_name;
```

2. **dimension_trader.py 中的 `_STRATEGY_PARAMS_DEFAULT`**（兜底，DB 故障时使用）

dimension_trader 每小时自动 reload 一次 strategy_params，**不需要重启**。

### 如何判断参数优化结果好坏

阅读报告时关注：

```
[1] 各持仓时长分析
   1h  120  61.2%   +0.8%   -0.6%     +0.2%
   2h  120  63.5%   +0.9%   -0.7%     +0.3% <<  ← 最优
   3h  120  62.0%   +0.8%   -0.8%     +0.1%

[2] MFE 50th=1.85%  TP 命中率：
    TP=1.5%  命中  68.3%
    TP=2.0%  命中  51.2%  <<
    TP=2.5%  命中  38.7%

[3] SL 净效益（赢单=76 输单=44）：
   SL%  赢单被截%  输单被截%  净效益
  1.0%       5.2%      38.6%    +33.4%  <<
  1.2%       8.1%      42.0%    +33.9%
  1.5%      12.4%      47.7%    +35.3%
```

- 净效益 = 输单被截% - 赢单被截%，越高越好
- TP 选能被 50%+ 信号触及的最高值
- hold_h 选 EV 最高的时长

---

## 五、完整新策略部署 Checklist

以发现一个新 AI 策略（路径 A）为例：

```
[ ] 1. 运行 strategy_explorer.py --rounds N
[ ] 2. 查看 Stage 4 结果，记录 PASS 的策略名和代码
[ ] 3. 在 dimension_trader.py 中添加信号函数 sig_Exx()
[ ] 4. 在 dimension_trader.py 的 compute_signal() 中接入信号
[ ] 5. 在 signal_analysis.py 的 ALL_STRATEGIES 字典中注册
[ ] 6. 运行 signal_analysis.py --strategies Exx
[ ] 7. 确认 strategy_params 已写入 DB（见 [OK] 日志）
[ ] 8. 在 dimension_trader.py 的 _STRATEGY_PARAMS_DEFAULT 中添加兜底参数
[ ] 9. 重启 dimension_trader.py（立即生效）
[  ] 10. 观察实盘 24-48h，确认信号正常触发、SL/TP/hold 符合预期
```

路径 B（auto_explore）的 checklist 更简单：

```
[ ] 1. 运行 auto_explore.py
[ ] 2. 运行 deploy_strategies.py（自动写入信号函数 + 接入 compute_signal）
[ ] 3. 在 signal_analysis.py ALL_STRATEGIES 注册新策略
[ ] 4. 运行 signal_analysis.py --strategies Exx（逐批或单个）
[ ] 5. 重启 dimension_trader.py
[ ] 6. 观察 24-48h
```

路径 C（alien 原语探索）按 SOP 六阶段执行（详见 `docs/alien_exploration_sop.md`）：

```
[ ] Phase 0: 设计 8 个原语，写入 auto_explore_alienN.py
[ ] Phase 1: 跑漏斗 S1→S4，提取 S4 PASS 列表
[ ] Phase 2: 去冗余筛选（5条规则，最终 ≤40 策略）
[ ] Phase 3: signal_analysis.py 参数优化（SL/TP/hold_h 写入 DB）
[ ] Phase 4: dispatch 模式集成到 dimension_trader.py，重启验证
[ ] Phase 5: 48h 实盘验证（6项指标全合格）
```

---

## 六、阈值说明与调整建议

### 验证阶段阈值

```python
# strategy_explorer.py 和 auto_explore.py 中的门槛
STAGE1_MIN_WR = 0.57   # Big4 训练集：57%
STAGE2_MIN_WR = 0.58   # 10alts（LONG 模式宽松至 55%）
STAGE3_MIN_WR = 0.60   # 全量（LONG 模式宽松至 57%）
# Stage 4 测试集：PASS ≥ 60%，border = 55-59%
```

**什么时候调整：**
- 当前市场是熊市，LONG 策略天然信号少 → Stage 2/3 最小样本数可降低（不降 WR）
- 大量策略卡在 border → 检查是否测试集与训练集市场结构差异过大（换仓入选时段）

### 参数优化的 hold_h 分布规律

实盘观测总结：
- **DecelBounce 深回调（h≥12）**：最优 3-6h，不要撑太久
- **FluxAccel 买压加速**：最优 8h，买压加速趋势持续时间短
- **OvrSold 超卖反弹**：最优 3-8h，反弹快速完成
- **SHORT E1-E15**：最优 8-12h，趋势延续需要时间

---

## 七、信号发现的可扩展方向

### 当前三个基本指标

```python
gradient(cs, n)   # 价格动量方向（斜率）
flux(cs, n)       # 买方成交量占比（买压）
amplitude(cs, n)  # 振幅（波动烈度）
```

### 可派生的二阶指标（已在 Gemini prompt 中提示）

```python
# 动量加速度
gradient(cs, 3) vs gradient(cs, 6)    # 近期 vs 中期斜率比

# 买压趋势
flux(cs, 2) vs flux(cs, 6)            # 近期 vs 中期买压比

# 能量效率比（大振幅小方向 = 能量浪费）
abs(gradient(cs, n)) / amplitude(cs, n)

# K线实体占比（实体越小越虚弱）
(close - open) / (high - low)

# 上影线压力
(high - max(open, close)) / (high - low)

# 量价背离
vol 增加 but gradient 减小
```

### 建议的新探索方向（尚未充分挖掘）

1. **下影线密集 + flux 回升** → LONG（空头平仓触底）
2. **多周期 flux 共振**：1h flux(2) > flux(6) AND 4h flux(2) > flux(6) → 买压全线加速
3. **振幅突破**：近 6h 低振幅 + 突然 amplitude(1) > amplitude(6) × 2 → 方向性突破
4. **量能加速背离**：vol(1h) 在加速但 gradient 在减速 → 换手顶/底
5. **K线实体质量**：连续 N 根阳线但实体占比递减 → 上涨乏力（已有 E6 但可延伸）

---

## 八、快速参考

```bash
# 发现新策略（AI）
.venv/Scripts/python.exe strategy_explorer.py --rounds 2

# 发现新策略（网格）
.venv/Scripts/python.exe auto_explore.py

# 部署网格发现的策略
.venv/Scripts/python.exe deploy_strategies.py

# 原语探索（路径C，以 alien3 为例）
nohup .venv/Scripts/python -u auto_explore_alien3.py >> logs/alien3_explore_run.log 2>&1 &
tail -f logs/alien3_explore_run.log | grep -E "S4|PASS|FAIL"

# 参数优化（单个策略）
.venv/Scripts/python.exe signal_analysis.py --strategies E99

# 参数优化（全量批次）
for i in $(seq 1 20); do
  .venv/Scripts/python.exe signal_analysis.py --batch $i
done

# 查看 DB 中已有的参数
# SQL: SELECT * FROM strategy_params ORDER BY strategy_name;

# 重启 dimension_trader
kill $(pgrep -f dimension_trader.py)
cd d:/test3/crypto-analyzer
nohup .venv/Scripts/python.exe -u dimension_trader.py >> logs/dimension_trader.log 2>&1 &
```

---

*文档基于当前代码库（2026-04-15）编写，如修改了验证阈值或指标函数需同步更新本文档*
