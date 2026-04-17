# 策略探索方法论手册

## 一、核心工具箱（三个指标）

```
gradient(cs, n) = sum(close-open 最近n根) / 当前close
  正值=净上涨  负值=净下跌  越大越明确

flux(cs, n) = avg(买方成交量/总成交量 最近n根)
  >0.53 = 主动买盘偏多  <0.47 = 主动卖盘偏多

amplitude(cs, n) = avg(high-low 最近n根) / 当前close
  衡量波动大小，越小越稳定
```

---

## 二、四阶段验证门槛（必须全部通过）

| 阶段 | 标的集          | 最小n | 胜率门槛 | 数据段    | 作用                   |
|------|----------------|-------|---------|----------|----------------------|
| S1   | Big4           | ≥5    | ≥57%    | 训练集70% | 快速淘汰无效信号         |
| S2   | 随机10个山寨    | ≥15   | ≥55%    | 训练集70% | 验证不只在Big4有效       |
| S3   | 全部86山寨      | ≥30   | ≥57%    | 训练集70% | 大样本统计显著性         |
| S4   | 全部86山寨      | ≥10   | ≥57%    | 测试集30% | **核心：从未见过的数据** |

**S4测试WR才是真实表现的预估，其余都是筛选工具。**

---

## 三、策略结构模板（三层条件）

```python
def sig_新策略名(cs1h, cs4h):
    """
    市场假设: [为什么这个条件下价格会涨/跌？]
    信号逻辑: 宏观(4h) + 历史形态(1h N根) + 触发确认(1h 最近2根)
    """
    if len(cs1h) < 最小根数 or len(cs4h) < 8:
        return None

    # 层1：宏观环境（4h）
    if gradient(cs4h, 4) <= 0.003:      # LONG：宏观向上
        return None                      # SHORT：改为 >= -0.003

    # 层2：历史状态（1h 过去N根）
    if gradient(cs1h, N) >= -0.002:     # LONG：经历了N根回调
        return None                      # SHORT：改为 <= 0.002 局部反弹

    # 层3：触发确认（1h 最近2根）
    if gradient(cs1h, 2) <= 0:          # LONG：刚刚翻正
        return None
    if flux(cs1h, 2) <= 0.53:           # 买压到位
        return None

    return "LONG"   # 或 "SHORT"
```

**关键原则：每加一个条件必须有明确假设，不能为了凑胜率而加。**

---

## 四、探索方向规划表

填写新方向时，每行对应 `auto_explore.py` 里的一个主题生成器。

| 方向名称 | 市场假设 | 核心条件 | 参数范围 | 状态 |
|---------|---------|---------|---------|------|
| **DecelBounce_Extended** | 更长回调(h>14)或更强买压(f>0.55)进一步提升胜率 | 4h上涨+1h回调N根+反转+flux | h_n:15-25, f_min:0.55-0.60 | 待运行 |
| **FluxAcceleration** | 买压加速上升(flux递增)比绝对值更能预测反弹 | 4h上涨+1h回调+flux(2)>flux(4)>flux(8) | hist_n:4-10, f_abs:0.49-0.53 | 待运行 |
| **OversoldDeepBounce** | 极深超跌(-1%~-2%)后反弹力度更强、胜率更高 | 4h上涨+gradient(1h,N)<-1.0%+强力反转 | depth:0.008-0.020, h_n:4-10 | 待运行 |
| **BTCLeadAlt** | BTC强势时，仍在1h回调的山寨是滞后跟涨机会 | BTC 4h强>0.7%+山寨1h仍回调+买压回升 | btc_th:0.005-0.010 | 待运行 |
| **VolCompressionBreakout** | 振幅收缩后突破往往是更可靠趋势起点 | 振幅(2)<振幅(8)*0.7+gradient(2)>0.4% | compress:0.6-0.8 | 待运行 |
| *(下一个新方向)* | *(填写假设)* | *(填写条件)* | *(填写参数)* | 待规划 |

---

## 五、如何新增一个探索方向

**第一步：在这个文档里填写一行方向规划表**（先想清楚假设）

**第二步：在 `auto_explore.py` 里添加工厂函数**

```python
def make_我的新策略(参数1, 参数2):
    """简短描述市场逻辑"""
    def signal(cs1h, cs4h):
        if len(cs1h) < 10 or len(cs4h) < 8: return None
        # ... 你的条件 ...
        return "LONG"
    return signal
```

**第三步：添加主题生成器**

```python
def theme_我的新方向():
    strategies = []
    for p1 in [值1, 值2, 值3]:
        for p2 in [值1, 值2]:
            strategies.append({
                "name": f"我的策略_p1{p1}_p2{p2}",
                "fn":   make_我的新策略(p1, p2),
                "mode": "mtf_self",   # 或 "mtf_btc"
            })
    return strategies
```

**第四步：注册到主题列表**

```python
EXPLORATION_THEMES = [
    ...
    ("我的新方向", theme_我的新方向),   # ← 加这行
]
```

---

## 六、夜间运行方式

```bash
# 运行所有主题（约 30-60 分钟）
.venv/Scripts/python.exe auto_explore.py

# 只运行指定主题
.venv/Scripts/python.exe auto_explore.py --theme DecelBounce_Extended

# 查看所有主题及策略数量
.venv/Scripts/python.exe auto_explore.py --list-themes
```

结果文件：
- `logs/explore_YYYYMMDD_HHMM.log` — 完整运行日志
- `logs/explore_YYYYMMDD_HHMM_passed.csv` — 通过策略汇总（可用Excel打开）

---

## 七、结果解读标准

| 指标 | 优秀 | 合格 | 危险信号 |
|------|------|------|---------|
| S4测试WR | ≥65% | 57-65% | <57% |
| S4 n | ≥100 | 50-100 | <50（不可靠）|
| 训练-测试WR差 | <5% | 5-10% | >10%（过拟合）|
| S3训练WR | 60-68% | 57-70% | >72%（过拟合风险）|

**过拟合警报：训练>70% 但测试<57%，坚决不部署。**

---

## 八、探索历史记录

| 日期 | 脚本 | 测试策略数 | 通过数 | 最优策略 | 最优测试WR | 部署情况 |
|------|------|-----------|--------|---------|-----------|---------|
| 2026-04-14 | manual v1-v5 | ~25 | 13 | DB-Deep h=8 | 63.9% | E17 |
| 2026-04-14 | manual v6 | 13 | 8 | DB-h7/h10 | 64.1% | E19-E26 |
| 2026-04-14 | manual v7 | 11 | 4 | DB-h14 | 69.0% | E27-E30 |
| 2026-04-15 | auto_explore_alien      | ~300 | 9  | SellCap_L_n8_hi55_d51 | 59.5% | DB source=auto_explore_alien |
| 2026-04-15 | auto_explore_alien2     | ~800 | 39 | MomDecay_S_l8_r30     | 59.0% | DB source=auto_explore_alien2 |
| 2026-04-16 | auto_explore_alien3     | ~600 | 32 | CC_L_n10_lo30         | 62.x% | DB source=auto_explore_alien3 |
| 2026-04-16 | auto_explore_alien4     | ~500 | ~30 | (与 alien2/3 重名)    | -     | 重名被跳过，0 新增 |
| 2026-04-16~17 | auto_explore_alien5  | ~400 | 5  | TakerSus_L 系列       | 58-60%| DB source=auto_explore_alien5 |

**DB 当前快照**（`strategy_params` 表 2026-04-17）：

| source | n | 备注 |
|--------|---|------|
| signal_analysis     | 172 | 早期 SHORT/LONG 参数优化批次 |
| auto_explore_alien2 |  39 | MomDecay/PriceMem/CC 等 |
| auto_explore_alien3 |  27 | CC_L/PVel_L 等 |
| auto_explore_alien  |   9 | SellCap_L/BuyExh_S 等 |
| auto_explore_alien5 |   5 | TakerSus_L 系列 |
| **合计**            | **252** | 全部统一为 2%SL/3%TP/3h |

---

## 九、去重机制：已探索策略不再重跑（2026-04-17）

**规则**：`strategy_params` 表里出现过的策略名，下次运行 `auto_explore_alien*.py`
时**自动跳过**，不再进入四阶段验证，节省时间。

**实现**：`explored_filter.py` 共享模块 + 所有 5 个 alien 脚本在主循环前过滤候选。

```bash
# 正常运行：自动跳过已部署策略
.venv/Scripts/python.exe auto_explore_alien5.py

# 强制重跑（比如调整了信号函数需要重新验证）
.venv/Scripts/python.exe auto_explore_alien5.py --force
```

**日志里会看到：**

```
主题: TakerSuspend (12 待跑 / 18 总候选，跳过 6 个已部署)
[OrderFlow] 全部已在 strategy_params，跳过。--force 可强制重跑。
```
