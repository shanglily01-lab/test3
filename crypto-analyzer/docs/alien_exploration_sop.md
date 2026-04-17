# Alien Lens 策略探索 SOP

> 路径 C：批次化原语探索（区别于路径A Gemini探索、路径B网格枚举）  
> 每个阶段有明确的**入口条件 → 执行内容 → 验收标准 → 失败处理**  
> 更新时间：2026-04-16

---

## 批次总览

| 批次 | 编号 | 状态 | 通过数 |
|------|------|------|--------|
| Batch 1 | A1–A9 | 完成+已集成 | 9 |
| Batch 2 | A10–A48 | 完成+已集成 | 39 |
| Batch 3 | A49+ | **进行中** | 待定 |
| Batch 4 | TBD | 未开始 | — |

**Batch 3 当前进度：**

| 主题 | S4 | 通过数 | 状态 |
|------|----|--------|------|
| OrderFlowPolarization | 通过 | 8 | 已auto-deploy |
| BodyDominanceBurst | 淘汰 | 0 | 丢弃 |
| FluxMemoryExtreme | 淘汰 | 0 | 丢弃 |
| VolMomentumDivergence | 通过 | 12 | 已auto-deploy |
| CloseConsistency | **S4 pending** | 待定 | alien3 跑中 |
| AmplitudeSkewSignal | 未启动 | — | 待跑 |
| PriceVelocityExhaustion | 未启动 | — | 待跑 |
| EntropyVelocityBreak | 未启动 | — | 待跑 |

---

## Phase 0：原语设计

### 入口条件
- 上一批次已全部完成 Phase 5 验收
- 已有原语列表已整理（避免重复）

### 执行内容

**每个原语必须定义以下四要素（写入脚本头部注释）：**

```python
# 原语名：CloseConsistency
# 物理含义：近 n 根K线收盘价持续偏向某侧，衡量方向一致性
# 参数轴：n（窗口长度）, threshold（一致性阈值%）
# 方向假设：持续低收盘 → 超卖反弹信号（LONG）；持续高收盘 → 超买回落（SHORT）
```

**批次规格：**
- 每批次 8 个原语
- 每个原语设计 8-24 个参数变体（LONG + SHORT 各自计数）
- 总候选数目标：80-160 个

### 验收标准

| 检查项 | 合格条件 | 不合格处理 |
|--------|---------|-----------|
| 原语数量 | 恰好 8 个 | 补充或削减 |
| 文档完整性 | 每个原语有四要素注释 | 补充注释 |
| 与已有原语重叠 | 无概念完全相同的重复 | 替换为新方向 |
| 脚本可运行 | `python -c "import auto_explore_alienN"` 无报错 | 修复语法 |
| 候选总数 | 80–160 个 | 调整参数网格密度 |

**输出物：** `auto_explore_alienN.py` 脚本就绪，注释完整

---

## Phase 1：漏斗探索（S1→S4）

### 入口条件
- Phase 0 验收通过
- 脚本无报错，可正常导入

### 执行内容

```bash
# 启动
nohup .venv/Scripts/python -u auto_explore_alienN.py \
  >> logs/alienN_explore_run.log 2>&1 &

# 监控
tail -f logs/alienN_explore_run.log | grep -E "--- S|PASS|FAIL|淘汰|通过"
```

**四阶段漏斗标准：**

| 阶段 | 标的 | WR门槛 | 最小样本 | 不通过则 |
|------|------|--------|---------|---------|
| S1 | Big4 训练集 | ≥57% | n≥5 | 丢弃候选 |
| S2 | 10随机山寨 训练集 | ≥58%（LONG宽松55%） | n≥15 | 丢弃候选 |
| S3 | 全量86山寨 训练集 | ≥60%（LONG宽松57%） | n≥30 | 丢弃候选 |
| S4 | 全量86山寨 测试集 | ≥57% PASS / 55-56% border | n≥20 | 丢弃候选 |

**S4 是唯一判定性阶段，S1-S3 只是过滤噪音。**

### 验收标准

| 检查项 | 合格条件 | 不合格处理 |
|--------|---------|-----------|
| 全部主题跑完 | 8个主题全部有 S4 结果 | 续跑未完成主题 |
| 有可用产出 | 至少 1 个主题有 S4 通过策略 | **整批次失败**，回 Phase 0 重新选题 |
| 日志完整 | 无 Exception / crash 截断 | 修复脚本后重跑中断主题 |
| 结果可读 | S4 PASS 列表可提取 | 手动整理日志 |

**批次产出原则：通过几个部署几个，不设最低数量门槛。**
单主题 0 通过 = 该主题原语方向失效，直接丢弃，不影响其他主题。
全批次 0 通过 = 整体选题失效，回 Phase 0 重新设计原语。

**输出物：** 每个主题的 S4 PASS / FAIL / border 列表（从日志提取）

---

## Phase 2：去冗余筛选

### 入口条件
- Phase 1 验收通过
- 已从日志提取出所有 S4 通过策略的 {名称, WR, ev, n, 参数}

### 执行内容

**去冗余五条规则（按顺序应用）：**

```
规则1：结果完全相同
  条件：两个变体 WR 差 < 0.5% 且 n（样本量）完全相同
  处理：只保留"最严格"入场条件的那个（阈值更极端）
  示例：CC_L_n6_lo20 / lo25 / lo30 n=14895 WR完全一样
        → 只保留 CC_L_n6_lo20

规则2：参数相邻且结果接近
  条件：WR 差 < 1% 且 n 差 < 20%
  处理：保留 ev 更高的；ev相同则保留样本量更大的
  示例：CC_S_n12_hi75 vs CC_S_n12_hi80 WR均64.5%
        → 只保留 hi75（宽门槛，样本量稍大）

规则3：不同窗口长度，WR有明显差异
  条件：n值不同，WR差 ≥ 2%
  处理：全部保留（代表不同"一致性持续时长"）
  示例：CC_L_n6 WR=59.3% vs CC_L_n12 WR=64.2% → 都保留

规则4：ev 显著高的策略强制保留
  条件：ev ≥ +0.8%
  处理：无论其他规则，保留（高期望值策略优先）

规则5：每主题每方向上限
  条件：每个主题 LONG 方向 ≤ 6 个，SHORT 方向 ≤ 6 个
  处理：超出时，按 ev 降序，截取前 N 个
```

### 验收标准

| 检查项 | 合格条件 | 不合格处理 |
|--------|---------|-----------|
| 无完全冗余 | 没有两个策略 WR差<0.5% 且 n完全相同 | 继续应用规则1 |
| 每主题数量 | 每方向 ≤ 6 个 | 按 ev 降序截取 |
| 批次总数 | 全批次 ≤ 40 个策略 | 各主题均匀压缩 |
| 最低质量线 | 每个保留策略 S4 WR ≥ 57%，ev ≥ +0.3% | 低于标准的丢弃 |
| 文档化 | 每个保留策略有一行记录（名称/WR/ev/n/参数） | 补充记录 |

**输出物：** 最终候选列表（Markdown 表格），含列：策略名 / 方向 / WR / ev / n / 参数值

---

## Phase 3：参数优化

### 入口条件
- Phase 2 最终候选列表确认
- `signal_analysis.py` 中已注册这批策略（ALL_STRATEGIES 字典）

### 执行内容

```bash
# 每批 5-8 个策略
.venv/Scripts/python signal_analysis.py --strategies A49 A50 A51 A52 A53

# 验证已写入DB
# SQL: SELECT strategy_name, sl_pct, tp_pct, hold_h, updated_at
#      FROM strategy_params WHERE strategy_name LIKE 'A%' ORDER BY strategy_name;
```

**参数合理性标准：**

| 参数 | 合理范围 | 超出范围的处理 |
|------|---------|--------------|
| hold_h | 1–24h | 超过24h → 检查信号持续性，手动设为12h |
| SL | 0.5%–3.0% | 超出范围 → 检查回测数据，手动设为1.9% |
| TP | SL×1.3 ≤ TP ≤ SL×3.0 | TP/SL比低于1.3 → 期望值为负，丢弃策略 |
| 净效益（SL） | ≥ +15%（输单被截% - 赢单被截%） | 净效益低 → 说明SL无效，放宽或丢弃 |
| TP命中率 | ≥ 40% | 命中率低 → 说明TP太激进，降低TP |

### 验收标准

| 检查项 | 合格条件 | 不合格处理 |
|--------|---------|-----------|
| DB写入完整 | 每个策略在 strategy_params 有记录 | 重跑 signal_analysis |
| 参数在合理范围 | 全部通过上表参数合理性标准 | 调整或丢弃不合理策略 |
| TP/SL比 | 全部 ≥ 1.3 | 期望值为负的策略从候选列表移除 |
| 净效益 | 全部 ≥ +15% | 净效益不达标 → 考虑放宽SL或丢弃 |
| 兜底参数 | _STRATEGY_PARAMS_DEFAULT 已更新 | 补充 |

**输出物：** 每个策略的 {hold_h, SL%, TP%} 已写入 DB + 代码兜底

---

## Phase 4：集成

### 入口条件
- Phase 3 全部通过
- 本批次策略编号已确定（如 A49-A68）

### 执行内容

**集成代码模式（dispatch模式，不改动已有逻辑）：**

```python
# 1. helper 函数（每个原语一个）
def _sig_cc_long(cs1h: list, n: int, lo_pct: float) -> bool:
    if len(cs1h) < n:
        return False
    closes = [c["close"] for c in cs1h[-n:]]
    # ... 计算逻辑
    return consistency >= 0.8

# 2. dispatch 列表（每个方向一个）
_CC_LONG_LIST = [
    (6,  0.20, "A49-CCLong-n6"),
    (8,  0.20, "A50-CCLong-n8"),
    ...
]

# 3. 接入 compute_signal()（置于对应的 symbol set 判断块内）
for _n, _lo, _nm in _CC_LONG_LIST:
    if _sig_cc_long(cs1h, _n, _lo):
        return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
```

**集成 Checklist：**

```
[ ] helper 函数添加完整（无语法错误）
[ ] dispatch 列表与最终候选列表一一对应
[ ] compute_signal() 接入位置正确（在对应 symbol set 块内）
[ ] _STRATEGY_PARAMS_DEFAULT 每个策略都有兜底参数
[ ] alien_strategies_doc.md 已更新（原语定义 + 最终通过策略 + 回测数字）
```

### 验收标准

| 检查项 | 合格条件 | 不合格处理 |
|--------|---------|-----------|
| 语法检查 | `python -m py_compile dimension_trader.py` 无报错 | 修复语法后重检 |
| 干跑验证 | `scan_once() --dry-run` 至少触发1个新策略信号 | 检查逻辑条件是否过严 |
| 无已有策略破坏 | 已有策略（A1-A48）信号触发数变化 < 10% | 检查 dispatch 顺序是否覆盖了之前的分支 |
| 启动无报错 | dimension_trader 重启后日志无 ERROR/CRITICAL | 排查错误后修复 |
| 编号连续 | 新策略编号从上一批次结束+1 开始，无跳号 | 补全缺失编号 |

**失败处理：** 集成出错时 git diff 回滚，不重启，保持已有策略运行。

**输出物：** dimension_trader.py 更新完毕，PID 锁保护下重启成功

---

## Phase 5：实盘验证

### 入口条件
- Phase 4 通过，dimension_trader 已重启运行
- 观察期：**48小时**（2个完整交易日）

### 执行内容

```bash
# 实时监控新策略信号
grep "SIGNAL A4[9-9]\|SIGNAL A5\|SIGNAL A6\|SIGNAL A7" logs/dimension_trader.log

# 统计新策略开仓/止盈/止损
.venv/Scripts/python -c "
import pymysql
import os
conn = pymysql.connect(host=os.getenv('DB_HOST','localhost'), port=int(os.getenv('DB_PORT','3306')),
                       user=os.getenv('DB_USER',''), password=os.getenv('DB_PASSWORD',''),
                       db=os.getenv('DB_NAME',''), charset='utf8mb4')
# 查新策略 48h 内的交易结果
"
```

### 验收标准

**信号触发检查（24h内）：**

| 检查项 | 合格条件 | 不合格处理 |
|--------|---------|-----------|
| 新策略触发数 | 每个策略 24h 内在全量标的触发 ≥ 1 次 | 检查逻辑条件，必要时放宽阈值 |
| 触发过于频繁 | 单策略 24h 触发 ≤ 50 次 | 收紧阈值，防止过度交易 |
| 信号价格偏差 | 开仓价与信号检测价偏差 < SL距离的 50% | 已有 live_price 保护；若仍超出则排查数据 |

**交易质量检查（48h后，需至少 10 个样本）：**

| 指标 | 合格条件 | 不合格处理 |
|------|---------|-----------|
| 立即止损率 | 开仓后 5 分钟内止损的比例 < 10% | 排查 SL 位置是否合理 |
| 实盘 WR | 回测 WR ± 10% 范围内 | 样本<10不评判；超出范围排查过拟合 |
| ROI 均值 | > 0（正期望） | 若连续 20 单 ROI 均值 < 0 → 下线策略 |
| 无异常大亏单 | 单笔 ROI < -20%（含手续费） | 排查 SL 是否失效 |

**合格判定：** 全部 6 项指标通过 → 本批次验收完成，进入常态运行

**部分不合格处理（分级）：**

```
轻微问题（不下线）：
  - 触发频率偏高/偏低 → 调整阈值，重启
  - WR 略低于预期（-10%内）→ 观察延长至 96h

需要下线的情况：
  - 立即止损率 ≥ 10%
  - ROI 均值连续 20 单 < 0
  - 单笔亏损 > -20%
  操作：从 compute_signal() 注释掉对应 dispatch 块，重启；不删代码

整批次失败（全部下线）：
  - 新策略上线后已有策略成交量下降 > 30%（说明资源竞争严重）
  - 系统稳定性下降（ERROR 日志增加 > 50%）
```

**输出物：** 48h 验收报告（记录到 `alien_strategies_doc.md`：策略名/触发数/WR/ROI均值/结论）

---

## 阶段关系总图

```
Phase 0: 原语设计
    ↓ [验收通过：8原语文档化，脚本就绪]
Phase 1: 漏斗探索 S1→S4
    ↓ [验收通过：≥2主题，≥5策略通过S4]
    ✗ [全批次0策略通过] → 回 Phase 0 重新选题
    → [部分主题通过] → 通过几个进 Phase 2 几个，淘汰主题直接丢弃
Phase 2: 去冗余筛选
    ↓ [验收通过：最终候选列表，≤40策略，无冗余]
Phase 3: 参数优化
    ↓ [验收通过：所有策略 SL/TP/hold_h 合理，DB写入]
    ✗ [TP/SL比<1.3 或 净效益<15%] → 丢弃该策略，继续其余
Phase 4: 集成
    ↓ [验收通过：语法无误，干跑触发，已有策略不受影响]
    ✗ [集成报错] → git 回滚，修复后重试
Phase 5: 实盘验证（48h）
    ↓ [验收通过：6项指标全合格]
    ✗ [部分不合格] → 分级处理（调参/下线/延长观察）
    ↓ [全部通过]
批次完成 → 更新 alien_strategies_doc.md → 开始规划下一批次 Phase 0
```

---

## Batch 3 剩余步骤

| # | 步骤 | 当前状态 | 负责操作 |
|---|------|---------|---------|
| 1 | 等待 alien3 跑完 CC S4 + 剩余3主题 | alien3 运行中 | 监控日志 |
| 2 | Phase 2: CC 去冗余筛选 | 待 S4 结果 | 人工按规则执行 |
| 3 | Phase 2: AmplitudeSkew / PriceVelocity / EntropyVelocity 筛选 | 待 S4 结果 | 同上 |
| 4 | Phase 3: 全批次参数优化 | 待候选列表确认 | signal_analysis.py |
| 5 | Phase 4: 集成 A49+ 到 dimension_trader | 待参数优化 | 集成 + 重启 |
| 6 | Phase 5: 48h 实盘验证 | 待集成 | 监控 + 写报告 |

---

## Batch 4 规划（Phase 0 模板）

启动条件：Batch 3 Phase 5 验收完成

候选原语方向（已有Batch1-3未覆盖）：

| 原语 | 维度 | 方向假设 |
|------|------|---------|
| WickBalance | 上下影线能量比 | 影线失衡预示次日方向 |
| VolumeAcceleration | 成交量加速度二阶导 | 量能爆发前兆 |
| PriceRangeContraction | 振幅收缩连续性 | 盘整突破前低波动 |
| MomentumDivergence | 1h vs 4h 动量背离 | 多周期分歧 = 转折 |
| LiquidityCluster | 近期密集成交价引力 | 价格回归支撑/压力 |
| TemporalAsymmetry | 上涨/下跌K线速度比 | 下跌慢/上涨快 = 健康趋势 |
| CandleQualityDecay | 连续阳线实体占比递减 | 上涨乏力预警 |
| VolumePriceEfficiency | vol增量 / 价格移动量 | 高耗能低产出 = 顶底 |

---

## 关键文件索引

| 文件 | 用途 | 更新时机 |
|------|------|---------|
| `auto_explore_alien3.py` | Batch 3 探索脚本 | Phase 1 |
| `logs/alienN_explore_run.log` | 探索过程结果 | Phase 1 实时 |
| `docs/alien_strategies_doc.md` | 已部署策略文档 | Phase 4/5 |
| `dimension_trader.py` | 信号函数 + dispatch | Phase 4 |
| `signal_analysis.py` | 参数优化工具 | Phase 3 |
| DB `strategy_params` | 实盘参数（每小时reload） | Phase 3 |

---

*本 SOP 与 `strategy_discovery_methodology.md` 并行：前者覆盖路径A/B通用流程，本文档专注路径C批次化原语探索*
