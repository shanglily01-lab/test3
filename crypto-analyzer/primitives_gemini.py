#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
primitives_gemini.py
====================
为 `gemini_theme_probe.py` 提供一个**纯函数**原语库，作为 Gemini 生成信号代码
的 exec 命名空间。所有函数都是无副作用的、仅依赖 K 线 dict 列表的纯计算。

K 线 dict 字段（每根）：
    open, high, low, close, vol, buy_vol, t(unix timestamp)

该库对 `dimension_trader.py` 里的原语做了精确复刻（无依赖 DB/网络），以便
Gemini 产出的代码可以被回测引擎安全 exec。请保持两边签名一致。
"""

from __future__ import annotations

import math


# ── 基础动量 / 振幅 / 买压 ────────────────────────────────────────────────────

def gradient(cs: list, n: int) -> float:
    """归一化净涨跌: sum(close-open over n) / current_close"""
    if len(cs) < n:
        return 0.0
    s = sum(c["close"] - c["open"] for c in cs[-n:])
    ref = cs[-1]["close"]
    return s / ref if ref else 0.0


def amplitude(cs: list, n: int) -> float:
    """归一化平均振幅: mean(high-low over n) / current_close"""
    if len(cs) < n:
        return 0.0
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.0


def flux(cs: list, n: int) -> float:
    """买压比率: mean(buy_vol/vol over n), 0.5 中性"""
    if len(cs) < n:
        return 0.5
    rs = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5


# ── 空间 / 位置记忆 ───────────────────────────────────────────────────────────

def _spatial_close(cs: list, n: int) -> float:
    """空间收盘得分 [0,1]: mean((close-low)/(high-low) over n)"""
    if len(cs) < n:
        return 0.5
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        scores.append((c["close"] - c["low"]) / rng if rng > 0 else 0.5)
    return sum(scores) / len(scores) if scores else 0.5


def _price_memory(cs: list, n: int) -> float:
    """价格区间位置 [0,1]: 1=近期顶 0=近期底"""
    if len(cs) < n:
        return 0.5
    hi = max(c["high"] for c in cs[-n:])
    lo = min(c["low"]  for c in cs[-n:])
    rng = hi - lo
    if rng <= 0:
        return 0.5
    return (cs[-1]["close"] - lo) / rng


def _close_consistency(cs: list, n: int) -> float:
    """收盘一致性 [0,1]: 最近n根收盘偏上半区的比例"""
    if len(cs) < n:
        return 0.5
    count = 0.0
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            count += 0.5
        elif (c["close"] - c["low"]) / rng > 0.5:
            count += 1.0
    return count / n


# ── 卖压 / 买压 / 动量比 ──────────────────────────────────────────────────────

def _sell_saturation(cs: list, n: int) -> float:
    """卖方饱和度 [0,1]: 1 - avg(buy_vol/vol over n)"""
    if len(cs) < n:
        return 0.5
    rs = [(c["vol"] - c["buy_vol"]) / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5


def _flux_momentum(cs: list, short_n: int, long_n: int) -> float:
    """流量动量: flux(short_n) - flux(long_n), >0 = 近期买压加速"""
    if len(cs) < long_n:
        return 0.0
    return flux(cs, short_n) - flux(cs, long_n)


def _order_flow_delta(cs: list, n: int) -> float:
    """订单净流量比 [-1,1]: sum(buy - sell) / sum(vol), 正=净买方主导"""
    if len(cs) < n:
        return 0.0
    net = sum(c["buy_vol"] - (c["vol"] - c["buy_vol"]) for c in cs[-n:])
    total = sum(c["vol"] for c in cs[-n:])
    return net / total if total > 0 else 0.0


def _saturation_velocity(cs: list, n: int, lag: int) -> float:
    """卖压加速度: _sell_saturation(now,n) - _sell_saturation(lag bars ago,n)"""
    if len(cs) < n + lag:
        return 0.0
    return _sell_saturation(cs, n) - _sell_saturation(cs[:-lag], n)


def _momentum_ratio(cs: list, short_n: int, long_n: int) -> float:
    """动量比: gradient(short_n)/gradient(long_n), 衰减时 <1"""
    if len(cs) < long_n + 2:
        return 1.0
    g_s = sum(c["close"] - c["open"] for c in cs[-short_n:])
    g_l = sum(c["close"] - c["open"] for c in cs[-long_n:])
    ref = cs[-1]["close"]
    if ref <= 0:
        return 1.0
    gs, gl = g_s / ref, g_l / ref
    if abs(gl) < 1e-9:
        return 0.0 if abs(gs) < 1e-9 else 2.0
    return gs / gl


def _vol_momentum(cs: list, n: int, lag: int) -> float:
    """成交量动量: avg_vol(now,n)/avg_vol(lag ago,n) - 1"""
    if len(cs) < n + lag:
        return 0.0
    v_now = sum(c["vol"] for c in cs[-n:]) / n
    v_past = sum(c["vol"] for c in cs[-n - lag:-lag]) / n
    return (v_now / v_past - 1) if v_past > 0 else 0.0


# ── 形态 / 能量 ────────────────────────────────────────────────────────────────

def _amplitude_skew(cs: list, n: int) -> float:
    """上下影线偏斜度: avg(up_wick/range) - avg(down_wick/range)"""
    if len(cs) < n:
        return 0.0
    upper, lower = [], []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        bt = max(c["open"], c["close"])
        bb = min(c["open"], c["close"])
        upper.append((c["high"] - bt) / rng)
        lower.append((bb - c["low"]) / rng)
    if not upper:
        return 0.0
    return sum(upper) / len(upper) - sum(lower) / len(lower)


def _price_velocity(cs: list, n: int, amp_n: int) -> float:
    """振幅归一化动量: gradient(n) / amplitude(amp_n)"""
    if len(cs) < max(n, amp_n) + 2:
        return 0.0
    amp = amplitude(cs, amp_n)
    return gradient(cs, n) / amp if amp > 0 else 0.0


def _time_pressure(cs: list, n: int, amp_th: float) -> float:
    """时间压力 [0,1]: 最近n根中振幅<amp_th的蜡烛占比"""
    if len(cs) < n:
        return 0.5
    return sum(
        1 for c in cs[-n:]
        if c["close"] > 0 and (c["high"] - c["low"]) / c["close"] < amp_th
    ) / n


def _entropy_velocity(cs: list, n: int, lag: int) -> float:
    """熵速率: 二元方向香农熵的差, 负值 = 方向共识形成"""
    if len(cs) < n + lag:
        return 0.0

    def _bent(window):
        if len(window) < 2:
            return 0.0
        up = sum(1 for c in window if c["close"] >= c["open"]) / len(window)
        dn = 1.0 - up
        if up <= 0 or dn <= 0:
            return 0.0
        return -(math.log(up) * up + math.log(dn) * dn)

    return _bent(cs[-n:]) - _bent(cs[-n - lag:-lag])


# ── alien5 新增原语（能量密度/静默/doji/聚集/承托/收缩/方向熵/量价相关）────────

def vol_energy_density(cs: list, n_near: int, n_far: int) -> float:
    """近n_near根成交量均值 / 远n_far根成交量均值, >1=能量在聚集"""
    if len(cs) < n_far + n_near:
        return 1.0
    near = sum(c["vol"] for c in cs[-n_near:]) / n_near
    far = sum(c["vol"] for c in cs[-n_far - n_near:-n_near]) / n_far
    return near / far if far > 0 else 1.0


def amplitude_silence(cs: list, n: int) -> float:
    """振幅静默度 [0,1]: 最近n根中振幅低于历史中位的比例"""
    if len(cs) < n * 2:
        return 0.5
    amps = [
        (c["high"] - c["low"]) / c["close"] if c["close"] > 0 else 0.0
        for c in cs[-n * 2:]
    ]
    hist = amps[:n]
    recent = amps[n:]
    if not hist:
        return 0.5
    sorted_hist = sorted(hist)
    median = sorted_hist[len(sorted_hist) // 2]
    if median <= 0:
        return 0.5
    return sum(1 for a in recent if a < median) / n


def doji_density(cs: list, n: int, body_ratio_max: float = 0.25) -> float:
    """doji 密度 [0,1]: 最近n根中实体占比小于 body_ratio_max 的比例"""
    if len(cs) < n:
        return 0.0
    count = 0
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            continue
        body = abs(c["close"] - c["open"])
        if body / rng < body_ratio_max:
            count += 1
    return count / n


def price_cluster_density(cs: list, n: int, zone_pct: float = 0.005) -> float:
    """价格聚集度 [0,1]: 最近n根收盘落在最末收盘 ±zone_pct 区间内的比例"""
    if len(cs) < n:
        return 0.0
    ref = cs[-1]["close"]
    lo = ref * (1 - zone_pct)
    hi = ref * (1 + zone_pct)
    cnt = sum(1 for c in cs[-n:] if lo <= c["close"] <= hi)
    return cnt / n


def taker_sustain(cs: list, n: int, threshold: float = 0.55) -> float:
    """taker 持续度 [0,1]: 最近n根 buy_vol/vol 连续高于 threshold 的比例"""
    if len(cs) < n:
        return 0.0
    cnt = sum(
        1 for c in cs[-n:]
        if c["vol"] > 0 and c["buy_vol"] / c["vol"] >= threshold
    )
    return cnt / n


def seq_contract(cs: list, max_n: int = 12) -> int:
    """连续收缩形态长度: 从最近一根往前数，连续 (high-low) 单调递减的长度"""
    if len(cs) < 3:
        return 0
    streak = 0
    prev_range = cs[-1]["high"] - cs[-1]["low"]
    for i in range(2, max_n + 1):
        if i > len(cs):
            break
        cur = cs[-i]
        rng = cur["high"] - cur["low"]
        if rng > prev_range:
            streak += 1
            prev_range = rng
        else:
            break
    return streak


def direction_entropy(cs: list, n: int) -> float:
    """方向香农熵 [0,1]: 基于近n根阴阳比例，高=方向混乱"""
    if len(cs) < n:
        return 1.0
    up = sum(1 for c in cs[-n:] if c["close"] >= c["open"]) / n
    dn = 1.0 - up
    if up <= 0 or dn <= 0:
        return 0.0
    return -(math.log(up) * up + math.log(dn) * dn) / math.log(2)


def vol_dir_correlation(cs: list, n: int) -> float:
    """量方向相关性 [-1,1]: 阳线量占总量比减去 0.5 再 ×2
    +1 = 所有量都在阳线上(强势多方)；-1 = 全在阴线上"""
    if len(cs) < n:
        return 0.0
    up_vol = sum(c["vol"] for c in cs[-n:] if c["close"] >= c["open"])
    total = sum(c["vol"] for c in cs[-n:])
    if total <= 0:
        return 0.0
    return (up_vol / total - 0.5) * 2


# ── 跨标的 ────────────────────────────────────────────────────────────────────

def _cross_residual(cs_alt: list, cs_ref: list, n: int) -> float:
    """超额动量: gradient_alt(n) - gradient_ref(n)"""
    def _g(cs, k):
        if len(cs) < k:
            return 0.0
        s = sum(c["close"] - c["open"] for c in cs[-k:])
        ref = cs[-1]["close"]
        return s / ref if ref else 0.0
    return _g(cs_alt, n) - _g(cs_ref, n)


# ── Exec namespace 工厂 ───────────────────────────────────────────────────────

_SAFE_BUILTINS = {
    "range": range, "len": len, "all": all, "any": any,
    "sum": sum, "min": min, "max": max, "abs": abs, "round": round,
    "list": list, "float": float, "int": int, "bool": bool, "str": str,
    "enumerate": enumerate, "zip": zip, "sorted": sorted, "map": map,
    "filter": filter, "True": True, "False": False, "None": None,
}


def exec_namespace() -> dict:
    """构造 Gemini 代码 exec 用的命名空间：仅暴露原语和安全内置。"""
    return {
        "__builtins__": _SAFE_BUILTINS,
        "math": math,
        # 基础
        "gradient": gradient,
        "amplitude": amplitude,
        "flux": flux,
        # 空间 / 位置
        "_spatial_close": _spatial_close,
        "_price_memory": _price_memory,
        "_close_consistency": _close_consistency,
        # 买卖压 / 动量
        "_sell_saturation": _sell_saturation,
        "_flux_momentum": _flux_momentum,
        "_order_flow_delta": _order_flow_delta,
        "_saturation_velocity": _saturation_velocity,
        "_momentum_ratio": _momentum_ratio,
        "_vol_momentum": _vol_momentum,
        # 形态 / 能量
        "_amplitude_skew": _amplitude_skew,
        "_price_velocity": _price_velocity,
        "_time_pressure": _time_pressure,
        "_entropy_velocity": _entropy_velocity,
        "vol_energy_density": vol_energy_density,
        "amplitude_silence": amplitude_silence,
        "doji_density": doji_density,
        "price_cluster_density": price_cluster_density,
        "taker_sustain": taker_sustain,
        "seq_contract": seq_contract,
        "direction_entropy": direction_entropy,
        "vol_dir_correlation": vol_dir_correlation,
        # 跨标的
        "_cross_residual": _cross_residual,
    }


PRIMITIVE_CATALOG = """\
## 可用原语目录（全部为纯函数，已在 exec 环境中提供）

### 基础
- gradient(cs, n)                    归一化净涨跌 [-1,+1], 正=上行
- amplitude(cs, n)                   归一化平均振幅 >0
- flux(cs, n)                        买压比率 [0,1], 0.5中性

### 空间 / 位置记忆
- _spatial_close(cs, n)              最近n根平均收盘位置 [0,1], 1=顶
- _price_memory(cs, n)               当前价在近n根区间的位置 [0,1]
- _close_consistency(cs, n)          最近n根收盘偏上半区比例 [0,1]

### 买卖压 / 动量
- _sell_saturation(cs, n)            卖方占比 [0,1]
- _flux_momentum(cs, short_n, long_n)  flux短期-长期差, >0=近期买压加速
- _order_flow_delta(cs, n)           净买量/总量 [-1,+1]
- _saturation_velocity(cs, n, lag)   卖压加速度（正=卖压变强）
- _momentum_ratio(cs, short_n, long_n)  gradient短长比，<1=动量衰减
- _vol_momentum(cs, n, lag)          成交量放大率，>0=量增

### 形态 / 能量
- _amplitude_skew(cs, n)             上影-下影偏斜，正=上方承压
- _price_velocity(cs, n, amp_n)      振幅归一化动量
- _time_pressure(cs, n, amp_th)      低波动蜡烛占比 [0,1], 高=蓄势
- _entropy_velocity(cs, n, lag)      方向熵速率，负=方向共识形成
- vol_energy_density(cs, n_near, n_far)  近/远成交量均值比 >1=能量聚集
- amplitude_silence(cs, n)           近n根振幅低于历史中位比例 [0,1]
- doji_density(cs, n, body_ratio_max)  最近n根实体小于阈值比例 [0,1]
- price_cluster_density(cs, n, zone_pct)  近n根收盘落在±zone_pct内比例
- taker_sustain(cs, n, threshold)    buy_vol/vol 连续高于阈值的比例
- seq_contract(cs, max_n)            从最近往前连续振幅递减条数(int)
- direction_entropy(cs, n)           归一化方向熵 [0,1]
- vol_dir_correlation(cs, n)         阳线量占比-0.5再×2  [-1,+1]

### 跨标的
- _cross_residual(cs_alt, cs_ref, n) alt比ref超额动量

## 允许使用的 Python 构造
range / len / all / any / sum / min / max / abs / round / list / float / int /
bool / str / enumerate / zip / sorted / map / filter / True / False / None。
以及 `math`（标准库 math 模块）。**禁止** import，任何 import 都会失败。

## 信号函数约定
- 函数名必须是 `sig(cs1h, cs4h)`
- 返回 "LONG" / "SHORT" / None 三者之一
- cs1h 是最近 30+ 根 1h 蜡烛；cs4h 是对齐到当前时点的 4h 蜡烛（8+ 根）
- 任何未满足条件都必须 return None，不要 raise
"""
