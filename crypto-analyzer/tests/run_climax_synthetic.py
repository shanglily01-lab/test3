# -*- coding: utf-8 -*-
"""合成 1H 数据跑 topshort-climax 纯逻辑（无 DB、无下单）。python tests/run_climax_synthetic.py"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import strategy_live as sl  # noqa: E402

H = 3600000


def bar(t0, o, hi, lo, c, vol):
    return {
        "open_time": t0,
        "open_price": o,
        "high_price": hi,
        "low_price": lo,
        "close_price": c,
        "volume": vol,
    }


def run_case(name, rows, now_ms, price):
    ok, det = sl.evaluate_topshort_climax_signal(rows, now_ms, price)
    print(f"\n--- {name} ---")
    print(f"  signal={ok}  price={price:.4f}")
    for k, v in sorted(det.items()):
        if k != "ok" or ok:
            print(f"  {k}={v}")
    return ok


def build_base_rows(n=40, base_t=1_700_000_000_000):
    """前段横盘小量，占位到 index 29；30 起留给场景改写。"""
    rows = []
    for i in range(n):
        t = base_t + i * H
        o, hi, lo, c = 100.0, 100.5, 99.8, 100.1
        rows.append(bar(t, o, hi, lo, c, 80.0))
    return rows


def main():
    base_t = 1_700_000_000_000
    n = 40
    # 领袖大阳在 index 30：O=100 H=110 L=99 C=108，振幅 11%，实体够大；前 20 根均量 80，本根 4000
    rows = build_base_rows(n, base_t)
    ci = 30
    t = base_t + ci * H
    rows[ci] = bar(t, 100.0, 110.0, 99.0, 108.0, 4000.0)
    # 31、32：更小振幅的阳，不能抢领袖；确认再等 2 根规则已满足
    rows[31] = bar(base_t + 31 * H, 108.0, 109.0, 107.0, 107.5, 100.0)
    rows[32] = bar(base_t + 32 * H, 107.5, 108.0, 106.5, 106.8, 100.0)
    # 最后一根 39：收盘弱于领袖阳收盘 108
    rows[39] = bar(base_t + 39 * H, 107.0, 107.5, 106.0, 106.5, 120.0)
    # now：最后一根已收盘之后
    now_ms = base_t + 40 * H + 60_000
    # 价从高点 110 回撤约 2.5%
    price_ok = 107.3
    price_fail_pull = 109.5
    run_case("A 典型见顶（应通过）", rows, now_ms, price_ok)
    run_case("B 现价相对高点回撤不足（应失败）", rows, now_ms, price_fail_pull)

    # C：领袖阳后 last 收盘仍 >= 领袖收盘
    rows_c = build_base_rows(n, base_t + 1_000_000_000)
    rows_c[30] = bar(base_t + 1_000_000_000 + 30 * H, 100.0, 110.0, 99.0, 108.0, 4000.0)
    rows_c[31] = bar(base_t + 1_000_000_000 + 31 * H, 108.0, 109.0, 107.0, 107.5, 100.0)
    rows_c[32] = bar(base_t + 1_000_000_000 + 32 * H, 107.5, 108.0, 106.5, 106.8, 100.0)
    rows_c[39] = bar(base_t + 1_000_000_000 + 39 * H, 108.0, 109.5, 107.5, 108.5, 150.0)
    now_c = base_t + 1_000_000_000 + 40 * H + 60_000
    run_case("C 最后一根又收回领袖阳之上（应失败）", rows_c, now_c, 107.3)

    # D：领袖不是最大阳 —— index 28 造更大振幅阳，领袖窗口仍含 28..37 时 bull_leader 应落到 28
    rows_d = build_base_rows(n, base_t + 2_000_000_000)
    rows_d[28] = bar(base_t + 2_000_000_000 + 28 * H, 100.0, 112.0, 99.0, 109.0, 4000.0)
    rows_d[30] = bar(base_t + 2_000_000_000 + 30 * H, 100.0, 108.0, 99.5, 105.0, 4000.0)
    for j in range(31, 39):
        rows_d[j] = bar(base_t + 2_000_000_000 + j * H, 105.0, 105.5, 104.5, 105.0, 90.0)
    rows_d[39] = bar(base_t + 2_000_000_000 + 39 * H, 105.0, 105.3, 104.0, 104.2, 100.0)
    now_d = base_t + 2_000_000_000 + 40 * H + 60_000
    run_case("D 更大阳在 index28 → 领袖变为 28（不应再用 30 当顶；本例仍满足则 True）", rows_d, now_d, 104.0)

    print("\n完成。")


if __name__ == "__main__":
    main()
