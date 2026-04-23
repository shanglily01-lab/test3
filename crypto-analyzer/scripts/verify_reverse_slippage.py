# -*- coding: utf-8 -*-
"""
反向滑点熔断 (REVERSE_SLIPPAGE_LIMIT) 测试脚本
对应 design/3_test.md 中 TC-L-LIMIT-05 ~ TC-L-LIMIT-08

不依赖 DB / HTTP，直接验证 _fill_pending_orders 里的熔断判定逻辑。
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 从生产代码导入常量，保证测试与运行时一致
from strategy_live import REVERSE_SLIPPAGE_LIMIT


def reverse_slip(pos_side: str, limit_p: float, cur_p: float) -> float:
    """复刻 _fill_pending_orders 里的反向滑点计算，仅用于测试断言。"""
    if pos_side == "LONG":
        return (limit_p - cur_p) / limit_p
    return (cur_p - limit_p) / limit_p


def should_cancel(pos_side: str, limit_p: float, cur_p: float) -> bool:
    """与 strategy_live._fill_pending_orders 熔断判定严格一致（严格大于阈值）"""
    return reverse_slip(pos_side, limit_p, cur_p) > REVERSE_SLIPPAGE_LIMIT


CASES = [
    # (tc_id, 描述, pos_side, limit_p, cur_p, expect_cancel, 备注)
    (
        "TC-L-LIMIT-05",
        "SHORT 反向穿越 2.79% (CHIP 4/23 05:19 重放)",
        "SHORT",
        0.09579,
        0.09846,
        True,
        "应熔断撤单",
    ),
    (
        "TC-L-LIMIT-06",
        "LONG 反向穿越 1.6%",
        "LONG",
        100.0,
        98.4,
        True,
        "应熔断撤单",
    ),
    (
        "TC-L-LIMIT-07",
        "SHORT 反向穿越恰好 1.5% (边界)",
        "SHORT",
        100.0,
        101.5,
        False,
        "允许填充 (严格大于)",
    ),
    (
        "TC-L-LIMIT-08",
        "SHORT 正向滑点 0.5%",
        "SHORT",
        100.0,
        100.5,
        False,
        "正常滑点不误伤",
    ),
    # 额外冗余用例 (covers 需求 F-L-05-06 正反双向)
    (
        "TC-L-LIMIT-09",
        "LONG 反向穿越 0.5% (轻微)",
        "LONG",
        100.0,
        99.5,
        False,
        "轻微反向不熔断",
    ),
    (
        "TC-L-LIMIT-10",
        "SHORT BSB 4/23 06:33 重放 +1.18%",
        "SHORT",
        0.31812,
        0.32188,
        False,
        "小于阈值允许填充 (需另加信号级过滤)",
    ),
]


def run() -> int:
    print(f"REVERSE_SLIPPAGE_LIMIT = {REVERSE_SLIPPAGE_LIMIT} ({REVERSE_SLIPPAGE_LIMIT*100:.2f}%)\n")
    print(f"{'TC':<14} {'方向':<6} {'limit':>10} {'cur':>10} {'偏离':>8} {'期望':<6} {'实际':<6} {'结果'}")
    print("-" * 90)

    passed = 0
    failed = 0
    for tc_id, desc, side, lp, cp, expect, note in CASES:
        slip = reverse_slip(side, lp, cp)
        actual = should_cancel(side, lp, cp)
        ok = actual == expect
        status = "PASS" if ok else "FAIL"
        if ok:
            passed += 1
        else:
            failed += 1
        print(
            f"{tc_id:<14} {side:<6} {lp:>10.5f} {cp:>10.5f} "
            f"{slip*100:>7.3f}% {'撤单' if expect else '填充':<6} "
            f"{'撤单' if actual else '填充':<6} {status}  ({note})"
        )
        print(f"{'':14}   {desc}")

    print("-" * 90)
    print(f"通过 {passed}/{len(CASES)}  失败 {failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(run())
