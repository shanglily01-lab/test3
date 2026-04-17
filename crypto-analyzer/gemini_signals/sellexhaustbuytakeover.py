# -*- coding: utf-8 -*-
"""
gemini_signals/sellexhaustbuytakeover.py
========================================
主题: SellExhaustBuyTakeover
生成时间: 2026-04-17 11:57:33
由 gemini_theme_probe.py 自动生成，请不要手改本文件。
"""
from __future__ import annotations

from primitives_gemini import exec_namespace


THEME_NAME   = 'SellExhaustBuyTakeover'
GENERATED_AT = '2026-04-17T11:57:33'

# 每个策略独立的 exec 命名空间，避免互相覆盖。

# ── [0] SEBT_L_v3  (LONG)  test_wr=61.2% ─
# hypothesis: 空间迁移模型：价格在近10根处于底部，但近3根开始向区间上半区移动，且卖压加速度消失。
_CODE_0 = "def sig(cs1h, cs4h):\n    if len(cs1h) < 20 or len(cs4h) < 5: return None\n    if gradient(cs4h, 4) < -0.004: return None\n    sp_long = _spatial_close(cs1h, 12)\n    sp_short = _spatial_close(cs1h, 3)\n    sat_v = _saturation_velocity(cs1h, 5, 1)\n    if sp_long < 0.35 and sp_short > 0.5 and sat_v < 0:\n        return 'LONG'\n    return None"
_NS_0 = exec_namespace()
exec(compile(_CODE_0, '<gemini:SEBT_L_v3>', 'exec'), _NS_0)
_SIG_0 = _NS_0['sig']

STRATEGIES: list[dict] = [
    {"name": 'SEBT_L_v3', "direction": 'LONG', "hypothesis": '空间迁移模型：价格在近10根处于底部，但近3根开始向区间上半区移动，且卖压加速度消失。', "fn": _SIG_0},
]
