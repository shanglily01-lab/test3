# -*- coding: utf-8 -*-
"""
gemini_signals
==============
本包存放由 `gemini_theme_probe.py` 通过 Gemini + 原语对话产出、
并通过四阶段回测验证的 alpha 信号函数。

每个主题一个模块，文件名格式 `<theme_slug>.py`，模块内约定暴露：

    STRATEGIES: list[dict]
        其中每个 dict 形如：
          {
              "name":      "Theme_L_xxx",     # 与 strategy_params 同名
              "direction": "LONG" / "SHORT",
              "fn":        callable(cs1h, cs4h) -> "LONG"/"SHORT"/None,
              "hypothesis":"...市场逻辑...",
          }

    THEME_NAME: str     # 方便追溯原始主题描述
    GENERATED_AT: str   # ISO 时间戳

`dimension_trader.py` 的 `_load_gemini_registry()` 会枚举本包下所有模块、
与 DB `strategy_params` 交叉匹配 (source LIKE 'gemini_%')，自动挂载到
compute_signal 的 LONG / SHORT 分支。
"""
