#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
signal_analysis.py
==================
对历史数据重跑策略，分析最优入场时机、持仓时间、SL/TP 倍率。
跑完后自动更新 dimension_trader.py 中的 STRATEGY_PARAMS。

用法:
  .venv/Scripts/python.exe signal_analysis.py --batch 1        # 跑第 1 批
  .venv/Scripts/python.exe signal_analysis.py --batch 2        # 跑第 2 批
  .venv/Scripts/python.exe signal_analysis.py --strategies E16 E21 E22
  .venv/Scripts/python.exe signal_analysis.py --start 2024-09-01 --end 2025-01-01
  .venv/Scripts/python.exe signal_analysis.py --no-update      # 只看结果，不更新参数
"""

import argparse
import os
import re
import sys
import time
from bisect import bisect_right
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pymysql
from dotenv import load_dotenv

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))

# ── 从 dimension_trader 导入策略函数和指标 ────────────────────────────────────

from functools import partial as _partial

from dimension_trader import (
    gradient, flux, amplitude,
    sig_E1,  sig_E2,  sig_E3,  sig_E4,  sig_E5,
    sig_E6,  sig_E7,  sig_E8,  sig_E9,  sig_E10,
    sig_E11, sig_E12, sig_E13, sig_E14, sig_E15,
    sig_E16, sig_E17, sig_E18, sig_E19, sig_E20,
    sig_E21, sig_E22, sig_E23, sig_E24, sig_E25,
    sig_E26, sig_E27, sig_E28, sig_E29, sig_E30,
    sig_E31, sig_E32, sig_E33, sig_E34, sig_E35,
    sig_E36, sig_E37, sig_E38, sig_E39, sig_E40,
    sig_E41, sig_E42, sig_E43, sig_E44, sig_E45,
    sig_E46, sig_E47, sig_E48, sig_E49, sig_E50,
    sig_E51, sig_E52, sig_E53, sig_E54, sig_E55,
    sig_E56, sig_E57, sig_E58, sig_E59, sig_E60,
    sig_E61, sig_E62, sig_E63, sig_E64, sig_E65,
    sig_E66, sig_E67, sig_E68, sig_E69, sig_E70,
    sig_E71, sig_E72, sig_E73, sig_E74, sig_E75,
    sig_E76, sig_E77, sig_E78, sig_E79, sig_E80,
    sig_E81, sig_E82, sig_E83, sig_E84, sig_E85,
    sig_E86, sig_E87, sig_E88, sig_E89, sig_E90,
    sig_E91, sig_E92, sig_E93, sig_E94, sig_E95,
    sig_E96, sig_E97, sig_E98,
    # Alien Batch1
    sig_A1, sig_A2, sig_A3, sig_A4, sig_A5,
    sig_A6, sig_A7, sig_A8, sig_A9,
    # Alien Batch2 helpers
    _sig_pm_short, _sig_pm_long,
    _sig_satvel_short, _sig_tp_short,
    _sig_fm_long, _sig_fm_short,
    # Alien Batch3 helpers
    _sig_ofd_long, _sig_volmom_long,
    # Alien Batch3 CC helpers
    _sig_cc_long,
    # Alien Batch5 PVel helpers
    _sig_pvel_long,
)

# ── Alien Batch2 partial 包装：固定参数，生成 fn(cs1h, cs4h) 接口 ──────────────
# PriceMemory SHORT (A10-A24)
_A10 = _partial(_sig_pm_short, mem_n=20, mem_hi=0.75)
_A11 = _partial(_sig_pm_short, mem_n=20, mem_hi=0.80)
_A12 = _partial(_sig_pm_short, mem_n=20, mem_hi=0.85)
_A13 = _partial(_sig_pm_short, mem_n=14, mem_hi=0.75)
_A16 = _partial(_sig_pm_short, mem_n=14, mem_hi=0.80)
_A21 = _partial(_sig_pm_short, mem_n=10, mem_hi=0.85)
_A22 = _partial(_sig_pm_short, mem_n=10, mem_hi=0.75)
_A23 = _partial(_sig_pm_short, mem_n=10, mem_hi=0.80)
_A24 = _partial(_sig_pm_short, mem_n=14, mem_hi=0.85)
# PriceMemory LONG (A14-A29)
_A14 = _partial(_sig_pm_long, mem_n=20, mem_lo=0.15)
_A15 = _partial(_sig_pm_long, mem_n=20, mem_lo=0.25)
_A17 = _partial(_sig_pm_long, mem_n=20, mem_lo=0.20)
_A18 = _partial(_sig_pm_long, mem_n=14, mem_lo=0.25)
_A19 = _partial(_sig_pm_long, mem_n=14, mem_lo=0.20)
_A20 = _partial(_sig_pm_long, mem_n=14, mem_lo=0.15)
_A26 = _partial(_sig_pm_long, mem_n=10, mem_lo=0.25)
_A27 = _partial(_sig_pm_long, mem_n=10, mem_lo=0.15)
_A29 = _partial(_sig_pm_long, mem_n=10, mem_lo=0.20)
# SatVelocity SHORT
_A25 = _partial(_sig_satvel_short, n=3, lag=3, vel_th=0.06)
_A31 = _partial(_sig_satvel_short, n=3, lag=2, vel_th=0.04)
_A34 = _partial(_sig_satvel_short, n=3, lag=4, vel_th=0.06)
_A36 = _partial(_sig_satvel_short, n=4, lag=4, vel_th=0.06)
_A37 = _partial(_sig_satvel_short, n=4, lag=2, vel_th=0.04)
_A48 = _partial(_sig_satvel_short, n=4, lag=4, vel_th=0.04)
# TimePressure SHORT
_A35 = _partial(_sig_tp_short, pres_n=8,  amp_th=0.006, pres_th=0.75)
_A38 = _partial(_sig_tp_short, pres_n=10, amp_th=0.006, pres_th=0.75)
_A45 = _partial(_sig_tp_short, pres_n=10, amp_th=0.006, pres_th=0.65)
# FluxMomentum SHORT
_A30 = _partial(_sig_fm_short, short_n=3, long_n=6,  fm_th=-0.03)
_A33 = _partial(_sig_fm_short, short_n=2, long_n=8,  fm_th=-0.04)
_A40 = _partial(_sig_fm_short, short_n=2, long_n=12, fm_th=-0.05)
# FluxMomentum LONG
_A28 = _partial(_sig_fm_long, short_n=3, long_n=12, fm_th=0.05)
_A32 = _partial(_sig_fm_long, short_n=3, long_n=12, fm_th=0.04)
_A39 = _partial(_sig_fm_long, short_n=2, long_n=8,  fm_th=0.04)
_A41 = _partial(_sig_fm_long, short_n=2, long_n=6,  fm_th=0.03)
_A42 = _partial(_sig_fm_long, short_n=2, long_n=12, fm_th=0.05)
_A43 = _partial(_sig_fm_long, short_n=2, long_n=6,  fm_th=0.04)
_A44 = _partial(_sig_fm_long, short_n=2, long_n=12, fm_th=0.03)
_A46 = _partial(_sig_fm_long, short_n=2, long_n=8,  fm_th=0.03)
_A47 = _partial(_sig_fm_long, short_n=3, long_n=6,  fm_th=0.05)
# OFD LONG (Batch3)
_OFD_L1 = _partial(_sig_ofd_long, n=5, ofd_th=0.15)
_OFD_L2 = _partial(_sig_ofd_long, n=8, ofd_th=0.10)
# VolMom LONG (Batch3)
_VM1  = _partial(_sig_volmom_long, n=5, lag=2, vm_th=0.40)
_VM2  = _partial(_sig_volmom_long, n=5, lag=3, vm_th=0.40)
_VM3  = _partial(_sig_volmom_long, n=5, lag=4, vm_th=0.40)
_VM4  = _partial(_sig_volmom_long, n=3, lag=2, vm_th=0.40)
_VM5  = _partial(_sig_volmom_long, n=3, lag=3, vm_th=0.40)
_VM6  = _partial(_sig_volmom_long, n=3, lag=4, vm_th=0.40)
_VM7  = _partial(_sig_volmom_long, n=5, lag=2, vm_th=0.20)
_VM8  = _partial(_sig_volmom_long, n=5, lag=3, vm_th=0.20)
_VM9  = _partial(_sig_volmom_long, n=5, lag=4, vm_th=0.20)
_VM10 = _partial(_sig_volmom_long, n=3, lag=2, vm_th=0.20)
_VM11 = _partial(_sig_volmom_long, n=3, lag=3, vm_th=0.20)
_VM12 = _partial(_sig_volmom_long, n=3, lag=4, vm_th=0.20)
# CloseConsistency LONG (Batch3 CC)
_CC_L1 = _partial(_sig_cc_long, n=12, cc_lo=0.20)
_CC_L2 = _partial(_sig_cc_long, n=10, cc_lo=0.25)
_CC_L3 = _partial(_sig_cc_long, n=12, cc_lo=0.30)
_CC_L4 = _partial(_sig_cc_long, n=8,  cc_lo=0.30)
# PriceVelocityExhaustion LONG (Batch5)
_PV_L1 = _partial(_sig_pvel_long, n=3, amp_n=10, pv_th=2.0)
_PV_L2 = _partial(_sig_pvel_long, n=3, amp_n=6,  pv_th=2.0)
_PV_L3 = _partial(_sig_pvel_long, n=5, amp_n=10, pv_th=1.5)
_PV_L4 = _partial(_sig_pvel_long, n=3, amp_n=10, pv_th=1.5)
_PV_L5 = _partial(_sig_pvel_long, n=5, amp_n=10, pv_th=2.0)
_PV_L6 = _partial(_sig_pvel_long, n=3, amp_n=6,  pv_th=1.5)
_PV_L7 = _partial(_sig_pvel_long, n=5, amp_n=6,  pv_th=1.5)

_DB_CFG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 3306)),
    "user":     os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "binance-data"),
    "charset":  "utf8mb4",
}

DEFAULT_START  = "2024-07-01"
DEFAULT_END    = "2025-03-31"
HORIZONS       = [1, 2, 3, 4, 6, 8, 12]
LOOKBACK_1H    = 30
LOOKBACK_4H    = 15

# ── 策略注册表 ─────────────────────────────────────────────────────────────────
# dir: LONG / SHORT / BOTH
# mode: self4h = 用标的自身4h数据, btc4h = 用 BTC 4h 数据
# test_wr: 走时测试胜率

ALL_STRATEGIES = {
    # ── SHORT E1-E15 ──────────────────────────────────────────────────────────
    "E1":  {"fn": sig_E1,  "dir": "SHORT", "mode": "self4h",
            "name": "E1-DecelFluxShort",           "test_wr": 62.3},
    "E2":  {"fn": sig_E2,  "dir": "BOTH",  "mode": "self4h",
            "name": "E2-MTFDivergentExhaust",       "test_wr": 62.0},
    "E4":  {"fn": sig_E4,  "dir": "SHORT", "mode": "self4h",
            "name": "E4-KineticEfficiencyShort",    "test_wr": 60.7},
    "E5":  {"fn": sig_E5,  "dir": "SHORT", "mode": "self4h",
            "name": "E5-TripleFluxEntropy",         "test_wr": 65.0},
    "E6":  {"fn": sig_E6,  "dir": "SHORT", "mode": "self4h",
            "name": "E6-InertialFrictionShort",     "test_wr": 63.1},
    "E7":  {"fn": sig_E7,  "dir": "SHORT", "mode": "btc4h",
            "name": "E7-PassiveSupplySuffoc",       "test_wr": 62.0},
    "E8":  {"fn": sig_E8,  "dir": "SHORT", "mode": "self4h",
            "name": "E8-HollowGrowthShort",         "test_wr": 60.9},
    "E9":  {"fn": sig_E9,  "dir": "SHORT", "mode": "self4h",
            "name": "E9-WickSaturationShort",       "test_wr": 62.9},
    "E10": {"fn": sig_E10, "dir": "SHORT", "mode": "self4h",
            "name": "E10-FrictionDecouplingShort",  "test_wr": 61.5},
    "E11": {"fn": sig_E11, "dir": "SHORT", "mode": "btc4h",
            "name": "E11-ShadowLaggardShort",       "test_wr": 61.1},
    "E12": {"fn": sig_E12, "dir": "SHORT", "mode": "self4h",
            "name": "E12-NonLinearExhaustion",      "test_wr": 63.3},
    "E13": {"fn": sig_E13, "dir": "SHORT", "mode": "self4h",
            "name": "E13-ConvexityTrapShort",       "test_wr": 61.4},
    "E14": {"fn": sig_E14, "dir": "SHORT", "mode": "self4h",
            "name": "E14-InertialDragShort",        "test_wr": 62.0},
    "E15": {"fn": sig_E15, "dir": "SHORT", "mode": "btc4h",
            "name": "E15-ElasticityCollapse",       "test_wr": 62.1},
    # ── LONG E3 (BTC-lead) ───────────────────────────────────────────────────
    "E3":  {"fn": sig_E3,  "dir": "LONG",  "mode": "btc4h",
            "name": "E3-AltDipRecovery",            "test_wr": 57.0},
    # ── LONG DecelBounce E16-E30 ─────────────────────────────────────────────
    "E16": {"fn": sig_E16, "dir": "LONG",  "mode": "self4h",
            "name": "E16-DecelBounce",              "test_wr": 58.3},
    "E17": {"fn": sig_E17, "dir": "LONG",  "mode": "self4h",
            "name": "E17-DecelBounce-Deep",         "test_wr": 63.9},
    "E18": {"fn": sig_E18, "dir": "LONG",  "mode": "self4h",
            "name": "E18-DecelBounce-LowAmp",       "test_wr": 59.3},
    "E19": {"fn": sig_E19, "dir": "LONG",  "mode": "self4h",
            "name": "E19-DB-h4",                    "test_wr": 59.6},
    "E20": {"fn": sig_E20, "dir": "LONG",  "mode": "self4h",
            "name": "E20-DB-h5",                    "test_wr": 58.3},
    "E21": {"fn": sig_E21, "dir": "LONG",  "mode": "self4h",
            "name": "E21-DB-h7",                    "test_wr": 64.1},
    "E22": {"fn": sig_E22, "dir": "LONG",  "mode": "self4h",
            "name": "E22-DB-h10",                   "test_wr": 63.1},
    "E23": {"fn": sig_E23, "dir": "LONG",  "mode": "self4h",
            "name": "E23-DB-AmpMed",                "test_wr": 59.0},
    "E24": {"fn": sig_E24, "dir": "LONG",  "mode": "self4h",
            "name": "E24-DB-DeepLowAmp",            "test_wr": 64.1},
    "E25": {"fn": sig_E25, "dir": "LONG",  "mode": "self4h",
            "name": "E25-DB-AmpMacro",              "test_wr": 59.1},
    "E26": {"fn": sig_E26, "dir": "LONG",  "mode": "self4h",
            "name": "E26-DB-3macroN",               "test_wr": 62.4},
    "E27": {"fn": sig_E27, "dir": "LONG",  "mode": "self4h",
            "name": "E27-DB-h3",                    "test_wr": 57.1},
    "E28": {"fn": sig_E28, "dir": "LONG",  "mode": "self4h",
            "name": "E28-DB-h12",                   "test_wr": 67.7},
    "E29": {"fn": sig_E29, "dir": "LONG",  "mode": "self4h",
            "name": "E29-DB-h14",                   "test_wr": 69.0},
    "E30": {"fn": sig_E30, "dir": "LONG",  "mode": "self4h",
            "name": "E30-DB-h7-DeepDecline",        "test_wr": 65.3},
    # ── LONG auto-deployed E31-E98 （全量）─────────────────────────────────
    "E31": {"fn": sig_E31, "dir": "LONG",  "mode": "self4h", "name": "sig_E31-DB_h16",                 "test_wr": 63.9},
    "E32": {"fn": sig_E32, "dir": "LONG",  "mode": "self4h", "name": "sig_E32-DB_h14_ht3",             "test_wr": 63.8},
    "E33": {"fn": sig_E33, "dir": "LONG",  "mode": "self4h", "name": "sig_E33-DB_h14_f55",             "test_wr": 63.7},
    "E34": {"fn": sig_E34, "dir": "LONG",  "mode": "self4h", "name": "sig_E34-DB_h15",                 "test_wr": 63.6},
    "E35": {"fn": sig_E35, "dir": "LONG",  "mode": "self4h", "name": "sig_E35-DB_h14_ht4",             "test_wr": 63.4},
    "E36": {"fn": sig_E36, "dir": "LONG",  "mode": "self4h", "name": "sig_E36-DB_h8_f56",              "test_wr": 63.3},
    "E37": {"fn": sig_E37, "dir": "LONG",  "mode": "self4h", "name": "sig_E37-DB_h14_ht5",             "test_wr": 62.9},
    "E38": {"fn": sig_E38, "dir": "LONG",  "mode": "self4h", "name": "sig_E38-DB_h14_f56",             "test_wr": 62.6},
    "E39": {"fn": sig_E39, "dir": "LONG",  "mode": "self4h", "name": "sig_E39-DB_h12_ht4",             "test_wr": 62.6},
    "E40": {"fn": sig_E40, "dir": "LONG",  "mode": "btc4h",  "name": "sig_E40-BTCLead_b10_h8_f50",     "test_wr": 60.0},
    "E41": {"fn": sig_E41, "dir": "LONG",  "mode": "self4h", "name": "sig_E41-DB_h14_ht6",             "test_wr": 62.3},
    "E42": {"fn": sig_E42, "dir": "LONG",  "mode": "self4h", "name": "sig_E42-DB_h12_ht3",             "test_wr": 62.2},
    "E43": {"fn": sig_E43, "dir": "LONG",  "mode": "self4h", "name": "sig_E43-DB_h12_ht5",             "test_wr": 62.2},
    "E44": {"fn": sig_E44, "dir": "LONG",  "mode": "self4h", "name": "sig_E44-DB_h12_ht6",             "test_wr": 61.9},
    "E45": {"fn": sig_E45, "dir": "LONG",  "mode": "self4h", "name": "sig_E45-DB_h10_f56",             "test_wr": 61.6},
    "E46": {"fn": sig_E46, "dir": "LONG",  "mode": "self4h", "name": "sig_E46-OvrSold_h10_d12_f55",    "test_wr": 61.5},
    "E47": {"fn": sig_E47, "dir": "LONG",  "mode": "btc4h",  "name": "sig_E47-BTCLead_b7_h8_f50",      "test_wr": 60.0},
    "E48": {"fn": sig_E48, "dir": "LONG",  "mode": "self4h", "name": "sig_E48-DB_h8_f55",              "test_wr": 61.4},
    "E49": {"fn": sig_E49, "dir": "LONG",  "mode": "self4h", "name": "sig_E49-DB_h20",                 "test_wr": 61.4},
    "E50": {"fn": sig_E50, "dir": "LONG",  "mode": "self4h", "name": "sig_E50-OvrSold_h10_d20_f53",    "test_wr": 61.3},
    "E51": {"fn": sig_E51, "dir": "LONG",  "mode": "self4h", "name": "sig_E51-OvrSold_h10_d10_f55",    "test_wr": 61.2},
    "E52": {"fn": sig_E52, "dir": "LONG",  "mode": "self4h", "name": "sig_E52-FluxAccel_h10_mac5_f51", "test_wr": 61.2},
    "E53": {"fn": sig_E53, "dir": "LONG",  "mode": "self4h", "name": "sig_E53-OvrSold_h10_d15_f55",    "test_wr": 61.2},
    "E54": {"fn": sig_E54, "dir": "LONG",  "mode": "self4h", "name": "sig_E54-FluxAccel_h10_mac5_f49", "test_wr": 61.2},
    "E55": {"fn": sig_E55, "dir": "LONG",  "mode": "self4h", "name": "sig_E55-OvrSold_h10_d8_f55",     "test_wr": 61.0},
    "E56": {"fn": sig_E56, "dir": "LONG",  "mode": "self4h", "name": "sig_E56-DB_h12_f55",             "test_wr": 61.2},
    "E57": {"fn": sig_E57, "dir": "LONG",  "mode": "self4h", "name": "sig_E57-FluxAccel_h10_mac3_f49", "test_wr": 61.0},
    "E58": {"fn": sig_E58, "dir": "LONG",  "mode": "self4h", "name": "sig_E58-DB_h8_ht6",              "test_wr": 61.0},
    "E59": {"fn": sig_E59, "dir": "LONG",  "mode": "self4h", "name": "sig_E59-DB_h10_ht4",             "test_wr": 61.1},
    "E60": {"fn": sig_E60, "dir": "LONG",  "mode": "self4h", "name": "sig_E60-FluxAccel_h10_mac3_f51", "test_wr": 60.9},
    "E61": {"fn": sig_E61, "dir": "LONG",  "mode": "self4h", "name": "sig_E61-FluxAccel_h10_mac2_f49", "test_wr": 60.8},
    "E62": {"fn": sig_E62, "dir": "LONG",  "mode": "self4h", "name": "sig_E62-DB_h8_ht5",              "test_wr": 60.8},
    "E63": {"fn": sig_E63, "dir": "LONG",  "mode": "self4h", "name": "sig_E63-OvrSold_h10_d15_f53",    "test_wr": 60.7},
    "E64": {"fn": sig_E64, "dir": "LONG",  "mode": "self4h", "name": "sig_E64-FluxAccel_h10_mac2_f51", "test_wr": 60.7},
    "E65": {"fn": sig_E65, "dir": "LONG",  "mode": "self4h", "name": "sig_E65-DB_h8_ht3",              "test_wr": 60.6},
    "E66": {"fn": sig_E66, "dir": "LONG",  "mode": "self4h", "name": "sig_E66-OvrSold_h10_d12_f53",    "test_wr": 60.6},
    "E67": {"fn": sig_E67, "dir": "LONG",  "mode": "self4h", "name": "sig_E67-DB_h18",                 "test_wr": 60.5},
    "E68": {"fn": sig_E68, "dir": "LONG",  "mode": "self4h", "name": "sig_E68-DB_h10_ht5",             "test_wr": 60.4},
    "E69": {"fn": sig_E69, "dir": "LONG",  "mode": "self4h", "name": "sig_E69-DB_h10_ht3",             "test_wr": 60.3},
    "E70": {"fn": sig_E70, "dir": "LONG",  "mode": "self4h", "name": "sig_E70-OvrSold_h10_d8_f53",     "test_wr": 60.3},
    "E71": {"fn": sig_E71, "dir": "LONG",  "mode": "self4h", "name": "sig_E71-DB_h8_ht4",              "test_wr": 60.2},
    "E72": {"fn": sig_E72, "dir": "LONG",  "mode": "self4h", "name": "sig_E72-DB_h10_ht6",             "test_wr": 60.2},
    "E73": {"fn": sig_E73, "dir": "LONG",  "mode": "self4h", "name": "sig_E73-FluxAccel_h10_mac5_f53", "test_wr": 60.1},
    "E74": {"fn": sig_E74, "dir": "LONG",  "mode": "self4h", "name": "sig_E74-OvrSold_h10_d10_f53",    "test_wr": 60.0},
    "E75": {"fn": sig_E75, "dir": "LONG",  "mode": "self4h", "name": "sig_E75-FluxAccel_h8_mac5_f51",  "test_wr": 60.0},
    "E76": {"fn": sig_E76, "dir": "LONG",  "mode": "self4h", "name": "sig_E76-DB_h10_f55",             "test_wr": 60.1},
    "E77": {"fn": sig_E77, "dir": "LONG",  "mode": "self4h", "name": "sig_E77-DB_h12_f56",             "test_wr": 60.0},
    "E78": {"fn": sig_E78, "dir": "LONG",  "mode": "self4h", "name": "sig_E78-FluxAccel_h8_mac3_f51",  "test_wr": 60.0},
    "E79": {"fn": sig_E79, "dir": "LONG",  "mode": "self4h", "name": "sig_E79-FluxAccel_h8_mac5_f49",  "test_wr": 60.0},
    "E80": {"fn": sig_E80, "dir": "LONG",  "mode": "self4h", "name": "sig_E80-FluxAccel_h8_mac2_f51",  "test_wr": 60.0},
    "E81": {"fn": sig_E81, "dir": "LONG",  "mode": "self4h", "name": "sig_E81-FluxAccel_h10_mac3_f53", "test_wr": 59.8},
    "E82": {"fn": sig_E82, "dir": "LONG",  "mode": "self4h", "name": "sig_E82-FluxAccel_h8_mac3_f49",  "test_wr": 59.6},
    "E83": {"fn": sig_E83, "dir": "LONG",  "mode": "self4h", "name": "sig_E83-FluxAccel_h8_mac2_f49",  "test_wr": 59.4},
    "E84": {"fn": sig_E84, "dir": "LONG",  "mode": "self4h", "name": "sig_E84-OvrSold_h8_d12_f55",     "test_wr": 59.5},
    "E85": {"fn": sig_E85, "dir": "LONG",  "mode": "self4h", "name": "sig_E85-FluxAccel_h10_mac2_f53", "test_wr": 59.4},
    "E86": {"fn": sig_E86, "dir": "LONG",  "mode": "self4h", "name": "sig_E86-FluxAccel_h8_mac3_f53",  "test_wr": 59.2},
    "E87": {"fn": sig_E87, "dir": "LONG",  "mode": "self4h", "name": "sig_E87-FluxAccel_h8_mac5_f53",  "test_wr": 59.0},
    "E88": {"fn": sig_E88, "dir": "LONG",  "mode": "self4h", "name": "sig_E88-FluxAccel_h8_mac2_f53",  "test_wr": 58.8},
    "E89": {"fn": sig_E89, "dir": "LONG",  "mode": "btc4h",  "name": "sig_E89-BTCLead_b10_h6_f50",     "test_wr": 60.0},
    "E90": {"fn": sig_E90, "dir": "LONG",  "mode": "self4h", "name": "sig_E90-DB_h25",                 "test_wr": 58.7},
    "E91": {"fn": sig_E91, "dir": "LONG",  "mode": "self4h", "name": "sig_E91-FluxAccel_h6_mac5_f49",  "test_wr": 58.5},
    "E92": {"fn": sig_E92, "dir": "LONG",  "mode": "self4h", "name": "sig_E92-FluxAccel_h6_mac3_f49",  "test_wr": 58.3},
    "E93": {"fn": sig_E93, "dir": "LONG",  "mode": "self4h", "name": "sig_E93-FluxAccel_h6_mac2_f49",  "test_wr": 58.0},
    "E94": {"fn": sig_E94, "dir": "LONG",  "mode": "self4h", "name": "sig_E94-FluxAccel_h6_mac5_f53",  "test_wr": 57.8},
    "E95": {"fn": sig_E95, "dir": "LONG",  "mode": "self4h", "name": "sig_E95-FluxAccel_h6_mac3_f51",  "test_wr": 57.6},
    "E96": {"fn": sig_E96, "dir": "LONG",  "mode": "self4h", "name": "sig_E96-FluxAccel_h6_mac5_f51",  "test_wr": 57.4},
    "E97": {"fn": sig_E97, "dir": "LONG",  "mode": "self4h", "name": "sig_E97-FluxAccel_h6_mac2_f51",  "test_wr": 57.2},
    "E98": {"fn": sig_E98, "dir": "LONG",  "mode": "self4h", "name": "sig_E98-FluxAccel_h4_mac5_f49",  "test_wr": 57.2},
    # ── Alien Batch1 A1-A9 ──────────────────────────────────────────────────────
    "A1":  {"fn": sig_A1, "dir": "LONG",  "mode": "self4h", "name": "A1-SellCapLong",           "test_wr": 59.5},
    "A2":  {"fn": sig_A2, "dir": "SHORT", "mode": "self4h", "name": "A2-BuyExhShort",            "test_wr": 58.8},
    "A3":  {"fn": sig_A3, "dir": "SHORT", "mode": "self4h", "name": "A3-MomDecay-l10-r50",       "test_wr": 61.7},
    "A4":  {"fn": sig_A4, "dir": "SHORT", "mode": "self4h", "name": "A4-MomDecay-l10-r40",       "test_wr": 61.6},
    "A5":  {"fn": sig_A5, "dir": "SHORT", "mode": "self4h", "name": "A5-MomDecay-l10-r30",       "test_wr": 61.4},
    "A6":  {"fn": sig_A6, "dir": "SHORT", "mode": "self4h", "name": "A6-MomDecay-l8-r50",        "test_wr": 59.4},
    "A7":  {"fn": sig_A7, "dir": "SHORT", "mode": "self4h", "name": "A7-MomDecay-l8-r40",        "test_wr": 59.3},
    "A8":  {"fn": sig_A8, "dir": "SHORT", "mode": "self4h", "name": "A8-MomDecay-l8-r30",        "test_wr": 59.0},
    "A9":  {"fn": sig_A9, "dir": "LONG",  "mode": "self4h", "name": "A9-SpatDivLong",            "test_wr": 57.1},
    # ── PriceMemory SHORT (A10-A24) ──────────────────────────────────────────────
    "A10": {"fn": _A10, "dir": "SHORT", "mode": "self4h", "name": "A10-PriceMem-S-n20-hi75",  "test_wr": 77.1},
    "A11": {"fn": _A11, "dir": "SHORT", "mode": "self4h", "name": "A11-PriceMem-S-n20-hi80",  "test_wr": 75.9},
    "A12": {"fn": _A12, "dir": "SHORT", "mode": "self4h", "name": "A12-PriceMem-S-n20-hi85",  "test_wr": 73.3},
    "A13": {"fn": _A13, "dir": "SHORT", "mode": "self4h", "name": "A13-PriceMem-S-n14-hi75",  "test_wr": 70.2},
    "A16": {"fn": _A16, "dir": "SHORT", "mode": "self4h", "name": "A16-PriceMem-S-n14-hi80",  "test_wr": 68.4},
    "A21": {"fn": _A21, "dir": "SHORT", "mode": "self4h", "name": "A21-PriceMem-S-n10-hi85",  "test_wr": 66.5},
    "A22": {"fn": _A22, "dir": "SHORT", "mode": "self4h", "name": "A22-PriceMem-S-n10-hi75",  "test_wr": 64.9},
    "A23": {"fn": _A23, "dir": "SHORT", "mode": "self4h", "name": "A23-PriceMem-S-n10-hi80",  "test_wr": 64.8},
    "A24": {"fn": _A24, "dir": "SHORT", "mode": "self4h", "name": "A24-PriceMem-S-n14-hi85",  "test_wr": 64.4},
    # ── PriceMemory LONG (A14-A29) ───────────────────────────────────────────────
    "A14": {"fn": _A14, "dir": "LONG",  "mode": "self4h", "name": "A14-PriceMem-L-n20-lo15",  "test_wr": 69.5},
    "A15": {"fn": _A15, "dir": "LONG",  "mode": "self4h", "name": "A15-PriceMem-L-n20-lo25",  "test_wr": 68.6},
    "A17": {"fn": _A17, "dir": "LONG",  "mode": "self4h", "name": "A17-PriceMem-L-n20-lo20",  "test_wr": 68.2},
    "A18": {"fn": _A18, "dir": "LONG",  "mode": "self4h", "name": "A18-PriceMem-L-n14-lo25",  "test_wr": 67.2},
    "A19": {"fn": _A19, "dir": "LONG",  "mode": "self4h", "name": "A19-PriceMem-L-n14-lo20",  "test_wr": 67.0},
    "A20": {"fn": _A20, "dir": "LONG",  "mode": "self4h", "name": "A20-PriceMem-L-n14-lo15",  "test_wr": 66.7},
    "A26": {"fn": _A26, "dir": "LONG",  "mode": "self4h", "name": "A26-PriceMem-L-n10-lo25",  "test_wr": 63.0},
    "A27": {"fn": _A27, "dir": "LONG",  "mode": "self4h", "name": "A27-PriceMem-L-n10-lo15",  "test_wr": 62.8},
    "A29": {"fn": _A29, "dir": "LONG",  "mode": "self4h", "name": "A29-PriceMem-L-n10-lo20",  "test_wr": 62.5},
    # ── SatVelocity SHORT ────────────────────────────────────────────────────────
    "A25": {"fn": _A25, "dir": "SHORT", "mode": "self4h", "name": "A25-SatVel-S-n3-l3-v6",    "test_wr": 63.9},
    "A31": {"fn": _A31, "dir": "SHORT", "mode": "self4h", "name": "A31-SatVel-S-n3-l2-v4",    "test_wr": 61.4},
    "A34": {"fn": _A34, "dir": "SHORT", "mode": "self4h", "name": "A34-SatVel-S-n3-l4-v6",    "test_wr": 58.9},
    "A36": {"fn": _A36, "dir": "SHORT", "mode": "self4h", "name": "A36-SatVel-S-n4-l4-v6",    "test_wr": 58.6},
    "A37": {"fn": _A37, "dir": "SHORT", "mode": "self4h", "name": "A37-SatVel-S-n4-l2-v4",    "test_wr": 58.6},
    "A48": {"fn": _A48, "dir": "SHORT", "mode": "self4h", "name": "A48-SatVel-S-n4-l4-v4",    "test_wr": 57.6},
    # ── TimePressure SHORT ───────────────────────────────────────────────────────
    "A35": {"fn": _A35, "dir": "SHORT", "mode": "self4h", "name": "A35-TimePres-S-n8-a6-t75",  "test_wr": 58.8},
    "A38": {"fn": _A38, "dir": "SHORT", "mode": "self4h", "name": "A38-TimePres-S-n10-a6-t75", "test_wr": 58.4},
    "A45": {"fn": _A45, "dir": "SHORT", "mode": "self4h", "name": "A45-TimePres-S-n10-a6-t65", "test_wr": 57.8},
    # ── FluxMomentum SHORT ───────────────────────────────────────────────────────
    "A30": {"fn": _A30, "dir": "SHORT", "mode": "self4h", "name": "A30-FluxMom-S-s3-l6-f3",   "test_wr": 62.2},
    "A33": {"fn": _A33, "dir": "SHORT", "mode": "self4h", "name": "A33-FluxMom-S-s2-l8-f4",   "test_wr": 59.4},
    "A40": {"fn": _A40, "dir": "SHORT", "mode": "self4h", "name": "A40-FluxMom-S-s2-l12-f5",  "test_wr": 58.2},
    # ── FluxMomentum LONG ────────────────────────────────────────────────────────
    "A28": {"fn": _A28, "dir": "LONG",  "mode": "self4h", "name": "A28-FluxMom-L-s3-l12-f5",  "test_wr": 62.6},
    "A32": {"fn": _A32, "dir": "LONG",  "mode": "self4h", "name": "A32-FluxMom-L-s3-l12-f4",  "test_wr": 61.1},
    "A39": {"fn": _A39, "dir": "LONG",  "mode": "self4h", "name": "A39-FluxMom-L-s2-l8-f4",   "test_wr": 58.3},
    "A41": {"fn": _A41, "dir": "LONG",  "mode": "self4h", "name": "A41-FluxMom-L-s2-l6-f3",   "test_wr": 58.2},
    "A42": {"fn": _A42, "dir": "LONG",  "mode": "self4h", "name": "A42-FluxMom-L-s2-l12-f5",  "test_wr": 58.1},
    "A43": {"fn": _A43, "dir": "LONG",  "mode": "self4h", "name": "A43-FluxMom-L-s2-l6-f4",   "test_wr": 58.0},
    "A44": {"fn": _A44, "dir": "LONG",  "mode": "self4h", "name": "A44-FluxMom-L-s2-l12-f3",  "test_wr": 57.9},
    "A46": {"fn": _A46, "dir": "LONG",  "mode": "self4h", "name": "A46-FluxMom-L-s2-l8-f3",   "test_wr": 57.6},
    "A47": {"fn": _A47, "dir": "LONG",  "mode": "self4h", "name": "A47-FluxMom-L-s3-l6-f5",   "test_wr": 57.6},
    # ── OFD LONG (Batch3) ────────────────────────────────────────────────────────
    "OFD_L1": {"fn": _OFD_L1, "dir": "LONG", "mode": "self4h", "name": "OFD_L_n5_t15",  "test_wr": 57.2},
    "OFD_L2": {"fn": _OFD_L2, "dir": "LONG", "mode": "self4h", "name": "OFD_L_n8_t10",  "test_wr": 59.2},
    # ── VolMom LONG (Batch3) ─────────────────────────────────────────────────────
    "VM1":  {"fn": _VM1,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n5_l2_v40", "test_wr": 63.9},
    "VM2":  {"fn": _VM2,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n5_l3_v40", "test_wr": 62.9},
    "VM3":  {"fn": _VM3,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n5_l4_v40", "test_wr": 62.1},
    "VM4":  {"fn": _VM4,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n3_l2_v40", "test_wr": 60.3},
    "VM5":  {"fn": _VM5,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n3_l3_v40", "test_wr": 60.3},
    "VM6":  {"fn": _VM6,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n3_l4_v40", "test_wr": 60.3},
    "VM7":  {"fn": _VM7,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n5_l2_v20", "test_wr": 62.5},
    "VM8":  {"fn": _VM8,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n5_l3_v20", "test_wr": 61.8},
    "VM9":  {"fn": _VM9,  "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n5_l4_v20", "test_wr": 60.8},
    "VM10": {"fn": _VM10, "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n3_l2_v20", "test_wr": 60.5},
    "VM11": {"fn": _VM11, "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n3_l3_v20", "test_wr": 59.5},
    "VM12": {"fn": _VM12, "dir": "LONG", "mode": "self4h", "name": "VolMom_L_n3_l4_v20", "test_wr": 59.2},
    # ── CloseConsistency LONG (Batch4 CC) ────────────────────────────────────────
    "CC_L1": {"fn": _CC_L1, "dir": "LONG", "mode": "self4h", "name": "CC_L_n12_lo20",  "test_wr": 63.2},
    "CC_L2": {"fn": _CC_L2, "dir": "LONG", "mode": "self4h", "name": "CC_L_n10_lo25",  "test_wr": 60.2},
    "CC_L3": {"fn": _CC_L3, "dir": "LONG", "mode": "self4h", "name": "CC_L_n12_lo30",  "test_wr": 59.7},
    "CC_L4": {"fn": _CC_L4, "dir": "LONG", "mode": "self4h", "name": "CC_L_n8_lo30",   "test_wr": 58.1},
    # ── PriceVelocityExhaustion LONG (Batch5 PVel) ───────────────────────────────
    "PV_L1": {"fn": _PV_L1, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n3_a10_v20", "test_wr": 60.9},
    "PV_L2": {"fn": _PV_L2, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n3_a6_v20",  "test_wr": 60.7},
    "PV_L3": {"fn": _PV_L3, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n5_a10_v15", "test_wr": 59.2},
    "PV_L4": {"fn": _PV_L4, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n3_a10_v15", "test_wr": 59.1},
    "PV_L5": {"fn": _PV_L5, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n5_a10_v20", "test_wr": 58.8},
    "PV_L6": {"fn": _PV_L6, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n3_a6_v15",  "test_wr": 58.7},
    "PV_L7": {"fn": _PV_L7, "dir": "LONG", "mode": "self4h", "name": "PVel_L_n5_a6_v15",  "test_wr": 57.8},
}

# ── 分批计划（每批 5 个，按优先级排序）───────────────────────────────────────
BATCHES = {
    # ── 已完成批次（1-8）────────────────────────────────────────────────────
    1:  ["E2",  "E7",  "E16", "E18", "E21"],   # SHORT+DB中等
    2:  ["E19", "E20", "E22", "E23", "E24"],   # DB浅/中
    3:  ["E25", "E26", "E27", "E30", "E3"],    # DB变体+AltDip
    4:  ["E31", "E34", "E36", "E49", "E90"],   # auto DB deep
    5:  ["E46", "E52", "E40", "E47", "E89"],   # OvrSold+FluxAccel+BTCLead
    6:  ["E4",  "E6",  "E8",  "E9",  "E10"],  # SHORT E4-E10
    7:  ["E11", "E12", "E13", "E14", "E15"],  # SHORT E11-E15
    8:  ["E1",  "E5",  "E17", "E28", "E29"],  # DEFAULT trial（补录）
    # ── 待跑批次（9-20）：E31-E98 全量 ──────────────────────────────────────
    9:  ["E32", "E33", "E35", "E37", "E38"],  # DB_h14 变体
    10: ["E39", "E41", "E42", "E43", "E44"],  # DB_h12 变体
    11: ["E45", "E48", "E56", "E58", "E62"],  # DB_h8-h12 + filter
    12: ["E59", "E65", "E67", "E68", "E69"],  # DB more
    13: ["E71", "E72", "E76", "E77", "E73"],  # DB variants + FluxAccel start
    14: ["E50", "E51", "E53", "E55", "E63"],  # OvrSold family
    15: ["E66", "E70", "E74", "E84", "E64"],  # OvrSold + FluxAccel
    16: ["E54", "E57", "E60", "E61", "E58"],  # FluxAccel + DB_h8_ht6
    17: ["E75", "E78", "E79", "E80", "E71"],  # FluxAccel h8 + DB_h8_ht4
    18: ["E81", "E82", "E83", "E85", "E86"],  # FluxAccel continued
    19: ["E87", "E88", "E91", "E92", "E93"],  # FluxAccel h6
    20: ["E94", "E95", "E96", "E97", "E98"],  # FluxAccel h6/h4
    # ── Alien Batch1 A1-A9 ──────────────────────────────────────────────────────
    21: ["A1", "A2", "A3", "A4", "A5"],
    22: ["A6", "A7", "A8", "A9"],
    # ── PriceMemory ─────────────────────────────────────────────────────────────
    23: ["A10", "A11", "A12", "A13", "A14"],
    24: ["A15", "A16", "A17", "A18", "A19"],
    25: ["A20", "A21", "A22", "A23", "A24"],
    26: ["A25", "A26", "A27", "A28", "A29"],
    # ── SatVel + TimePres + FluxMom ─────────────────────────────────────────────
    27: ["A30", "A31", "A32", "A33", "A34"],
    28: ["A35", "A36", "A37", "A38", "A39"],
    29: ["A40", "A41", "A42", "A43", "A44"],
    30: ["A45", "A46", "A47", "A48"],
    # ── Batch3 OFD + VolMom ─────────────────────────────────────────────────────
    31: ["OFD_L1", "OFD_L2", "VM1", "VM2", "VM3"],
    32: ["VM4", "VM5", "VM6", "VM7", "VM8"],
    33: ["VM9", "VM10", "VM11", "VM12"],
    # ── Batch3 CC ───────────────────────────────────────────────────────────────
    34: ["CC_L1", "CC_L2", "CC_L3", "CC_L4"],
    # ── Batch5 PriceVelocityExhaustion ──────────────────────────────────────────
    35: ["PV_L1", "PV_L2", "PV_L3", "PV_L4", "PV_L5", "PV_L6", "PV_L7"],
}

DEFAULT_STRATS = ["E1", "E5", "E17", "E28", "E29"]

ANALYSIS_SYMBOLS = [
    "ETH/USDT","BNB/USDT","SOL/USDT","XRP/USDT","DOGE/USDT",
    "AVAX/USDT","LINK/USDT","DOT/USDT","NEAR/USDT","AAVE/USDT",
    "LTC/USDT","BCH/USDT","UNI/USDT","ARB/USDT","OP/USDT",
    "INJ/USDT","APT/USDT","SUI/USDT","ICP/USDT","ADA/USDT",
    "TRX/USDT","ATOM/USDT","FIL/USDT","SEI/USDT","HBAR/USDT",
]

# ── 数据加载 ───────────────────────────────────────────────────────────────────

def load_candles_range(conn, symbol: str, timeframe: str,
                        start: str, end: str) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT timestamp, open_price, high_price, low_price, close_price,
                   volume, taker_buy_base_volume
            FROM kline_data
            WHERE symbol=%s AND timeframe=%s
              AND taker_buy_base_volume IS NOT NULL AND volume > 0
              AND timestamp >= %s AND timestamp < DATE_ADD(%s, INTERVAL 1 DAY)
            ORDER BY timestamp ASC
        """, (symbol, timeframe, start, end))
        rows = cur.fetchall()
    return [{"t": r[0], "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]),
             "vol": float(r[5]), "buy_vol": float(r[6])}
            for r in rows]


def align_4h(cs4h_all: list, ts_1h) -> list:
    lo, hi = 0, len(cs4h_all)
    while lo < hi:
        mid = (lo + hi) // 2
        if cs4h_all[mid]["t"] <= ts_1h:
            lo = mid + 1
        else:
            hi = mid
    idx = lo
    return cs4h_all[max(0, idx - LOOKBACK_4H): idx]


# ── 分析核心 ───────────────────────────────────────────────────────────────────

class SignalRecord:
    __slots__ = ("symbol", "ts", "direction", "entry_close", "entry_next_open",
                 "fwd_closes", "fwd_highs", "fwd_lows", "hour_of_day", "amp_entry")
    def __init__(self, symbol, ts, direction, entry_close, entry_next_open,
                 fwd_closes, fwd_highs, fwd_lows, hour_of_day, amp_entry):
        self.symbol        = symbol
        self.ts            = ts
        self.direction     = direction
        self.entry_close   = entry_close
        self.entry_next_open = entry_next_open
        self.fwd_closes    = fwd_closes
        self.fwd_highs     = fwd_highs
        self.fwd_lows      = fwd_lows
        self.hour_of_day   = hour_of_day
        self.amp_entry     = amp_entry


def scan_strategy(strat_key: str, strat: dict,
                  conn, start: str, end: str,
                  btc4h_all: list) -> list[SignalRecord]:
    records = []
    fn      = strat["fn"]
    mode    = strat["mode"]
    base_dir = strat["dir"]

    for sym in ANALYSIS_SYMBOLS:
        cs1h_all = load_candles_range(conn, sym, "1h", start, end)
        if mode == "self4h":
            cs4h_all = load_candles_range(conn, sym, "4h", start, end)
        else:
            cs4h_all = btc4h_all

        if len(cs1h_all) < LOOKBACK_1H + 12:
            continue

        n = len(cs1h_all)
        for i in range(LOOKBACK_1H, n):
            cs1h = cs1h_all[max(0, i - LOOKBACK_1H + 1): i + 1]
            cs4h = align_4h(cs4h_all, cs1h_all[i]["t"])

            if len(cs4h) < 5:
                continue

            result = fn(cs1h, cs4h)

            if base_dir == "BOTH":
                if result not in ("LONG", "SHORT"):
                    continue
                direction = result
            else:
                if not result:
                    continue
                direction = base_dir

            entry_close = cs1h[-1]["close"]
            entry_next_open = cs1h_all[i + 1]["open"] if i + 1 < n else entry_close

            fwd_closes = []
            fwd_highs  = []
            fwd_lows   = []
            for h in range(1, 13):
                j = i + h
                if j < n:
                    fwd_closes.append(cs1h_all[j]["close"])
                    fwd_highs.append(cs1h_all[j]["high"])
                    fwd_lows.append(cs1h_all[j]["low"])
                else:
                    break

            if len(fwd_closes) < 3:
                continue

            hour_of_day = cs1h_all[i]["t"].hour if hasattr(cs1h_all[i]["t"], "hour") else 0
            amp_entry   = amplitude(cs1h, 6)

            records.append(SignalRecord(
                sym, cs1h_all[i]["t"], direction,
                entry_close, entry_next_open,
                fwd_closes, fwd_highs, fwd_lows,
                hour_of_day, amp_entry
            ))

    return records


# ── 统计计算 ───────────────────────────────────────────────────────────────────

def pnl_at(direction: str, entry: float, fwd_price: float) -> float:
    if direction == "LONG":
        return (fwd_price - entry) / entry
    else:
        return (entry - fwd_price) / entry


def compute_stats(records: list[SignalRecord], strat_name: str) -> dict:
    stats = {
        "strategy": strat_name,
        "n_signals": len(records),
        "horizons": {},
        "sl_analysis": {},
        "tp_analysis": {},
        "time_of_day": defaultdict(lambda: {"n": 0, "wins": 0}),
        "mfe_at_3h": [],
        "mae_at_3h": [],
        "peak_time": [],
    }

    for rec in records:
        entry = rec.entry_close
        dir_  = rec.direction
        pnl_seq = [pnl_at(dir_, entry, p) for p in rec.fwd_closes]

        for h in HORIZONS:
            if h > len(pnl_seq):
                continue
            pnl = pnl_seq[h - 1]
            if h not in stats["horizons"]:
                stats["horizons"][h] = {"n": 0, "wins": 0, "pnl_sum": 0.0, "pnl_list": []}
            d = stats["horizons"][h]
            d["n"]        += 1
            d["wins"]     += 1 if pnl > 0 else 0
            d["pnl_sum"]  += pnl
            d["pnl_list"].append(pnl)

        n_fwd = len(rec.fwd_closes)
        if dir_ == "LONG":
            fav_prices = rec.fwd_highs[:n_fwd]
            adv_prices = rec.fwd_lows[:n_fwd]
            mfe = max((p - entry) / entry for p in fav_prices) if fav_prices else 0
            mae = max((entry - p) / entry for p in adv_prices) if adv_prices else 0
        else:
            fav_prices = rec.fwd_lows[:n_fwd]
            adv_prices = rec.fwd_highs[:n_fwd]
            mfe = max((entry - p) / entry for p in fav_prices) if fav_prices else 0
            mae = max((p - entry) / entry for p in adv_prices) if adv_prices else 0

        stats["mfe_at_3h"].append(mfe)
        stats["mae_at_3h"].append(mae)

        if pnl_seq:
            peak_idx = max(range(len(pnl_seq)), key=lambda k: pnl_seq[k])
            stats["peak_time"].append(peak_idx + 1)

        h_slot = (rec.hour_of_day // 4) * 4
        tod    = stats["time_of_day"][h_slot]
        tod["n"] += 1
        if len(pnl_seq) >= 3:
            tod["wins"] += 1 if pnl_seq[2] > 0 else 0

    mae_list = sorted(stats["mae_at_3h"])
    if mae_list:
        for sl_pct in [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020]:
            stopped = sum(1 for m in mae_list if m >= sl_pct)
            stats["sl_analysis"][sl_pct] = {
                "stopped_pct": stopped / len(mae_list) * 100,
                "stopped_n": stopped,
            }

    mfe_list = stats["mfe_at_3h"]
    if mfe_list:
        for tp_pct in [0.010, 0.015, 0.020, 0.025, 0.030, 0.040, 0.050]:
            hit = sum(1 for m in mfe_list if m >= tp_pct)
            stats["tp_analysis"][tp_pct] = {
                "hit_pct": hit / len(mfe_list) * 100,
                "hit_n": hit,
            }

    return stats


def percentile(lst: list, p: float) -> float:
    if not lst: return 0.0
    s = sorted(lst)
    idx = max(0, min(len(s) - 1, int(len(s) * p / 100)))
    return s[idx]


# ── 最优参数提取 ───────────────────────────────────────────────────────────────

def derive_optimal_params(stats: dict) -> dict:
    """
    从统计结果推导最优 SL、TP、hold_h。
    返回 {"sl_pct": float, "tp_pct": float, "hold_h": int}
    """
    # 最优持仓时长 = 期望值最高的时长
    best_ev_h = 3
    best_ev   = -999
    for h in HORIZONS:
        if h not in stats["horizons"]:
            continue
        d = stats["horizons"][h]
        if d["n"] == 0:
            continue
        ev = d["pnl_sum"] / d["n"] * 100
        if ev > best_ev:
            best_ev   = ev
            best_ev_h = h

    # 最优 SL = 净效益（切输单% - 切赢单%）最大的 SL
    h3  = stats["horizons"].get(3)
    best_sl  = 0.010
    best_net = -999
    if h3:
        win_mae  = [mae for mae, pnl in zip(stats["mae_at_3h"], h3["pnl_list"]) if pnl > 0]
        lose_mae = [mae for mae, pnl in zip(stats["mae_at_3h"], h3["pnl_list"]) if pnl <= 0]
        for sl_pct in [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020]:
            w_cut = sum(1 for m in win_mae  if m >= sl_pct)
            l_cut = sum(1 for m in lose_mae if m >= sl_pct)
            w_pct = w_cut / len(win_mae)  * 100 if win_mae  else 0
            l_pct = l_cut / len(lose_mae) * 100 if lose_mae else 0
            net   = l_pct - w_pct
            if net > best_net:
                best_net = net
                best_sl  = sl_pct

    # 最优 TP = 50th 百分位 MFE 取整到 0.5% (确保 50% 以上信号能触及)
    mfe_50 = percentile(stats["mfe_at_3h"], 50) if stats["mfe_at_3h"] else 0.015
    # 向下对齐到 0.005 步长
    best_tp = max(0.010, round(mfe_50 / 0.005) * 0.005)
    best_tp = min(best_tp, 0.030)  # 上限 3%

    return {"sl_pct": round(best_sl, 4), "tp_pct": round(best_tp, 4), "hold_h": best_ev_h}


# ── 自动更新 dimension_trader.py 中的 STRATEGY_PARAMS ─────────────────────────

TRADER_FILE = Path(__file__).parent / "dimension_trader.py"

_DB_CFG = {
    "host":      os.getenv("DB_HOST", "localhost"),
    "port":      int(os.getenv("DB_PORT", 3306)),
    "user":      os.getenv("DB_USER", "root"),
    "password":  os.getenv("DB_PASSWORD", ""),
    "database":  os.getenv("DB_NAME", "binance-data"),
    "charset":   "utf8mb4",
    "autocommit": True,
}


def update_strategy_params(strat_name: str, params: dict,
                            signal_count: int = None, backtest_wr: float = None) -> bool:
    """
    将策略最优参数写入 DB strategy_params 表（主路径）。
    同时更新 dimension_trader.py 中的硬编码兜底 dict（备份路径）。
    """
    # 1. 写 DB
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute("""
                INSERT INTO strategy_params
                    (strategy_name, sl_pct, tp_pct, hold_h, source, signal_count, backtest_wr)
                VALUES (%s, %s, %s, %s, 'signal_analysis', %s, %s)
                ON DUPLICATE KEY UPDATE
                    sl_pct=VALUES(sl_pct), tp_pct=VALUES(tp_pct), hold_h=VALUES(hold_h),
                    source='signal_analysis', signal_count=VALUES(signal_count),
                    backtest_wr=VALUES(backtest_wr), updated_at=CURRENT_TIMESTAMP
            """, (strat_name, params["sl_pct"], params["tp_pct"], params["hold_h"],
                  signal_count, backtest_wr))
        conn.close()
        print(f"  [OK] DB updated: strategy_params[\"{strat_name}\"] "
              f"= SL={params['sl_pct']*100:.1f}%  TP={params['tp_pct']*100:.1f}%  hold={params['hold_h']}h")
    except Exception as e:
        print(f"  [WARNING] DB write failed for {strat_name}: {e}")

    # 2. 同步更新 dimension_trader.py 中的硬编码 dict（作为兜底）
    try:
        code = TRADER_FILE.read_text(encoding="utf-8")
        sl_str   = str(params["sl_pct"])
        tp_str   = str(params["tp_pct"])
        hold_str = str(params["hold_h"])
        new_entry = (f'    "{strat_name}":{" " * max(1, 42 - len(strat_name))}'
                     f'{{"sl_pct": {sl_str}, "tp_pct": {tp_str}, "hold_h": {hold_str}}},')
        pattern = rf'^    "{re.escape(strat_name)}"\s*:.*$'
        new_code, count = re.subn(pattern, new_entry, code, flags=re.MULTILINE)
        if count == 0:
            marker = '    # ── LONG 其他 ───'
            if marker in new_code:
                new_code = new_code.replace(marker, new_entry + "\n" + marker)
            else:
                print(f"  [WARNING] cannot locate insertion point for {strat_name} in .py")
                return True  # DB 已写入，不算失败
        TRADER_FILE.write_text(new_code, encoding="utf-8")
    except Exception as e:
        print(f"  [WARNING] .py file update failed for {strat_name}: {e}")

    return True


# ── 报告输出 ───────────────────────────────────────────────────────────────────

def print_report(stats: dict, strat_meta: dict, optimal: dict):
    n = stats["n_signals"]
    sep = "-" * 72

    print(f"\n{'='*72}")
    print(f"  策略: {stats['strategy']}  (历史验证胜率 {strat_meta['test_wr']}%)")
    print(f"  信号总数: {n}  |  数据: {DEFAULT_START} ~ {DEFAULT_END}  |  标的: {len(ANALYSIS_SYMBOLS)}个")
    print(f"  [最优参数] SL={optimal['sl_pct']*100:.1f}%  "
          f"TP={optimal['tp_pct']*100:.1f}%  hold={optimal['hold_h']}h")
    print(f"{'='*72}")

    if n == 0:
        print("  无信号，跳过。")
        return

    # 1. 各持仓时长
    print("\n[1] 各持仓时长分析")
    print(f"  {'时长':>4}  {'信号数':>6}  {'胜率':>7}  {'均盈%':>8}  {'均亏%(败样本)':>14}  {'EV%(期望)':>10}")
    print(f"  {sep[:66]}")
    best_ev_h, best_ev = None, -999
    for h in HORIZONS:
        if h not in stats["horizons"]:
            continue
        d  = stats["horizons"][h]
        hn = d["n"]
        if hn == 0:
            continue
        wr    = d["wins"] / hn * 100
        avg   = d["pnl_sum"] / hn * 100
        wins  = [p for p in d["pnl_list"] if p > 0]
        loses = [p for p in d["pnl_list"] if p <= 0]
        avg_w = sum(wins)  / len(wins)  * 100 if wins  else 0
        avg_l = sum(loses) / len(loses) * 100 if loses else 0
        mark  = " <<" if h == optimal["hold_h"] else ""
        print(f"  {h:>4}h  {hn:>6}  {wr:>6.1f}%  {avg_w:>+7.3f}%  "
              f"{avg_l:>+12.3f}%    {avg:>+8.3f}%{mark}")
        if avg > best_ev:
            best_ev   = avg
            best_ev_h = h
    if best_ev_h:
        print(f"\n  >> 最优持仓时长：{best_ev_h}h（EV={best_ev:+.3f}%）")

    # 2. TP 分析
    mfe = sorted(stats["mfe_at_3h"])
    if mfe:
        print(f"\n[2] MFE 50th={percentile(mfe,50)*100:.2f}%  TP 命中率：")
        for tp_pct, v in stats["tp_analysis"].items():
            mark = " <<" if abs(tp_pct - optimal["tp_pct"]) < 0.001 else ""
            print(f"    TP={tp_pct*100:.1f}%  命中 {v['hit_pct']:>5.1f}%  ({v['hit_n']}/{len(mfe)}){mark}")

    # 3. SL 分析
    if stats["mae_at_3h"]:
        h3 = stats["horizons"].get(3)
        if h3:
            win_pnl  = [d for d in zip(stats["mae_at_3h"], h3["pnl_list"]) if d[1] > 0]
            lose_pnl = [d for d in zip(stats["mae_at_3h"], h3["pnl_list"]) if d[1] <= 0]
            print(f"\n[3] SL 净效益（赢单={len(win_pnl)} 输单={len(lose_pnl)}）：")
            print(f"  {'SL%':>5}  {'赢单被截%':>10}  {'输单被截%':>10}  {'净效益':>10}")
            for sl_pct in [0.005, 0.008, 0.010, 0.012, 0.015, 0.018, 0.020]:
                w_cut = sum(1 for mae_v, _ in win_pnl  if mae_v >= sl_pct)
                l_cut = sum(1 for mae_v, _ in lose_pnl if mae_v >= sl_pct)
                w_pct = w_cut / len(win_pnl)  * 100 if win_pnl  else 0
                l_pct = l_cut / len(lose_pnl) * 100 if lose_pnl else 0
                benefit = l_pct - w_pct
                mark = " <<" if abs(sl_pct - optimal["sl_pct"]) < 0.001 else ""
                print(f"  {sl_pct*100:.1f}%  {w_pct:>9.1f}%  {l_pct:>9.1f}%  {benefit:>+9.1f}%{mark}")

    # 4. 时段分布
    tod = stats["time_of_day"]
    if tod:
        print(f"\n[4] 信号时段分布（UTC+8，3h 胜负）：")
        for slot in sorted(tod.keys()):
            d   = tod[slot]
            hn  = d["n"]
            wr  = d["wins"] / hn * 100 if hn > 0 else 0
            end = (slot + 4) % 24
            bar = "#" * int(wr / 2)
            print(f"  {slot:02d}-{end:02d}  {hn:>4}信号  {wr:>5.1f}%  {bar}")

    print()


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="策略信号历史分析 + 自动更新最优参数")
    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("--batch",      type=int, default=None,
                     help=f"跑指定批次（1-{len(BATCHES)}），每批 5 个策略")
    grp.add_argument("--strategies", nargs="+",
                     choices=list(ALL_STRATEGIES.keys()),
                     help="手动指定策略列表")
    parser.add_argument("--start",     default=DEFAULT_START)
    parser.add_argument("--end",       default=DEFAULT_END)
    parser.add_argument("--symbols",   nargs="+", default=None)
    parser.add_argument("--no-update", action="store_true",
                        help="只看报告，不写回 dimension_trader.py")
    args = parser.parse_args()

    if args.symbols:
        global ANALYSIS_SYMBOLS
        ANALYSIS_SYMBOLS = [s.upper().replace("USDT", "/USDT") if "/" not in s else s
                            for s in args.symbols]

    # 决定本次跑哪些策略
    if args.batch is not None:
        if args.batch not in BATCHES:
            print(f"ERROR: --batch 必须在 1-{len(BATCHES)} 之间")
            sys.exit(1)
        strat_keys = BATCHES[args.batch]
        print(f"批次 {args.batch}/{len(BATCHES)}: {strat_keys}")
    elif args.strategies:
        strat_keys = args.strategies
    else:
        strat_keys = DEFAULT_STRATS

    print(f"\n信号历史分析  {args.start} ~ {args.end}")
    print(f"标的数: {len(ANALYSIS_SYMBOLS)}  |  策略: {strat_keys}")
    if not args.no_update:
        print("完成后将自动更新 dimension_trader.py 中的 STRATEGY_PARAMS")
    print()

    conn = pymysql.connect(**_DB_CFG)

    print("加载 BTC 4h 数据...")
    btc4h_all = load_candles_range(conn, "BTC/USDT", "4h", args.start, args.end)
    print(f"  BTC 4h: {len(btc4h_all)} 根\n")

    updated = []
    for strat_key in strat_keys:
        strat = ALL_STRATEGIES[strat_key]
        print(f"{'='*72}")
        print(f"扫描策略: {strat['name']}  ({args.start} ~ {args.end})")
        t0 = time.time()

        records = scan_strategy(strat_key, strat, conn, args.start, args.end, btc4h_all)

        elapsed = time.time() - t0
        print(f"找到 {len(records)} 个信号  ({elapsed:.1f}s)")

        if not records:
            print("  无信号数据，跳过。")
            continue

        stats   = compute_stats(records, strat["name"])
        optimal = derive_optimal_params(stats)
        print_report(stats, strat, optimal)

        if not args.no_update:
            ok = update_strategy_params(
                strat["name"], optimal,
                signal_count=stats["n_signals"],
                backtest_wr=strat.get("test_wr"),
            )
            if ok:
                updated.append(strat["name"])
            else:
                print(f"  [WARNING] {strat['name']} 未能写入，请手动添加")

    conn.close()

    if updated:
        print(f"\n{'='*72}")
        print(f"本批次共更新 {len(updated)} 个策略参数：")
        for name in updated:
            print(f"  - {name}")
        print("strategy_params 已写入 DB，dimension_trader 将在下次刷新（最多 1h）自动加载新参数。")

    print("\n分析完成。")


if __name__ == "__main__":
    main()
