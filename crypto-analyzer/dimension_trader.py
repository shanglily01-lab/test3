#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dimension_trader.py
===================
7个维度策略的实盘信号交易机器人（含 AI 探索发现的新策略）。

原有策略（Big4 x 1h x 90d，走时验证通过）：

  D1a-MTF-HDecay         : 4h宏观下行(<-0.4%) + 1h 3连阳振幅+成交量双衰减
                           Big4: 76.5%胜率  34样本  测试集 85.7%

  D4b-FluxQuality        : 1h 3连阳 + 买压比率严格↓ + 最新买压 < 47%
                           Big4: 73.5%胜率  34样本  测试集 91.7%

  D3-AltLag              : BTC 4h下行(<-0.5%) + 山寨1h局部反弹(>0.3%) + 山寨买压 < 55%
                           41山寨: 64.7%胜率  2061样本  测试集 63.0%

AI探索发现的新策略（strategy_explorer.py，走时验证通过）：

  E1-DecelFluxShort      : 1h梯度减速(fast<slow*0.95) + flux衰减 + 4h宏观不强
                           SHORT  99alts: 66.0%胜率训练  62.3%测试  n=1072+369

  E2-MTFDivergentExhaust : 4h宏观下行且加速 + 1h局部反弹但买压<0.48 => SHORT
                           4h宏观上行且持续 + 1h局部回调但买压>0.52 => LONG
                           99alts: 68.8%胜率训练  62.0%测试  n=736+337

  E3-AltDipRecovery      : BTC 4h强上行(>0.5%) + 山寨急跌+高振幅 + flux回升 => LONG
                           top20alts: 63.7%胜率训练  57.0%测试  n=1445+881
                           仅用于高胜率精选标的

  E4-KineticEfficiencyShort : 4h下行 + 1h反弹但能效比衰减>30% + 买压弱 => SHORT
                           99alts: 63.0%胜率训练  60.7%测试  n=670+313

  E5-TripleFluxEntropy   : 4h宏观下行 + 1h价格小涨 + 三级买压严格阶梯递减(f2<f4<f6) + f2<0.47
                           SHORT  99alts: 63.2%胜率训练  65.0%测试  n=620+223

  E6-InertialFrictionShort : 4h宏观下行 + 1h微涨 + K线实体占比<22% + 买压高位后转头
                           SHORT  99alts: 62.8%胜率训练  63.1%测试  n=541+255

  E7-PassiveSupplySuffocation : BTC 4h下行 + 山寨高flux反弹但单位买压涨幅效率暴跌(>60%) => SHORT
                           mtf_btc  99alts: 68.0%胜率训练  62.0%测试  n=878+531

  E8-HollowGrowthShort   : 4h宏观下行 + 1h小K线上漂 + 成交量载荷骤降(<60%) + flux低 => SHORT
                           SHORT  99alts: 65.6%胜率训练  60.9%测试  n=244+64

  E9-WickSaturationShort : 4h宏观下行 + 1h反弹 + 上影线>实体1.5倍 + flux<0.49 => SHORT
                           SHORT  99alts: 62.2%胜率训练  62.9%测试  n=529+280

  E10-FrictionDecoupling : 4h下行 + 1h反弹 + flux>0.48但实体<25% + 买压边际减弱 => SHORT
                           SHORT  99alts: 61.3%胜率训练  61.5%测试  n=1116+512

  E11-ShadowLaggardShort : BTC 4h强弱 + 山寨1h抗跌 + 买压三级阶梯衰减(f2<f5<f10) + f2<0.48
                           mtf_btc  99alts: 62.4%胜率训练  61.1%测试  n=643+319

  E12-NonLinearExhaustion : 4h下行 + 1h反弹 + 振幅放大但购买力转换效率暴跌至20%以下 => SHORT
                           SHORT  99alts: 61.6%胜率训练  63.3%测试  n=511+267

  E13-ConvexityTrapShort : 4h弱势 + 1h反弹 + 买压转换效率不足前期30% + flux<0.53 => SHORT
                           SHORT  99alts: 66.7%胜率训练  61.4%测试  n=579+347

  E14-InertialDragShort  : 4h强弱(gradient<-0.6%) + 1h虚涨(实体<22%) + 梯度减速60% => SHORT
                           SHORT  99alts: 62.0%胜率训练  62.0%测试  n=313+263

  E15-ElasticityCollapse : BTC 4h下行 + 山寨1h涨 + 超额买压弹性断崖跌至<30% + flux<0.51 => SHORT
                           mtf_btc  99alts: 66.8%胜率训练  62.1%测试  n=301+195

手工验证的 LONG 策略（DecelBounce 家族，E16-E30）：

  核心逻辑：4h上升 + 1h回调N根 + 1h刚刚翻正 + flux>0.53 => LONG
  所有策略均在86个标的 70/30 走时验证中通过，测试胜率 57-69%

  E16-DecelBounce       : h=6 + amp(3)<0.030   训练60.9% 测试58.3%
  E17-DecelBounce-Deep  : h=8                   训练63.7% 测试63.9%
  E18-DecelBounce-LowAmp: h=6 + amp(2)<0.018   训练60.6% 测试59.3%
  E19-DB-h4             : h=4                   训练61.1% 测试59.6%
  E20-DB-h5             : h=5                   训练62.0% 测试58.3%
  E21-DB-h7             : h=7                   训练63.6% 测试64.1%
  E22-DB-h10            : h=10                  训练62.7% 测试63.1%
  E23-DB-AmpMed         : h=6 + amp(2)<0.022   训练60.7% 测试59.0%
  E24-DB-DeepLowAmp     : h=8 + amp(2)<0.018   训练63.9% 测试64.1%
  E25-DB-AmpMacro       : h=6 + 4h-amp>2%      训练62.4% 测试59.1%
  E26-DB-3macroN        : h=6 + mac_n=3         训练60.4% 测试62.4%
  E27-DB-h3             : h=3                   训练60.1% 测试57.1%
  E28-DB-h12            : h=12                  训练62.9% 测试67.7%
  E29-DB-h14            : h=14                  训练60.7% 测试69.0%
  E30-DB-h7-DeepDecline : h=7 + hist<-0.4%     训练64.0% 测试65.3%

用法:
  .venv/Scripts/python.exe dimension_trader.py
  .venv/Scripts/python.exe dimension_trader.py --dry-run
  .venv/Scripts/python.exe dimension_trader.py --scan
"""

import argparse
import json
import os
import sys
import time
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

# 固定使用 UTC+8，避免系统时区配置影响
UTC8 = timezone(timedelta(hours=8))

import pymysql
import requests as req
from dotenv import load_dotenv
from loguru import logger

load_dotenv()

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# ── 配置 ───────────────────────────────────────────────────────────────────────

API_BASE         = "http://localhost:9021"
ACCOUNT_ID       = 2
MARGIN_PER_TRADE = 1000.0
# 按策略覆盖保证金（不影响全局默认值）
MARGIN_OVERRIDE = {
    "D3-AltLag": 100.0,
}
LEVERAGE         = 5
MAX_HOLD_HOURS   = 3
CHECK_INTERVAL   = 60    # 1min: 加 BTC 闸门后降低扫描间隔，方向反应更快

SL_MULT = 1.5
TP_MULT = 2.5
SL_MIN  = 0.005
SL_MAX  = 0.020

# 全局统一止损止盈（覆盖所有策略的 sl_pct/tp_pct）
# 基于开仓实时价格计算，与策略信号价格无关
GLOBAL_SL_PCT = 0.02   # 2%
GLOBAL_TP_PCT = 0.03   # 3%

# 开仓前最小价差保护：live_price 距 SL 的距离 / live_price 不得低于此值
MIN_SL_GAP_PCT = 0.003

MAX_POSITIONS_PER_STRATEGY = 5   # 每种策略最多同时持仓数

# ── 策略参数系统默认（统一 SL=2% / TP=3% / hold=3h）──────────────────────────
# 按用户指令：所有通过的策略不再跑 Phase 2 参数优化，统一按系统默认配置。
# DB 加载失败时走此兜底；DB 加载成功后由 strategy_params 表覆盖（也全部为 2/3/3h）。
# sl_pct: 0.02, tp_pct: 0.03, hold_h: 3（对应 system_settings.max_hold_hours=3）
_UNIFIED_DEFAULT = {"sl_pct": 0.02, "tp_pct": 0.03, "hold_h": 3}

_STRATEGY_PARAMS_DEFAULT: dict[str, dict] = {
    # ── SHORT 策略 E1-E15：回测最优持仓 8h，SL 1.0-1.2%，TP 1.5% ───────────
    "E1-DecelFluxShort":                         {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "E2-MTFDivergentExhaust":                    {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 12},
    "E4-KineticEfficiencyShort":                 {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 8},
    "E5-TripleFluxEntropy":                      {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "E6-InertialFrictionShort":                  {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 12},
    "E8-HollowGrowthShort":                      {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "E9-WickSaturationShort":                    {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 12},
    "E10-FrictionDecouplingShort":               {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "E12-NonLinearExhaustion":                   {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "E14-InertialDragShort":                     {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 12},
    # ── DecelBounce 家族 LONG：按回调深度决定参数 ───────────────────────────
    # h=3-5：浅回调，快进快出
    "E19-DB-h4":                                 {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 12},
    "E20-DB-h5":                                 {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 12},
    # h=6-8：中等回调
    "E16-DecelBounce":                           {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "E17-DecelBounce-Deep":                      {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 6},
    "E18-DecelBounce-LowAmp":                    {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "E21-DB-h7":                                 {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 12},
    "E23-DB-AmpMed":                             {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "E24-DB-DeepLowAmp":                         {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "E25-DB-AmpMacro":                           {"sl_pct": 0.02, "tp_pct": 0.025, "hold_h": 12},
    "E26-DB-3macroN":                            {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "E30-DB-h7-DeepDecline":                     {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 12},
    # h=10-14：深回调，胜率最高，SL 稍宽，TP 更高
    "E22-DB-h10":                                {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 6},
    "E28-DB-h12":                                {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "E29-DB-h14":                                {"sl_pct": 0.012, "tp_pct": 0.03, "hold_h": 6},

    "sig_E31-DB_h16":                            {"sl_pct": 0.008, "tp_pct": 0.03, "hold_h": 12},
    "sig_E34-DB_h15":                            {"sl_pct": 0.008, "tp_pct": 0.025, "hold_h": 3},
    "sig_E36-DB_h8_f56":                         {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "sig_E49-DB_h20":                            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    "sig_E90-DB_h25":                            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    "sig_E46-OvrSold_h10_d12_f55":               {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E52-FluxAccel_h10_mac5_f51":            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    "sig_E40-BTCLead_b10_h8_f50":                {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},

    "sig_E89-BTCLead_b10_h6_f50":                {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 12},
    "sig_E32-DB_h14_ht3":                        {"sl_pct": 0.018, "tp_pct": 0.03, "hold_h": 6},
    "sig_E33-DB_h14_f55":                        {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "sig_E35-DB_h14_ht4":                        {"sl_pct": 0.018, "tp_pct": 0.03, "hold_h": 6},
    "sig_E37-DB_h14_ht5":                        {"sl_pct": 0.018, "tp_pct": 0.03, "hold_h": 6},
    "sig_E38-DB_h14_f56":                        {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 8},
    "sig_E39-DB_h12_ht4":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "sig_E41-DB_h14_ht6":                        {"sl_pct": 0.015, "tp_pct": 0.03, "hold_h": 6},
    "sig_E42-DB_h12_ht3":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "sig_E43-DB_h12_ht5":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "sig_E44-DB_h12_ht6":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "sig_E45-DB_h10_f56":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 12},
    "sig_E48-DB_h8_f55":                         {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 12},
    "sig_E56-DB_h12_f55":                        {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 8},
    "sig_E58-DB_h8_ht6":                         {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 3},
    "sig_E62-DB_h8_ht5":                         {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 3},
    "sig_E59-DB_h10_ht4":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "sig_E65-DB_h8_ht3":                         {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 3},
    "sig_E67-DB_h18":                            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    "sig_E68-DB_h10_ht5":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "sig_E69-DB_h10_ht3":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "sig_E71-DB_h8_ht4":                         {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 3},
    "sig_E72-DB_h10_ht6":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "sig_E76-DB_h10_f55":                        {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "sig_E77-DB_h12_f56":                        {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "sig_E73-FluxAccel_h10_mac5_f53":            {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "sig_E50-OvrSold_h10_d20_f53":               {"sl_pct": 0.02, "tp_pct": 0.03, "hold_h": 3},
    "sig_E51-OvrSold_h10_d10_f55":               {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E53-OvrSold_h10_d15_f55":               {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "sig_E55-OvrSold_h10_d8_f55":                {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E63-OvrSold_h10_d15_f53":               {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "sig_E66-OvrSold_h10_d12_f53":               {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "sig_E70-OvrSold_h10_d8_f53":                {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "sig_E74-OvrSold_h10_d10_f53":               {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "sig_E84-OvrSold_h8_d12_f55":                {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "sig_E64-FluxAccel_h10_mac2_f51":            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E54-FluxAccel_h10_mac5_f49":            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E57-FluxAccel_h10_mac3_f49":            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E60-FluxAccel_h10_mac3_f51":            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E61-FluxAccel_h10_mac2_f49":            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E75-FluxAccel_h8_mac5_f51":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 4},
    "sig_E78-FluxAccel_h8_mac3_f51":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E79-FluxAccel_h8_mac5_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E80-FluxAccel_h8_mac2_f51":             {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "sig_E81-FluxAccel_h10_mac3_f53":            {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "sig_E82-FluxAccel_h8_mac3_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E83-FluxAccel_h8_mac2_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E85-FluxAccel_h10_mac2_f53":            {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 8},
    "sig_E86-FluxAccel_h8_mac3_f53":             {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 8},
    "sig_E87-FluxAccel_h8_mac5_f53":             {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 8},
    "sig_E88-FluxAccel_h8_mac2_f53":             {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 8},
    "sig_E91-FluxAccel_h6_mac5_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E92-FluxAccel_h6_mac3_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E93-FluxAccel_h6_mac2_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "sig_E94-FluxAccel_h6_mac5_f53":             {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 8},
    "sig_E95-FluxAccel_h6_mac3_f51":             {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "sig_E96-FluxAccel_h6_mac5_f51":             {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "sig_E97-FluxAccel_h6_mac2_f51":             {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "sig_E98-FluxAccel_h4_mac5_f49":             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "OFD_L_n5_t15":                              {"sl_pct": 0.008, "tp_pct": 0.02, "hold_h": 8},
    "OFD_L_n8_t10":                              {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "VolMom_L_n5_l2_v40":                        {"sl_pct": 0.015, "tp_pct": 0.03, "hold_h": 12},
    "VolMom_L_n5_l3_v40":                        {"sl_pct": 0.015, "tp_pct": 0.03, "hold_h": 6},
    "VolMom_L_n5_l4_v40":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 6},
    "VolMom_L_n3_l2_v40":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "VolMom_L_n3_l3_v40":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},

    "VolMom_L_n5_l2_v20":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 6},
    "VolMom_L_n5_l3_v20":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 6},
    "VolMom_L_n5_l4_v20":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "VolMom_L_n3_l2_v20":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "VolMom_L_n3_l3_v20":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "VolMom_L_n3_l4_v20":                        {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "CC_L_n12_lo20":                             {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "CC_L_n10_lo25":                             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "CC_L_n12_lo30":                             {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    "CC_L_n8_lo30":                              {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    "PVel_L_n3_a10_v20":                         {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 6},
    "PVel_L_n3_a6_v20":                          {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 3},
    "PVel_L_n5_a10_v15":                         {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "PVel_L_n3_a10_v15":                         {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 6},
    "PVel_L_n5_a10_v20":                         {"sl_pct": 0.012, "tp_pct": 0.03, "hold_h": 6},
    "PVel_L_n3_a6_v15":                          {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    "PVel_L_n5_a6_v15":                          {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    # ── LONG 其他 ───────────────────────────────────────────────────────────
    "E3-AltDipRecovery":                         {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 8},
    # ── Alien 系列：auto_explore_alien.py 四阶段验证通过 ─────────────────────────
    # SHORT alien
    "A2-BuyExhShort":                            {"sl_pct": 0.01, "tp_pct": 0.015, "hold_h": 12},
    "A3-MomDecay-l10-r50":                       {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 4},
    "A4-MomDecay-l10-r40":                       {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 4},
    "A5-MomDecay-l10-r30":                       {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 4},

    "A7-MomDecay-l8-r40":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    "A8-MomDecay-l8-r30":                        {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 6},
    # LONG alien
    "A1-SellCapLong":                            {"sl_pct": 0.008, "tp_pct": 0.02, "hold_h": 12},
    "A9-SpatDivLong":                            {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 8},
    # ── Alien 系列 Batch2：auto_explore_alien2.py 四阶段验证通过 ────────────────
    # PriceMemory SHORT (test 64-77%)
    "A10-PriceMem-S-n20-hi75":                   {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 6},
    "A11-PriceMem-S-n20-hi80":                   {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 8},
    "A12-PriceMem-S-n20-hi85":                   {"sl_pct": 0.012, "tp_pct": 0.03, "hold_h": 8},
    "A13-PriceMem-S-n14-hi75":                   {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 6},
    "A16-PriceMem-S-n14-hi80":                   {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 6},
    "A21-PriceMem-S-n10-hi85":                   {"sl_pct": 0.008, "tp_pct": 0.025, "hold_h": 8},
    "A22-PriceMem-S-n10-hi75":                   {"sl_pct": 0.01, "tp_pct": 0.03, "hold_h": 8},
    "A23-PriceMem-S-n10-hi80":                   {"sl_pct": 0.008, "tp_pct": 0.03, "hold_h": 8},
    "A24-PriceMem-S-n14-hi85":                   {"sl_pct": 0.018, "tp_pct": 0.03, "hold_h": 8},
    # PriceMemory LONG (test 63-70%)
    "A14-PriceMem-L-n20-lo15":                   {"sl_pct": 0.02, "tp_pct": 0.03, "hold_h": 4},
    "A15-PriceMem-L-n20-lo25":                   {"sl_pct": 0.015, "tp_pct": 0.03, "hold_h": 3},
    "A17-PriceMem-L-n20-lo20":                   {"sl_pct": 0.018, "tp_pct": 0.03, "hold_h": 4},
    "A18-PriceMem-L-n14-lo25":                   {"sl_pct": 0.012, "tp_pct": 0.025, "hold_h": 4},
    "A19-PriceMem-L-n14-lo20":                   {"sl_pct": 0.015, "tp_pct": 0.025, "hold_h": 3},
    "A20-PriceMem-L-n14-lo15":                   {"sl_pct": 0.02, "tp_pct": 0.025, "hold_h": 3},
    "A26-PriceMem-L-n10-lo25":                   {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 4},
    "A27-PriceMem-L-n10-lo15":                   {"sl_pct": 0.018, "tp_pct": 0.02, "hold_h": 4},
    "A29-PriceMem-L-n10-lo20":                   {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 4},
    # SaturationVelocity SHORT (test 58-64%)
    "A25-SatVel-S-n3-l3-v6":                     {"sl_pct": 0.008, "tp_pct": 0.015, "hold_h": 12},
    "A31-SatVel-S-n3-l2-v4":                     {"sl_pct": 0.008, "tp_pct": 0.02, "hold_h": 12},
    "A34-SatVel-S-n3-l4-v6":                     {"sl_pct": 0.01, "tp_pct": 0.015, "hold_h": 8},
    "A36-SatVel-S-n4-l4-v6":                     {"sl_pct": 0.008, "tp_pct": 0.01, "hold_h": 12},
    "A37-SatVel-S-n4-l2-v4":                     {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    "A48-SatVel-S-n4-l4-v4":                     {"sl_pct": 0.008, "tp_pct": 0.015, "hold_h": 12},
    # TimePressure SHORT (test 58-59%)
    "A35-TimePres-S-n8-a6-t75":                  {"sl_pct": 0.005, "tp_pct": 0.01, "hold_h": 12},
    "A38-TimePres-S-n10-a6-t75":                 {"sl_pct": 0.005, "tp_pct": 0.01, "hold_h": 12},
    "A45-TimePres-S-n10-a6-t65":                 {"sl_pct": 0.005, "tp_pct": 0.01, "hold_h": 12},
    # FluxMomentum SHORT (test 58-62%)
    "A30-FluxMom-S-s3-l6-f3":                    {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 12},
    "A33-FluxMom-S-s2-l8-f4":                    {"sl_pct": 0.01, "tp_pct": 0.025, "hold_h": 8},
    "A40-FluxMom-S-s2-l12-f5":                   {"sl_pct": 0.01, "tp_pct": 0.02, "hold_h": 12},
    # FluxMomentum LONG (test 57-63%)
    "A28-FluxMom-L-s3-l12-f5":                   {"sl_pct": 0.02, "tp_pct": 0.02, "hold_h": 4},
    "A32-FluxMom-L-s3-l12-f4":                   {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 4},
    "A39-FluxMom-L-s2-l8-f4":                    {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "A41-FluxMom-L-s2-l6-f3":                    {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "A42-FluxMom-L-s2-l12-f5":                   {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 2},
    "A43-FluxMom-L-s2-l6-f4":                    {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "A44-FluxMom-L-s2-l12-f3":                   {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "A46-FluxMom-L-s2-l8-f3":                    {"sl_pct": 0.012, "tp_pct": 0.02, "hold_h": 8},
    "A47-FluxMom-L-s3-l6-f5":                    {"sl_pct": 0.015, "tp_pct": 0.02, "hold_h": 4},
}

# 按统一指令：上面保留的 key 列表仅作为"硬编码兜底的策略名集合"
# value 全部覆盖为系统默认（SL=2%/TP=3%/hold=3h）；DB 加载后仍会再次覆盖但默认一致。
_STRATEGY_PARAMS_DEFAULT = {_k: dict(_UNIFIED_DEFAULT) for _k in _STRATEGY_PARAMS_DEFAULT}

# 运行时参数表（从 DB 加载后覆盖 _STRATEGY_PARAMS_DEFAULT）
STRATEGY_PARAMS: dict[str, dict] = dict(_STRATEGY_PARAMS_DEFAULT)
_params_last_reload: float = 0.0   # unix timestamp

# 运行时方向开关（从 Big4 regime 自动推断）
_ALLOW_LONG:   bool  = True   # score >= -0.5：任何LONG策略的最宽入场门槛
_ALLOW_SHORT:  bool  = False  # score <= -0.5：任何SHORT策略的最宽入场门槛
_REGIME_SCORE: float = 0.0    # 五级：-1.0=强空, -0.5=轻空, 0=中性, +0.5=轻多, +1.0=强多

# Big4 Regime 参数
_BIG4_LOOKBACK_HOURS  = 6   # 近6H窗口（比48H更及时）
_BIG4_MIN_DIR_RECORDS = 5   # 有方向信号最少条数（不足则中性）

# ── BTC 即时方向闸门（比 4h/Big4 敏感得多） ───────────────────────────────
# 三周期共振判定趋势强弱，最小延迟路径：5m+15m（≤5min），稳态：1h+15m。
#   强空 → block_long=True（禁止开 LONG）
#   强多 → block_short=True（禁止开 SHORT）
# 触发规则（OR）：
#   (1) 1h 和 15m 同向达阈值      → 稳态确认（延迟 ≤15min，需 1h 整点）
#   (2) 5m 和 15m 同向达阈值      → 快速响应（延迟 ≤5min，不等 1h 闭合）
_BTC_GATE_1H_N         = 4       # 1h 最近 4 根（覆盖最近 4 小时）
_BTC_GATE_1H_TH_STRONG = 0.006   # |gradient| 超过 0.6% 视为强趋势
_BTC_GATE_15M_N        = 8       # 15m 最近 8 根（覆盖最近 2 小时）
_BTC_GATE_15M_TH_STRONG = 0.004  # |gradient| 超过 0.4% 视为强趋势
_BTC_GATE_5M_N         = 6       # 5m 最近 6 根（覆盖最近 30 分钟）
_BTC_GATE_5M_TH_STRONG = 0.003   # |gradient| 超过 0.3% 视为强趋势
_BTC_GATE_TTL_SEC      = 300     # 全局 regime 缓存 5 分钟

_BTC_REGIME: dict = {
    "block_long":  False,
    "block_short": False,
    "reason":      "init",
    "ts":          0.0,
    "g1h":         0.0,
    "g15m":        0.0,
    "g5m":         0.0,
}

# ── BTC Gemini 大方向（异步后台线程） ─────────────────────────────────────
_BTC_GEMINI_INTERVAL_SEC = 5 * 60    # 每 5 分钟问一次 Gemini（加速响应）
_BTC_GEMINI_MIN_CONF     = 0.6       # confidence 门槛：低于此值不屏蔽
_BTC_GEMINI_STALE_SEC    = 15 * 60   # 超过 15 分钟没更新（3 轮）则视为过期、不屏蔽

_BTC_GEMINI_REGIME: dict = {
    "verdict":    "NEUTRAL",   # STRONG_LONG / STRONG_SHORT / NEUTRAL
    "confidence": 0.0,
    "reason":     "init",
    "ts":         0.0,
}
_btc_gemini_thread_started: bool = False


def _btc_gemini_tick() -> None:
    """跑一次 BTC Gemini 探测并刷新 _BTC_GEMINI_REGIME。"""
    global _BTC_GEMINI_REGIME
    try:
        import btc_gemini_regime as bgr  # 延迟 import，避免循环依赖
        cs1h  = load_candles_db("BTC/USDT", "1h",  10)
        cs15m = load_candles_db("BTC/USDT", "15m", 10)
        cs5m  = load_candles_db("BTC/USDT", "5m",  14)
        if len(cs1h) < 4 or len(cs15m) < 4:
            logger.warning(
                f"[BTC-GEMINI] data insufficient 1h={len(cs1h)} "
                f"15m={len(cs15m)} 5m={len(cs5m)}"
            )
            return
        r = bgr.ask_gemini_btc(cs1h, cs15m, cs5m)
        _BTC_GEMINI_REGIME.update({
            "verdict":    r.get("verdict", "NEUTRAL"),
            "confidence": float(r.get("confidence", 0.0)),
            "reason":     r.get("reason", ""),
            "ts":         r.get("ts", time.time()),
        })
        logger.info(
            f"[BTC-GEMINI] {_BTC_GEMINI_REGIME['verdict']} "
            f"conf={_BTC_GEMINI_REGIME['confidence']:.2f}  "
            f"reason={_BTC_GEMINI_REGIME['reason'][:160]}"
        )
        # Gemini 刚拿到新判定时，立即检查是否需要紧急平仓（方案 B）
        try:
            _check_and_emergency_exit()
        except Exception as e:
            logger.warning(f"emergency-exit after gemini failed: {e}")
    except Exception as e:
        logger.warning(f"_btc_gemini_tick failed: {e}")


def _start_btc_gemini_worker() -> None:
    """起一个 daemon 线程，每 _BTC_GEMINI_INTERVAL_SEC 秒跑一次 Gemini 判定。"""
    global _btc_gemini_thread_started
    if _btc_gemini_thread_started:
        return
    _btc_gemini_thread_started = True
    import threading

    def _loop():
        while True:
            try:
                _btc_gemini_tick()
            except Exception as e:
                logger.warning(f"[BTC-GEMINI] loop error: {e}")
            time.sleep(_BTC_GEMINI_INTERVAL_SEC)

    t = threading.Thread(target=_loop, name="btc-gemini-worker", daemon=True)
    t.start()
    logger.info(f"[BTC-GEMINI] worker started, interval={_BTC_GEMINI_INTERVAL_SEC}s")


def _gemini_blocks() -> tuple[bool, bool]:
    """返回 (block_long, block_short) 由 Gemini 给出；受 confidence 和陈旧度过滤。"""
    gm = _BTC_GEMINI_REGIME
    if not gm or time.time() - float(gm.get("ts", 0) or 0) > _BTC_GEMINI_STALE_SEC:
        return False, False
    verdict = str(gm.get("verdict", "NEUTRAL")).upper()
    conf    = float(gm.get("confidence", 0.0) or 0)
    if conf < _BTC_GEMINI_MIN_CONF:
        return False, False
    if verdict == "STRONG_SHORT":
        return True, False     # 强空 → 禁 LONG
    if verdict == "STRONG_LONG":
        return False, True     # 强多 → 禁 SHORT
    return False, False


# ── Gemini 正向许可闸门（档位 A）──────────────────────────────────────────
# 开关：True 时，**必须** Gemini 明确同向才允许开单；
#       NEUTRAL/过期/低置信度 → 两方向都不开（空仓优先）
# 开 LONG  前提：verdict=STRONG_LONG  且 conf >= _GEMINI_POSITIVE_MIN_CONF
# 开 SHORT 前提：verdict=STRONG_SHORT 且 conf >= _GEMINI_POSITIVE_MIN_CONF
_REQUIRE_GEMINI_POSITIVE = True
_GEMINI_POSITIVE_MIN_CONF = 0.6


def _gemini_allows() -> tuple[bool, bool]:
    """返回 (allow_long, allow_short)——只有 Gemini 明确同向强信号才放行。

    NEUTRAL / 过期 / 低置信 → (False, False)，即两方向都禁开。
    """
    gm = _BTC_GEMINI_REGIME
    if not gm or time.time() - float(gm.get("ts", 0) or 0) > _BTC_GEMINI_STALE_SEC:
        return False, False
    verdict = str(gm.get("verdict", "NEUTRAL")).upper()
    conf    = float(gm.get("confidence", 0.0) or 0)
    if conf < _GEMINI_POSITIVE_MIN_CONF:
        return False, False
    if verdict == "STRONG_LONG":
        return True, False
    if verdict == "STRONG_SHORT":
        return False, True
    return False, False


# ── 趋势反转紧急平仓 ──────────────────────────────────────────────────────
# 触发条件（全部满足）：
#   1) 技术闸门判同向强（tech block_long / block_short 为 True）
#   2) Gemini 判同向强（STRONG_SHORT / STRONG_LONG）
#   3) Gemini confidence >= _EMERGENCY_EXIT_CONF_TH
# 动作：只平「浮亏 <= _EMERGENCY_EXIT_PNL_TH（保证金级）」的反向仓；
# 浮盈仓和小浮亏仓保留，让 TP/SL 自然走完。
_EMERGENCY_EXIT_CONF_TH     = 0.75       # Gemini 置信度阈值
_EMERGENCY_EXIT_PNL_TH      = -0.05      # 保证金浮亏阈值（-5% ≈ 价格 -1%，在 5x 杠杆下）
_EMERGENCY_EXIT_COOLDOWN_SEC = 600        # 触发后冷却 10 分钟，避免翻来覆去
_last_emergency_exit_ts: float = 0.0


def _check_and_emergency_exit() -> None:
    """检查趋势反转信号，按方案 B 分层平掉"已经明显错了"的反向仓。

    本函数安全地在主循环每轮和 Gemini tick 后各调用一次，
    内部有冷却期，不会重复触发。
    """
    global _last_emergency_exit_ts
    now = time.time()
    if now - _last_emergency_exit_ts < _EMERGENCY_EXIT_COOLDOWN_SEC:
        return

    tech_bl = bool(_BTC_REGIME.get("block_long",  False))
    tech_bs = bool(_BTC_REGIME.get("block_short", False))
    gm = _BTC_GEMINI_REGIME
    gm_ts = float(gm.get("ts", 0) or 0)
    if now - gm_ts > _BTC_GEMINI_STALE_SEC:
        return  # Gemini 数据过期，不敢乱动仓
    gm_verdict = str(gm.get("verdict", "NEUTRAL")).upper()
    gm_conf    = float(gm.get("confidence", 0.0) or 0)

    strong_down = tech_bl and gm_verdict == "STRONG_SHORT" and gm_conf >= _EMERGENCY_EXIT_CONF_TH
    strong_up   = tech_bs and gm_verdict == "STRONG_LONG"  and gm_conf >= _EMERGENCY_EXIT_CONF_TH
    if not (strong_down or strong_up):
        return

    target_side  = "LONG" if strong_down else "SHORT"
    regime_label = "STRONG_DOWN" if strong_down else "STRONG_UP"

    try:
        conn = pymysql.connect(**_DB_CFG)
        try:
            with conn.cursor() as c:
                c.execute(
                    """
                    SELECT id, symbol, position_side, entry_price, leverage, source
                    FROM futures_positions
                    WHERE status='open' AND source LIKE 'dimension_trader:%%'
                      AND position_side=%s
                    """,
                    (target_side,)
                )
                rows = c.fetchall()
        finally:
            conn.close()
    except Exception as e:
        logger.warning(f"[EMERGENCY-EXIT] DB query failed: {e}")
        return

    if not rows:
        logger.info(
            f"[EMERGENCY-EXIT] {regime_label} triggered but no open {target_side} position"
        )
        _last_emergency_exit_ts = now
        return

    logger.warning(
        f"[EMERGENCY-EXIT] {regime_label} CONFIRMED  "
        f"(tech_block_{'long' if strong_down else 'short'}=True, "
        f"gemini={gm_verdict}@{gm_conf:.2f})  "
        f"scanning {len(rows)} {target_side} positions..."
    )

    closed, kept, skipped = 0, 0, 0
    for pid, symbol, side, entry_price, leverage, source in rows:
        try:
            live = _get_live_price(symbol)
            if live is None:
                skipped += 1
                logger.info(f"  [EMERGENCY-EXIT] pid={pid} {symbol}: no live price, skip")
                continue
            entry = float(entry_price or 0)
            lev   = float(leverage or 1)
            if entry <= 0:
                skipped += 1
                continue
            # 价格变动率（方向归一化：LONG 涨为正，SHORT 跌为正）
            if side == "LONG":
                price_change = (live - entry) / entry
            else:
                price_change = (entry - live) / entry
            margin_pnl = price_change * lev   # 保证金级浮盈浮亏

            tag = (source or "").replace("dimension_trader:", "")[:28]
            if margin_pnl <= _EMERGENCY_EXIT_PNL_TH:
                logger.warning(
                    f"  [EMERGENCY-EXIT] CLOSE pid={pid} {symbol:<12s} {side:<5s} "
                    f"{tag:<28s} entry={entry:.6f} live={live:.6f} "
                    f"price_chg={price_change*100:+.2f}% margin_pnl={margin_pnl*100:+.2f}%"
                )
                try:
                    close_position(pid, reason=f"emergency_exit_{regime_label.lower()}")
                    closed += 1
                except Exception as e:
                    logger.error(f"  [EMERGENCY-EXIT] close pid={pid} failed: {e}")
            else:
                kept += 1
                logger.info(
                    f"  [EMERGENCY-EXIT] KEEP  pid={pid} {symbol:<12s} {side:<5s} "
                    f"{tag:<28s} margin_pnl={margin_pnl*100:+.2f}% (>{_EMERGENCY_EXIT_PNL_TH*100:.1f}%)"
                )
        except Exception as e:
            logger.error(f"  [EMERGENCY-EXIT] eval pid={pid} err: {e}")

    logger.warning(
        f"[EMERGENCY-EXIT] done: closed={closed} kept={kept} skipped={skipped} "
        f"total={len(rows)}  next trigger available in {_EMERGENCY_EXIT_COOLDOWN_SEC}s"
    )
    _last_emergency_exit_ts = now


def _update_btc_regime(cs_btc1h: list | None = None,
                       cs_btc15m: list | None = None,
                       cs_btc5m: list | None = None) -> None:
    """根据 BTC 5m/15m/1h 三周期计算即时方向闸门。

    触发规则（任一成立即激活）：
      (1) 1h+15m 同向共振  → 稳态确认
      (2) 5m+15m 同向共振  → 快速响应（不等 1h 整点闭合）
    单周期突刺不触发，至少需要两周期同向确认避免噪声误杀。
    """
    global _BTC_REGIME
    try:
        if cs_btc1h is None:
            cs_btc1h  = load_candles_db("BTC/USDT", "1h",  max(_BTC_GATE_1H_N + 2, 8))
        if cs_btc15m is None:
            cs_btc15m = load_candles_db("BTC/USDT", "15m", max(_BTC_GATE_15M_N + 2, 12))
        if cs_btc5m is None:
            cs_btc5m  = load_candles_db("BTC/USDT", "5m",  max(_BTC_GATE_5M_N + 2, 10))

        if (len(cs_btc1h) < _BTC_GATE_1H_N
                or len(cs_btc15m) < _BTC_GATE_15M_N
                or len(cs_btc5m) < _BTC_GATE_5M_N):
            _BTC_REGIME.update({
                "block_long": False, "block_short": False,
                "reason": (f"not_enough_data 1h={len(cs_btc1h)} "
                           f"15m={len(cs_btc15m)} 5m={len(cs_btc5m)}"),
                "ts": time.time(),
            })
            logger.warning(f"[BTC-GATE] data insufficient, gate disabled")
            return

        g1h  = gradient(cs_btc1h,  _BTC_GATE_1H_N)
        g15m = gradient(cs_btc15m, _BTC_GATE_15M_N)
        g5m  = gradient(cs_btc5m,  _BTC_GATE_5M_N)

        down_1h_15m = (g1h  <= -_BTC_GATE_1H_TH_STRONG)  and (g15m <= -_BTC_GATE_15M_TH_STRONG)
        down_5m_15m = (g5m  <= -_BTC_GATE_5M_TH_STRONG)  and (g15m <= -_BTC_GATE_15M_TH_STRONG)
        up_1h_15m   = (g1h  >= +_BTC_GATE_1H_TH_STRONG)  and (g15m >= +_BTC_GATE_15M_TH_STRONG)
        up_5m_15m   = (g5m  >= +_BTC_GATE_5M_TH_STRONG)  and (g15m >= +_BTC_GATE_15M_TH_STRONG)

        block_long  = down_1h_15m or down_5m_15m
        block_short = up_1h_15m   or up_5m_15m

        def _trig(a_1h15m, a_5m15m):
            if a_1h15m and a_5m15m: return "1h+15m&5m+15m"
            if a_1h15m:             return "1h+15m"
            if a_5m15m:             return "5m+15m"
            return "-"

        stats = f"g5m={g5m*100:+.2f}% g15m={g15m*100:+.2f}% g1h={g1h*100:+.2f}%"
        if block_long:
            reason = f"STRONG_DOWN via {_trig(down_1h_15m, down_5m_15m)}  {stats}  → BLOCK_LONG"
        elif block_short:
            reason = f"STRONG_UP   via {_trig(up_1h_15m,   up_5m_15m)}  {stats}  → BLOCK_SHORT"
        else:
            reason = f"neutral     {stats}"

        _BTC_REGIME.update({
            "block_long":  block_long,
            "block_short": block_short,
            "reason":      reason,
            "ts":          time.time(),
            "g1h":         g1h,
            "g15m":        g15m,
            "g5m":         g5m,
        })
        logger.info(f"[BTC-GATE] {reason}")
    except Exception as e:
        logger.warning(f"_update_btc_regime failed: {e}")


def _load_settings_from_db() -> None:
    """基于 Big4 近6H趋势计算5级 Regime Score，分别控制不同类型策略入场。

    算法（排除 NEUTRAL，仅比较有方向信号的权重比）：
      bull = STRONG_BULLISH*2 + BULLISH
      bear = STRONG_BEARISH*2 + BEARISH
      total_dir = bull + bear

      total_dir < 5    → score=0.0  (中性，信号不足)
      bull/total >= 85% → score=+1.0 (强多)
      bull/total >= 60% → score=+0.5 (轻多)
      bear/total >= 85% → score=-1.0 (强空)
      bear/total >= 60% → score=-0.5 (轻空)
      否则              → score=0.0  (中性)

    策略敏感度分层：
      LONG 全部策略 : score >= -0.5（非强空即允许做多）
      SHORT 全部策略: score <=  0.5（非强多即允许做空）
      具体时机由策略自身与 4h/BTC 条件过滤，不再按 score 分档叠加屏蔽。
    """
    global _ALLOW_LONG, _ALLOW_SHORT, _REGIME_SCORE
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            # 1. 兜底：读取 system_settings
            c.execute(
                "SELECT setting_key, setting_value FROM system_settings "
                "WHERE setting_key IN ('allow_long', 'allow_short')"
            )
            db_rows = {r[0]: r[1] for r in c.fetchall()}
            db_long  = str(db_rows.get("allow_long",  "1")).strip() == "1"
            db_short = str(db_rows.get("allow_short", "1")).strip() == "1"

            # 2. 读取 Big4 近N小时信号分布
            c.execute(
                "SELECT overall_signal, COUNT(*) as cnt FROM big4_trend_history "
                "WHERE created_at >= NOW() - INTERVAL %s HOUR "
                "GROUP BY overall_signal",
                (_BIG4_LOOKBACK_HOURS,)
            )
            b4 = {r[0]: r[1] for r in c.fetchall()}
        conn.close()

        bull = b4.get('STRONG_BULLISH', 0) * 2 + b4.get('BULLISH', 0)
        bear = b4.get('STRONG_BEARISH', 0) * 2 + b4.get('BEARISH', 0)
        total_dir = bull + bear

        if total_dir < _BIG4_MIN_DIR_RECORDS:
            score = 0.0
            regime = f"中性(方向信号不足{total_dir}条)"
        else:
            bp = bull / total_dir
            rp = bear / total_dir
            if bp >= 0.85:
                score, regime = +1.0, f"强多(bull {bp*100:.0f}%  bull={bull} bear={bear})"
            elif bp >= 0.60:
                score, regime = +0.5, f"轻多(bull {bp*100:.0f}%  bull={bull} bear={bear})"
            elif rp >= 0.85:
                score, regime = -1.0, f"强空(bear {rp*100:.0f}%  bull={bull} bear={bear})"
            elif rp >= 0.60:
                score, regime = -0.5, f"轻空(bear {rp*100:.0f}%  bull={bull} bear={bear})"
            else:
                score, regime = 0.0, f"中性(bull {bp*100:.0f}% bear {rp*100:.0f}%  dir={total_dir})"

        _REGIME_SCORE = score
        # Big4 仅在"强反向"时屏蔽；其余情形由策略本身 + 4h 过滤决定
        _ALLOW_LONG   = score >= -0.5   # 非强空都允许做多
        _ALLOW_SHORT  = score <=  0.5   # 非强多都允许做空（不再屏蔽中性/轻多）
        logger.info(
            f"[BIG4-REGIME] {regime} | score={score:+.1f}"
            f" allow_long={_ALLOW_LONG} allow_short={_ALLOW_SHORT}"
        )
    except Exception as e:
        # Big4数据不可用，退化到 system_settings
        try:
            _ALLOW_LONG, _ALLOW_SHORT = db_long, db_short
            logger.warning(f"_load_settings_from_db Big4 failed, fallback to DB: {e}")
        except Exception:
            logger.warning(f"_load_settings_from_db failed, keeping current values: {e}")


def _load_strategy_params_from_db() -> None:
    """从 strategy_params 表加载参数，覆盖运行时 STRATEGY_PARAMS。"""
    global STRATEGY_PARAMS, _params_last_reload
    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute("SELECT strategy_name, sl_pct, tp_pct, hold_h FROM strategy_params")
            rows = c.fetchall()
        conn.close()
        loaded = {r[0]: {"sl_pct": float(r[1]), "tp_pct": float(r[2]), "hold_h": int(r[3])} for r in rows}
        STRATEGY_PARAMS = {**_STRATEGY_PARAMS_DEFAULT, **loaded}
        _params_last_reload = time.time()
        logger.info(f"strategy_params loaded from DB: {len(loaded)} rows, total={len(STRATEGY_PARAMS)}")
    except Exception as e:
        logger.warning(f"strategy_params DB load failed, using defaults: {e}")
    _load_settings_from_db()
    _load_alien5_registry()
    _load_gemini_registry()


_ALIEN5_LONG:  list = []   # [(fn, name), ...]  按 test_wr 降序
_ALIEN5_SHORT: list = []


def _load_alien5_registry() -> None:
    """枚举 auto_explore_alien5 所有候选策略，筛出 DB 里 source='auto_explore_alien5'
    的已部署策略，按 backtest_wr 降序构建 LONG / SHORT 注册表供 compute_signal 调用。

    策略命名遵循 `{Theme}_{L|S}_...` 约定，方向从策略名中的 `_L_` / `_S_` 解析。
    """
    global _ALIEN5_LONG, _ALIEN5_SHORT
    try:
        import auto_explore_alien5 as _alien5
    except Exception as e:
        logger.warning(f"alien5 import failed, skip integration: {e}")
        _ALIEN5_LONG, _ALIEN5_SHORT = [], []
        return

    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute(
                "SELECT strategy_name, backtest_wr FROM strategy_params "
                "WHERE source = 'auto_explore_alien5'"
            )
            db_rows = {r[0]: (float(r[1]) if r[1] is not None else 0.0) for r in c.fetchall()}
        conn.close()
    except Exception as e:
        logger.warning(f"alien5 DB load failed: {e}")
        _ALIEN5_LONG, _ALIEN5_SHORT = [], []
        return

    if not db_rows:
        _ALIEN5_LONG, _ALIEN5_SHORT = [], []
        logger.info("alien5 registry: 0 strategies in DB (尚未部署)")
        return

    candidates: dict[str, tuple] = {}
    for _theme_name, theme_fn in getattr(_alien5, "EXPLORATION_THEMES", []):
        try:
            for strat in theme_fn():
                nm = strat.get("name")
                if not nm or nm not in db_rows:
                    continue
                if "_L_" in nm:
                    direction = "LONG"
                elif "_S_" in nm:
                    direction = "SHORT"
                else:
                    continue
                candidates[nm] = (strat["fn"], direction)
        except Exception as _ex:
            logger.warning(f"alien5 theme {_theme_name} enumerate failed: {_ex}")

    ranked = sorted(candidates.items(), key=lambda kv: -db_rows.get(kv[0], 0.0))
    _ALIEN5_LONG  = [(fn, nm) for nm, (fn, d) in ranked if d == "LONG"]
    _ALIEN5_SHORT = [(fn, nm) for nm, (fn, d) in ranked if d == "SHORT"]
    logger.info(
        f"alien5 registry loaded: {len(_ALIEN5_LONG)} LONG + {len(_ALIEN5_SHORT)} SHORT "
        f"(DB rows={len(db_rows)})"
    )


# ── Gemini theme-probe registry ────────────────────────────────────────────────
# 由 gemini_theme_probe.py 通过 Gemini 与原语对话产生的策略，每次运行把通过四阶段
# 验证的信号函数写入 gemini_signals/*.py（每个主题一个模块，含 STRATEGIES 列表），
# 并把参数 + source='gemini_theme_probe' 入 strategy_params。本函数每次 DB 热加载
# 时被调用，从目录 + DB 交叉构建注册表。
_GEMINI_LONG:  list = []   # [(fn, name), ...]  按 backtest_wr 降序
_GEMINI_SHORT: list = []


def _load_gemini_registry() -> None:
    """从 gemini_signals/ 目录 + DB 构建 Gemini 策略注册表。

    约定：
      gemini_signals/<theme_slug>.py 中暴露一个 `STRATEGIES` 列表，
      每个元素形如 `{"name": str, "fn": callable, "direction": "LONG"/"SHORT"}`。
      fn 接受 (cs1h, cs4h) 返回 "LONG" / "SHORT" / None。
    """
    import importlib
    import pkgutil

    global _GEMINI_LONG, _GEMINI_SHORT
    _GEMINI_LONG, _GEMINI_SHORT = [], []

    try:
        conn = pymysql.connect(**_DB_CFG)
        with conn.cursor() as c:
            c.execute(
                "SELECT strategy_name, backtest_wr FROM strategy_params "
                "WHERE source LIKE 'gemini_%'"
            )
            db_rows = {r[0]: (float(r[1]) if r[1] is not None else 0.0) for r in c.fetchall()}
        conn.close()
    except Exception as e:
        logger.warning(f"gemini DB load failed: {e}")
        return

    if not db_rows:
        logger.info("gemini registry: 0 strategies in DB (尚未部署)")
        return

    try:
        import gemini_signals as _pkg
    except Exception as e:
        logger.warning(f"gemini_signals package import failed: {e}")
        return

    candidates: dict[str, tuple] = {}
    pkg_path = getattr(_pkg, "__path__", [])
    for mod_info in pkgutil.iter_modules(pkg_path):
        mod_name = mod_info.name
        try:
            mod = importlib.import_module(f"gemini_signals.{mod_name}")
            importlib.reload(mod)   # 确保热加载最新代码
        except Exception as ex:
            logger.warning(f"gemini_signals.{mod_name} import failed: {ex}")
            continue
        for strat in getattr(mod, "STRATEGIES", []) or []:
            nm = strat.get("name")
            fn = strat.get("fn")
            direction = strat.get("direction", "").upper()
            if not nm or not callable(fn) or direction not in ("LONG", "SHORT"):
                continue
            if nm not in db_rows:
                continue
            candidates[nm] = (fn, direction)

    ranked = sorted(candidates.items(), key=lambda kv: -db_rows.get(kv[0], 0.0))
    _GEMINI_LONG  = [(fn, nm) for nm, (fn, d) in ranked if d == "LONG"]
    _GEMINI_SHORT = [(fn, nm) for nm, (fn, d) in ranked if d == "SHORT"]
    logger.info(
        f"gemini registry loaded: {len(_GEMINI_LONG)} LONG + {len(_GEMINI_SHORT)} SHORT "
        f"(DB rows={len(db_rows)})"
    )


def _strat_params(strategy: str) -> dict:
    """
    按策略名查找优化参数。
    用户指令：所有通过的策略不再跑参数优化，统一系统默认 2%SL/3%TP/3h。
    - DB 已登记的策略走 DB 值（也已全量统一为默认）
    - 未登记则直接返回 _UNIFIED_DEFAULT
    """
    if strategy in STRATEGY_PARAMS:
        return STRATEGY_PARAMS[strategy]
    return dict(_UNIFIED_DEFAULT)


CANDLE_LOAD_1H = 30
CANDLE_LOAD_4H = 15

# D1a + D4b 仅跑 Big4
BIG4 = ["BTC/USDT", "ETH/USDT", "BNB/USDT", "SOL/USDT"]

# D3-AltLag 精选 41 个山寨（30天回测 64.7% 胜率，筛选自 top-100 市值）
D3_SYMBOLS = [
    "SUPER/USDT", "HBAR/USDT", "LPT/USDT", "XLM/USDT", "AR/USDT",
    "ALGO/USDT", "ID/USDT", "1000FLOKI/USDT", "CFX/USDT", "ORDI/USDT",
    "PYTH/USDT", "LTC/USDT", "1000SHIB/USDT", "FIL/USDT", "APT/USDT",
    "1000LUNC/USDT", "STRK/USDT", "DOT/USDT", "SEI/USDT", "IMX/USDT",
    "FET/USDT", "NEIRO/USDT", "ENS/USDT", "AXL/USDT", "ARKM/USDT",
    "CHZ/USDT", "PENGU/USDT", "NEAR/USDT", "CAKE/USDT", "WLD/USDT",
    "ETHFI/USDT", "ZEC/USDT", "AAVE/USDT", "TRB/USDT", "RENDER/USDT",
    "THETA/USDT", "OP/USDT", "TIA/USDT", "POL/USDT", "LDO/USDT",
    "STX/USDT",
]

# E1-DecelFluxShort + E2-MTFDivergentExhaust 跑全量（BIG4 + D3_SYMBOLS）
# E3-AltDipRecovery 精选 20 个山寨（训练测试均有正向 EV 的高胜率标的）
E3_SYMBOLS = [
    "1000LUNC/USDT", "ICP/USDT", "BCH/USDT", "1000PEPE/USDT", "COMP/USDT",
    "AAVE/USDT", "SUPER/USDT", "HYPE/USDT", "ETC/USDT", "LTC/USDT",
    "ARB/USDT", "UNI/USDT", "NEAR/USDT", "CAKE/USDT", "SOL/USDT",
    "ETH/USDT", "BNB/USDT", "APT/USDT", "FET/USDT", "LINK/USDT",
]

# E16-E30 DecelBounce LONG family 覆盖的全集（含 Big4），不含 BTC 自身（仅提供宏观）
ALT99 = [
    "ETH/USDT","SOL/USDT","ZEC/USDT","XRP/USDT","DOGE/USDT","HYPE/USDT",
    "1000PEPE/USDT","BNB/USDT","TAO/USDT","ENJ/USDT","ADA/USDT","ENA/USDT",
    "AVAX/USDT","LINK/USDT","SUI/USDT","DOT/USDT","WLD/USDT","AAVE/USDT",
    "NEAR/USDT","FIL/USDT","LTC/USDT","BCH/USDT","UNI/USDT","TRX/USDT",
    "1000SHIB/USDT","PENGU/USDT","FET/USDT","CRV/USDT","1000BONK/USDT",
    "APT/USDT","WIF/USDT","VIRTUAL/USDT","LDO/USDT","GALA/USDT","TON/USDT",
    "HBAR/USDT","NEIRO/USDT","ARB/USDT","ONDO/USDT","XLM/USDT","ALGO/USDT",
    "OP/USDT","RENDER/USDT","ETC/USDT","JTO/USDT","DRIFT/USDT","ORDI/USDT",
    "TRU/USDT","XMR/USDT","CAKE/USDT","ONT/USDT","TIA/USDT","DUSK/USDT",
    "AXS/USDT","ICP/USDT","ZRO/USDT","POL/USDT","CHZ/USDT","ATOM/USDT",
    "SEI/USDT","BLUR/USDT","INJ/USDT","BOME/USDT","SAND/USDT","ETHFI/USDT",
    "STRK/USDT","CTSI/USDT","PENDLE/USDT","EDU/USDT","JUP/USDT",
    "W/USDT","IMX/USDT","SUPER/USDT","ID/USDT","SNX/USDT",
    "COMP/USDT","IOTA/USDT","VET/USDT","ANKR/USDT","ROSE/USDT","XTZ/USDT",
    "1000LUNC/USDT","PYTH/USDT","ARKM/USDT","APE/USDT",
]

# 主循环扫描的全集（BTC 仅提供 4h 宏观，不做 D3/E3 信号）
_d3_set   = set(D3_SYMBOLS)
_e3_set   = set(E3_SYMBOLS)
_long_set = set(BIG4) | set(ALT99)   # LONG 策略覆盖范围（Big4 + ALT99）
_all_set  = set(BIG4) | set(D3_SYMBOLS) | set(E3_SYMBOLS) | set(ALT99)
ALL_SYMBOLS = (
    BIG4
    + [s for s in D3_SYMBOLS if s not in set(BIG4)]
    + [s for s in E3_SYMBOLS if s not in set(BIG4) | set(D3_SYMBOLS)]
    + [s for s in ALT99 if s not in set(BIG4) | set(D3_SYMBOLS) | set(E3_SYMBOLS)]
)

SESSION_DIR = Path(__file__).parent / "discovery_sessions"
SESSION_DIR.mkdir(exist_ok=True)

_DB_CFG = {
    "host":      os.getenv("DB_HOST", "localhost"),
    "port":      int(os.getenv("DB_PORT", 3306)),
    "user":      os.getenv("DB_USER", "root"),
    "password":  os.getenv("DB_PASSWORD", ""),
    "database":  os.getenv("DB_NAME", "binance-data"),
    "charset":   "utf8mb4",
    "autocommit": True,
}


# ── K线读取 ────────────────────────────────────────────────────────────────────

def load_candles_db(symbol: str, timeframe: str, limit: int) -> list[dict]:
    conn = pymysql.connect(**_DB_CFG)
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT timestamp, open_price, high_price, low_price, close_price,
                       volume, taker_buy_base_volume
                FROM kline_data
                WHERE symbol=%s AND timeframe=%s
                  AND taker_buy_base_volume IS NOT NULL AND volume > 0
                ORDER BY timestamp DESC LIMIT %s
            """, (symbol, timeframe, limit))
            rows = cur.fetchall()
    finally:
        conn.close()
    rows = list(reversed(rows))
    return [{"t": r[0], "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]),
             "vol": float(r[5]), "buy_vol": float(r[6])} for r in rows]


# ── 指标工具 ───────────────────────────────────────────────────────────────────

def _pround(value: float, ref: float) -> float:
    """Price-aware rounding: prevents round(0.000062, 4)==0.0001 for micro-price tokens."""
    import math
    if ref <= 0 or value <= 0:
        return round(value, 8)
    mag = math.floor(math.log10(ref))
    return round(value, max(4, -mag + 3))


def gradient(cs: list, n: int) -> float:
    if len(cs) < n: return 0.0
    s = sum(c["close"] - c["open"] for c in cs[-n:])
    ref = cs[-1]["close"]
    return s / ref if ref else 0.0

def amplitude(cs: list, n: int) -> float:
    if len(cs) < n: return 0.0
    avg = sum(c["high"] - c["low"] for c in cs[-n:]) / n
    ref = cs[-1]["close"]
    return avg / ref if ref else 0.0

def flux(cs: list, n: int) -> float:
    if len(cs) < n: return 0.5
    rs = [c["buy_vol"] / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def sl_tp_from(cs: list) -> tuple[float, float]:
    amp = amplitude(cs, 6)
    amp = max(SL_MIN, min(SL_MAX, amp))
    return amp * SL_MULT, amp * TP_MULT


# ── Alien 非人类原语（auto_explore_alien.py 使用的同名函数，私有版本）──────────────

def _sell_saturation(cs: list, n: int) -> float:
    """卖方饱和度: 最近n根卖方成交量占比 = 1 - avg(buy_vol/vol), [0,1]"""
    if len(cs) < n: return 0.5
    rs = [(c["vol"] - c["buy_vol"]) / c["vol"] for c in cs[-n:] if c["vol"] > 0]
    return sum(rs) / len(rs) if rs else 0.5

def _spatial_close(cs: list, n: int) -> float:
    """空间收盘得分: avg((close-low)/(high-low)) over n bars, [0,1]"""
    if len(cs) < n: return 0.5
    scores = []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        scores.append((c["close"] - c["low"]) / rng if rng > 0 else 0.5)
    return sum(scores) / len(scores) if scores else 0.5

def _momentum_ratio(cs: list, short_n: int, long_n: int) -> float:
    """动量比值: gradient(short_n) / gradient(long_n), 衰减时 < 1"""
    if len(cs) < long_n + 2: return 1.0
    g_s = sum(c["close"] - c["open"] for c in cs[-short_n:])
    g_l = sum(c["close"] - c["open"] for c in cs[-long_n:])
    ref = cs[-1]["close"]
    if ref <= 0: return 1.0
    gs = g_s / ref; gl = g_l / ref
    if abs(gl) < 1e-9: return 0.0 if abs(gs) < 1e-9 else 2.0
    return gs / gl

def _cross_residual(cs_alt: list, cs_ref: list, n: int) -> float:
    """超额动量: gradient_alt(n) - gradient_ref(n), 正=独立强, 负=滞后"""
    def _g(cs: list, k: int) -> float:
        if len(cs) < k: return 0.0
        s = sum(c["close"] - c["open"] for c in cs[-k:])
        ref = cs[-1]["close"]
        return s / ref if ref else 0.0
    return _g(cs_alt, n) - _g(cs_ref, n)


# ── Batch2 新原语 ──────────────────────────────────────────────────────────────

def _price_memory(cs: list, n: int) -> float:
    """价格记忆系数: (close - n根最低) / (n根最高 - n根最低), [0,1]
    1.0=当前价在近期最高附近; 0.0=当前价在近期最低附近"""
    if len(cs) < n: return 0.5
    hi = max(c["high"] for c in cs[-n:])
    lo = min(c["low"]  for c in cs[-n:])
    rng = hi - lo
    if rng <= 0: return 0.5
    return (cs[-1]["close"] - lo) / rng

def _saturation_velocity(cs: list, n: int, lag: int) -> float:
    """饱和度速率: sell_saturation(now,n) - sell_saturation(lag bars ago,n)
    正值=卖压在快速上升(空头); 负值=卖压快速下降(多头投降)"""
    if len(cs) < n + lag: return 0.0
    return _sell_saturation(cs, n) - _sell_saturation(cs[:-lag], n)

def _time_pressure(cs: list, n: int, amp_th: float) -> float:
    """时间压力: 最近n根中振幅<amp_th的蜡烛占比
    高值(>0.7)=长期低波动压缩,即将爆发"""
    if len(cs) < n: return 0.5
    return sum(
        1 for c in cs[-n:]
        if c["close"] > 0 and (c["high"] - c["low"]) / c["close"] < amp_th
    ) / n

def _flux_momentum(cs: list, short_n: int, long_n: int) -> float:
    """流量动量: flux(short_n) - flux(long_n)
    正值=近期买压比长期均值更高; 负值=买压在消退"""
    if len(cs) < long_n: return 0.0
    return flux(cs, short_n) - flux(cs, long_n)

def _order_flow_delta(cs: list, n: int) -> float:
    """订单净流量: sum(buy_vol - sell_vol) / sum(vol) over n bars
    正值->净买方主导; 负值->净卖方主导"""
    if len(cs) < n: return 0.0
    net   = sum(c["buy_vol"] - (c["vol"] - c["buy_vol"]) for c in cs[-n:])
    total = sum(c["vol"] for c in cs[-n:])
    return net / total if total > 0 else 0.0

def _vol_momentum(cs: list, n: int, lag: int) -> float:
    """成交量动量: avg_vol(now,n) / avg_vol(lag bars ago,n) - 1
    正值->量能放大; 负值->量能萎缩"""
    if len(cs) < n + lag: return 0.0
    v_now  = sum(c["vol"] for c in cs[-n:]) / n
    v_past = sum(c["vol"] for c in cs[-n-lag:-lag]) / n
    return (v_now / v_past - 1) if v_past > 0 else 0.0

def _close_consistency(cs: list, n: int) -> float:
    """收盘一致性: 最近n根中收盘价偏上半区(>50%)的比例
    高值->持续收在上半(看涨); 低值->持续收在下半(看跌)"""
    if len(cs) < n: return 0.5
    count = 0.0
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0:
            count += 0.5
        elif (c["close"] - c["low"]) / rng > 0.5:
            count += 1.0
    return count / n

def _amplitude_skew(cs: list, n: int) -> float:
    """振幅偏斜度: avg(上影线/range) - avg(下影线/range)
    正值->上影线更大(上方承压); 负值->下影线更大(下方支撑)"""
    if len(cs) < n: return 0.0
    upper, lower = [], []
    for c in cs[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0: continue
        bt = max(c["open"], c["close"])
        bb = min(c["open"], c["close"])
        upper.append((c["high"] - bt) / rng)
        lower.append((bb - c["low"])  / rng)
    if not upper: return 0.0
    return sum(upper) / len(upper) - sum(lower) / len(lower)

def _price_velocity(cs: list, n: int, amp_n: int) -> float:
    """价格速度(振幅归一化动量): gradient(n) / amplitude(amp_n)"""
    if len(cs) < max(n, amp_n) + 2: return 0.0
    amp = amplitude(cs, amp_n)
    return gradient(cs, n) / amp if amp > 0 else 0.0

def _entropy_velocity(cs: list, n: int, lag: int) -> float:
    """熵速率: body_entropy(now) - body_entropy(lag bars ago)
    负值->市场在变有序(方向共识形成,即将突破)"""
    import math as _math
    if len(cs) < n + lag: return 0.0
    def _bent(window):
        if len(window) < 2: return 0.0
        up = sum(1 for c in window if c["close"] >= c["open"]) / len(window)
        dn = 1.0 - up
        if up <= 0 or dn <= 0: return 0.0
        return -(_math.log(up) * up + _math.log(dn) * dn)
    return _bent(cs[-n:]) - _bent(cs[-n-lag:-lag])


# ── 3个信号函数（参数冻结）────────────────────────────────────────────────────

def sig_D1a(cs1h: list, cs4h: list) -> bool:
    """4h宏观下行 + 1h 3连阳振幅+成交量双衰减"""
    if len(cs1h) < 5 or len(cs4h) < 3:
        return False
    if gradient(cs4h, 3) >= -0.004:
        return False
    if not all(c["close"] > c["open"] for c in cs1h[-3:]):
        return False
    a = [cs1h[-i]["high"] - cs1h[-i]["low"] for i in range(1, 4)]
    v = [cs1h[-i]["vol"] for i in range(1, 4)]
    return a[2] > a[1] > a[0] and v[2] > v[1] > v[0]

def sig_D4b(cs1h: list) -> bool:
    """3连阳 + 买压比率严格↓ + 最新买压 < 47%"""
    if len(cs1h) < 5:
        return False
    if not all(c["close"] > c["open"] for c in cs1h[-3:]):
        return False
    def bf(c): return c["buy_vol"] / c["vol"] if c["vol"] > 0 else 0.5
    f = [bf(cs1h[-i]) for i in range(1, 4)]
    return f[2] > f[1] > f[0] and f[0] < 0.47

def sig_D3(cs_alt: list, cs_btc4h: list) -> bool:
    """BTC 4h下行 + 山寨局部反弹 + 山寨买压弱"""
    if len(cs_alt) < 7 or len(cs_btc4h) < 4:
        return False
    if gradient(cs_btc4h, 4) >= -0.005:
        return False
    if gradient(cs_alt, 6) <= 0.003:
        return False
    if flux(cs_alt, 3) >= 0.55:
        return False
    return True


# ── AI 探索发现的新信号（strategy_explorer.py，走时验证通过）────────────────────

def sig_E1(cs1h: list, cs4h: list) -> bool:
    """E1-DecelFluxShort: 1h梯度减速 + flux衰减 + 4h宏观不强 => SHORT
    训练 66.0% / 测试 62.3%  n=1072+369  覆盖全量标的"""
    if len(cs1h) < 7 or len(cs4h) < 7:
        return False
    g1h_3 = gradient(cs1h, 3)
    g1h_6 = gradient(cs1h, 6)
    f1h_3 = flux(cs1h, 3)
    f1h_6 = flux(cs1h, 6)
    g4h_6 = gradient(cs4h, 6)
    return (g1h_3 > 0.002
            and g1h_3 < g1h_6 * 0.95
            and f1h_3 < f1h_6 * 0.98
            and g4h_6 < 0.001)


def sig_E2(cs1h: list, cs4h: list) -> str | None:
    """E2-MTFDivergentExhaust: 4h宏观加速 + 1h逆势 + 买压背离 => SHORT/LONG
    训练 68.8% / 测试 62.0%  n=736+337  覆盖全量标的"""
    if len(cs1h) < 7 or len(cs4h) < 7:
        return None
    g4h_3 = gradient(cs4h, 3)
    g4h_6 = gradient(cs4h, 6)
    g1h_3 = gradient(cs1h, 3)
    f1h_3 = flux(cs1h, 3)
    # 4h宏观加速下行 + 1h假反弹 + 买压弱
    if g4h_6 < -0.003 and g4h_3 < g4h_6 * 0.8 and g1h_3 > 0.001 and f1h_3 < 0.48:
        return "SHORT"
    # 4h宏观加速上行 + 1h回调 + 买压仍强
    if g4h_6 > 0.003 and g4h_3 > g4h_6 * 0.8 and g1h_3 < -0.001 and f1h_3 > 0.52:
        return "LONG"
    return None


def sig_E3(cs_alt: list, cs_btc4h: list) -> bool:
    """E3-AltDipRecovery: BTC 4h强上行 + 山寨急跌+高振幅 + flux回升 => LONG
    训练 63.7% / 测试 57.0%  n=1445+881  仅精选标的"""
    if len(cs_alt) < 7 or len(cs_btc4h) < 7:
        return False
    g_btc_6 = gradient(cs_btc4h, 6)
    g_alt_3 = gradient(cs_alt, 3)
    f_alt_3 = flux(cs_alt, 3)
    f_alt_6 = flux(cs_alt, 6)
    a_alt_3 = amplitude(cs_alt, 3)
    return (g_btc_6 > 0.005
            and g_alt_3 < -0.003
            and a_alt_3 > 0.007
            and f_alt_3 > 0.45
            and f_alt_3 > f_alt_6)


def sig_E4(cs1h: list, cs4h: list) -> bool:
    """E4-KineticEfficiencyShort: 4h下行 + 1h反弹但能效比衰减>30% + 买压弱 => SHORT
    能效比 = |gradient(n)| / (amplitude(n) + eps)，衰减代表分布/耗竭
    训练 63.0% / 测试 60.7%  n=670+313  覆盖全量标的"""
    if len(cs1h) < 10 or len(cs4h) < 7:
        return False
    eff_short = abs(gradient(cs1h, 3)) / (amplitude(cs1h, 3) + 1e-6)
    eff_long  = abs(gradient(cs1h, 7)) / (amplitude(cs1h, 7) + 1e-6)
    return (gradient(cs4h, 4) < -0.005
            and gradient(cs1h, 3) > 0.002
            and eff_short < eff_long * 0.7
            and flux(cs1h, 3) < 0.48)


def sig_E5(cs1h: list, cs4h: list) -> bool:
    """E5-TripleFluxEntropy: 4h宏观下行 + 1h价格小涨 + 三级买压严格阶梯递减 => SHORT
    多头力量在价格上涨中逐级崩塌的非线性特征
    训练 63.2% / 测试 65.0%  n=620+223  覆盖全量标的"""
    if len(cs1h) < 7 or len(cs4h) < 5:
        return False
    f2 = flux(cs1h, 2)
    f4 = flux(cs1h, 4)
    f6 = flux(cs1h, 6)
    return (gradient(cs4h, 4) < -0.003
            and gradient(cs1h, 2) > 0.001
            and f2 < f4 < f6
            and f2 < 0.47)


def sig_E6(cs1h: list, cs4h: list) -> bool:
    """E6-InertialFrictionShort: 4h宏观下行 + 1h微涨 + K线实体占比低 + 高买压后转头 => SHORT
    高买压却低位移，说明市场摩擦力极大，上涨已触碰天花板
    训练 62.8% / 测试 63.1%  n=541+255  覆盖全量标的"""
    if len(cs1h) < 10 or len(cs4h) < 5:
        return False

    def body_ratio(k: dict) -> float:
        r = k["high"] - k["low"]
        return abs(k["close"] - k["open"]) / r if r > 0 else 0.0

    recent_br = (body_ratio(cs1h[-1]) + body_ratio(cs1h[-2])) / 2
    return (gradient(cs4h, 4) < -0.003
            and gradient(cs1h, 3) > 0.001
            and recent_br < 0.22
            and flux(cs1h, 2) > 0.48
            and flux(cs1h, 1) < flux(cs1h, 3))


def sig_E7(cs1h: list, cs4h_btc: list) -> bool:
    """E7-PassiveSupplySuffocation: BTC 4h下行 + 山寨高flux反弹但单位买压涨幅效率暴跌 => SHORT
    主动买盘被宏观趋势下的被动卖盘（限价单）窒息
    训练 68.0% / 测试 62.0%  n=878+531  mtf_btc模式"""
    if len(cs1h) < 10 or len(cs4h_btc) < 8:
        return False
    flux_excess = flux(cs1h, 2) - 0.5
    if flux_excess <= 0.01:
        return False
    reward_now  = gradient(cs1h, 2) / flux_excess
    prev_excess = max(0.01, flux(cs1h, 8) - 0.5)
    reward_prev = gradient(cs1h, 8) / prev_excess
    return (gradient(cs4h_btc, 8) < -0.005
            and flux(cs1h, 2) > 0.55
            and reward_now < reward_prev * 0.4)


def sig_E8(cs1h: list, cs4h: list) -> bool:
    """E8-HollowGrowthShort: 4h宏观下行 + 1h小K线漂移上涨 + 成交量载荷骤降 + flux低 => SHORT
    缺乏能量支撑的价格漂移，典型多头陷阱
    训练 65.6% / 测试 60.9%  n=244+64  覆盖全量标的"""
    if len(cs1h) < 10 or len(cs4h) < 6:
        return False
    amp_last = amplitude(cs1h, 1)
    vol_load_now = cs1h[-1]["vol"] / (amp_last + 1e-9)
    amp_sum_avg  = sum(amplitude(cs1h, i) for i in range(2, 6)) + 1e-9
    vol_load_avg = sum(cs1h[-i]["vol"] for i in range(2, 6)) / amp_sum_avg
    return (gradient(cs4h, 6) < -0.005
            and gradient(cs1h, 3) > 0.001
            and vol_load_now < vol_load_avg * 0.6
            and flux(cs1h, 2) < 0.46)


def sig_E9(cs1h: list, cs4h: list) -> bool:
    """E9-WickSaturationShort: 4h宏观下行 + 1h反弹 + 上影线>实体1.5倍 + flux<0.49 => SHORT
    上影线密集饱和，多头向上尝试被限价卖单完全吸收
    训练 62.2% / 测试 62.9%  n=529+280  覆盖全量标的"""
    if len(cs1h) < 5 or len(cs4h) < 5:
        return False
    upper_wicks = sum(
        cs1h[-i]["high"] - max(cs1h[-i]["open"], cs1h[-i]["close"])
        for i in range(1, 4)
    )
    bodies = sum(abs(cs1h[-i]["close"] - cs1h[-i]["open"]) for i in range(1, 4))
    return (gradient(cs4h, 4) < -0.005
            and gradient(cs1h, 3) > 0.001
            and upper_wicks > bodies * 1.5
            and flux(cs1h, 2) < 0.49)


def sig_E10(cs1h: list, cs4h: list) -> bool:
    """E10-FrictionDecouplingShort: 4h下行 + 1h反弹 + flux>0.48但实体<25% + 买压边际减弱 => SHORT
    多头努力被空头头寸完全抵消，上涨动力枯竭
    训练 61.3% / 测试 61.5%  n=1116+512  覆盖全量标的"""
    if len(cs1h) < 10 or len(cs4h) < 6:
        return False
    last = cs1h[-1]
    body_ratio = abs(last["close"] - last["open"]) / (last["high"] - last["low"] + 1e-9)
    return (gradient(cs4h, 6) < -0.004
            and gradient(cs1h, 3) > 0.001
            and flux(cs1h, 2) > 0.48
            and body_ratio < 0.25
            and flux(cs1h, 1) < flux(cs1h, 3))


def sig_E11(cs1h: list, cs4h_btc: list) -> bool:
    """E11-ShadowLaggardShort: BTC 4h下行 + 山寨1h抗跌 + 买压三级阶梯衰减 + f2<0.48 => SHORT
    买压衰减比价格更快，山寨"抗跌"只是惯性滞后
    训练 62.4% / 测试 61.1%  n=643+319  mtf_btc模式"""
    if len(cs1h) < 12 or len(cs4h_btc) < 12:
        return False
    if gradient(cs4h_btc, 8) > -0.005:
        return False
    if gradient(cs1h, 2) < 0.001:
        return False
    f2 = flux(cs1h, 2)
    f5 = flux(cs1h, 5)
    f10 = flux(cs1h, 10)
    return f2 < f5 < f10 and f2 < 0.48


def sig_E12(cs1h: list, cs4h: list) -> bool:
    """E12-NonLinearExhaustion: 4h下行 + 1h反弹 + 振幅放大但购买力转换效率暴跌至<20% => SHORT
    非线性耗竭：价格越来越剧烈但效率趋近于零
    训练 61.6% / 测试 63.3%  n=511+267  覆盖全量标的"""
    if len(cs1h) < 15 or len(cs4h) < 10:
        return False

    def get_conversion(cs: list, n: int) -> float:
        f = flux(cs, n)
        denom = (f - 0.4) if f > 0.4 else 0.05
        return gradient(cs, n) / denom

    conv2 = get_conversion(cs1h, 2)
    conv8 = get_conversion(cs1h, 8)
    return (gradient(cs4h, 8) < -0.006
            and gradient(cs1h, 3) > 0.001
            and conv2 < conv8 * 0.2
            and amplitude(cs1h, 2) > amplitude(cs1h, 8))


def sig_E13(cs1h: list, cs4h: list) -> bool:
    """E13-ConvexityTrapShort: 4h弱势 + 1h反弹 + 买压转换效率<前期30% + flux<0.53 => SHORT
    凸性陷阱：外表看起来买压尚可，实则效率已死
    训练 66.7% / 测试 61.4%  n=579+347  覆盖全量标的"""
    if len(cs1h) < 15 or len(cs4h) < 10:
        return False
    if gradient(cs4h, 8) > -0.003:
        return False

    def get_trans_rate(cs: list, n: int) -> float:
        f = flux(cs, n)
        return gradient(cs, n) / (f - 0.4 + 1e-9)

    rate_now  = get_trans_rate(cs1h, 2)
    rate_prev = get_trans_rate(cs1h, 5)
    return (gradient(cs1h, 2) > 0.001
            and rate_now < rate_prev * 0.3
            and flux(cs1h, 2) < 0.53)


def sig_E14(cs1h: list, cs4h: list) -> bool:
    """E14-InertialDragShort: 4h强弱 + 1h虚涨(实体<22%) + 梯度减速60% => SHORT
    惯性拖拽：价格上涨但内部能量已被拖拽耗尽
    训练 62.0% / 测试 62.0%  n=313+263  覆盖全量标的"""
    if len(cs1h) < 10 or len(cs4h) < 8:
        return False
    if gradient(cs4h, 8) > -0.006:
        return False

    def get_body_ratio(cs: list, n: int) -> float:
        rs = []
        for i in range(1, n + 1):
            r = cs[-i]["high"] - cs[-i]["low"]
            b = abs(cs[-i]["close"] - cs[-i]["open"])
            rs.append(b / r if r > 0 else 0.5)
        return sum(rs) / n

    br = get_body_ratio(cs1h, 2)
    g2 = gradient(cs1h, 2)
    g5 = gradient(cs1h, 5)
    return g2 > 0 and br < 0.22 and g2 < g5 * 0.4


def sig_E15(cs1h: list, cs4h_btc: list) -> bool:
    """E15-ElasticityCollapse: BTC 4h下行 + 山寨1h涨 + 超额买压弹性断崖跌至<30% + flux<0.51 => SHORT
    弹性崩塌：多头买压对价格的推动力急剧丧失
    训练 66.8% / 测试 62.1%  n=301+195  mtf_btc模式"""
    if len(cs1h) < 20 or len(cs4h_btc) < 10:
        return False

    def get_elasticity(cs: list, n: int) -> float:
        f = flux(cs, n)
        excess = max(f - 0.45, 0.01)
        return gradient(cs, n) / excess

    if gradient(cs4h_btc, 6) >= -0.005:
        return False
    e_short = get_elasticity(cs1h, 2)
    e_long  = get_elasticity(cs1h, 10)
    return (gradient(cs1h, 2) > 0.001
            and e_short < e_long * 0.3
            and flux(cs1h, 2) < 0.51)


# ── LONG 策略：DecelBounce 家族 E16-E30（手工验证，60-69% 测试胜率）────────────

def _db_long(cs1h: list, cs4h: list, h_n: int,
             mac_n: int = 4, hist_th: float = 0.002, f_min: float = 0.53,
             amp1_max: float = None, amp1_n: int = 2,
             amp4_min: float = None) -> bool:
    """DecelBounce LONG 核心: 4h上升趋势 + 1h h_n-bar下行 + 2bar反转 + flux达标.
    mac_n:    4h梯度计算窗口
    hist_th:  1h历史下行阈值（正数，内部取负）
    f_min:    flux下限
    amp1_max: 1h振幅上限（None=不过滤）
    amp1_n:   1h振幅窗口
    amp4_min: 4h振幅下限（None=不过滤，用于DB-AmpMacro）
    """
    if len(cs1h) < max(h_n + 3, amp1_n + 1, 10) or len(cs4h) < max(mac_n + 1, 8):
        return False
    if gradient(cs4h, mac_n) <= 0.003:
        return False
    if amp4_min is not None and amplitude(cs4h, 4) < amp4_min:
        return False
    if gradient(cs1h, h_n) >= -hist_th:
        return False
    if gradient(cs1h, 2) <= 0:
        return False
    if flux(cs1h, 2) <= f_min:
        return False
    if amp1_max is not None and amplitude(cs1h, amp1_n) > amp1_max:
        return False
    return True


def sig_E16(cs1h: list, cs4h: list) -> bool:
    """E16-DecelBounce: 4h up + 1h 6-bar decline + bounce + flux>0.53 + amp(3)<0.030
    训练 60.9% / 测试 58.3%  n=465+250  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=6, amp1_max=0.030, amp1_n=3)


def sig_E17(cs1h: list, cs4h: list) -> bool:
    """E17-DecelBounce-Deep: h=8-bar decline (deeper pullback confirmation)
    训练 63.7% / 测试 63.9%  n=651+330  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8)


def sig_E18(cs1h: list, cs4h: list) -> bool:
    """E18-DecelBounce-LowAmp: h=6 + amp(2)<0.018 (quiet orderly bounce)
    训练 60.6% / 测试 59.3%  n=609+301  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=6, amp1_max=0.018)


def sig_E19(cs1h: list, cs4h: list) -> bool:
    """E19-DB-h4: short 4-bar decline
    训练 61.1% / 测试 59.6%  n=465+250  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=4)


def sig_E20(cs1h: list, cs4h: list) -> bool:
    """E20-DB-h5: 5-bar decline
    训练 62.0% / 测试 58.3%  n=540+288  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=5)


def sig_E21(cs1h: list, cs4h: list) -> bool:
    """E21-DB-h7: 7-bar decline
    训练 63.6% / 测试 64.1%  n=621+320  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=7)


def sig_E22(cs1h: list, cs4h: list) -> bool:
    """E22-DB-h10: 10-bar decline (long pullback)
    训练 62.7% / 测试 63.1%  n=679+317  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10)


def sig_E23(cs1h: list, cs4h: list) -> bool:
    """E23-DB-AmpMed: h=6 + amp(2)<0.022 (medium amplitude filter)
    训练 60.7% / 测试 59.0%  n=577+283  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=6, amp1_max=0.022)


def sig_E24(cs1h: list, cs4h: list) -> bool:
    """E24-DB-DeepLowAmp: h=8 + amp(2)<0.018 (deep pullback + quiet bounce)
    训练 63.9% / 测试 64.1%  n=609+301  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, amp1_max=0.018)


def sig_E25(cs1h: list, cs4h: list) -> bool:
    """E25-DB-AmpMacro: h=6 + 4h-amp>2% (amplitude-confirmed macro uptrend)
    训练 62.4% / 测试 59.1%  n=410+198  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=6, amp4_min=0.020)


def sig_E26(cs1h: list, cs4h: list) -> bool:
    """E26-DB-3macroN: 3-bar 4h macro gradient (shorter macro window)
    训练 60.4% / 测试 62.4%  n=639+303  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=6, mac_n=3)


def sig_E27(cs1h: list, cs4h: list) -> bool:
    """E27-DB-h3: 3-bar decline (mini-dip entry)
    训练 60.1% / 测试 57.1%  n=298+112  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=3)


def sig_E28(cs1h: list, cs4h: list) -> bool:
    """E28-DB-h12: 12-bar decline (very deep pullback)
    训练 62.9% / 测试 67.7%  n=671+341  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12)


def sig_E29(cs1h: list, cs4h: list) -> bool:
    """E29-DB-h14: 14-bar decline (extended deep pullback, best test WR)
    训练 60.7% / 测试 69.0%  n=721+406  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14)


def sig_E30(cs1h: list, cs4h: list) -> bool:
    """E30-DB-h7-DeepDecline: h=7 + hist<-0.4% (deeper decline threshold)
    训练 64.0% / 测试 65.3%  n=506+251  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=7, hist_th=0.004)


# ── 信号计算 ───────────────────────────────────────────────────────────────────


# -- 自动部署策略（deploy_strategies.py）-----------------

def sig_E31(cs1h: list, cs4h: list) -> bool:
    """sig_E31-DB_h16
    train=74.3% / test=63.9%  n=1680+2142  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=16)

def sig_E32(cs1h: list, cs4h: list) -> bool:
    """sig_E32-DB_h14_ht3
    train=74.6% / test=63.8%  n=1412+1789  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14, hist_th=0.003)

def sig_E33(cs1h: list, cs4h: list) -> bool:
    """sig_E33-DB_h14_f55
    train=75.0% / test=63.7%  n=632+1040  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14, f_min=0.55)

def sig_E34(cs1h: list, cs4h: list) -> bool:
    """sig_E34-DB_h15
    train=77.5% / test=63.6%  n=1492+2001  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=15)

def sig_E35(cs1h: list, cs4h: list) -> bool:
    """sig_E35-DB_h14_ht4
    train=74.0% / test=63.4%  n=1256+1624  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14, hist_th=0.004)

def sig_E36(cs1h: list, cs4h: list) -> bool:
    """sig_E36-DB_h8_f56
    train=60.2% / test=63.3%  n=615+577  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, f_min=0.56)

def sig_E37(cs1h: list, cs4h: list) -> bool:
    """sig_E37-DB_h14_ht5
    train=73.5% / test=62.9%  n=1113+1486  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14, hist_th=0.005)

def sig_E38(cs1h: list, cs4h: list) -> bool:
    """sig_E38-DB_h14_f56
    train=72.8% / test=62.6%  n=243+522  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14, f_min=0.56)

def sig_E39(cs1h: list, cs4h: list) -> bool:
    """sig_E39-DB_h12_ht4
    train=64.2% / test=62.6%  n=2034+1751  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12, hist_th=0.004)

def sig_E40(cs1h: list, cs4h_btc: list) -> bool:
    """sig_E40-BTCLead_b10_h8_f50: BTC lead alt LONG
    train=58.1% / test=62.3%  n=8727+3200  mtf_btc"""
    if len(cs1h) < 11 or len(cs4h_btc) < 8: return False
    if gradient(cs4h_btc, 4) <= 0.01: return False
    if gradient(cs1h, 8) >= -0.002: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.5: return False
    return True

def sig_E41(cs1h: list, cs4h: list) -> bool:
    """sig_E41-DB_h14_ht6
    train=74.6% / test=62.3%  n=1006+1361  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=14, hist_th=0.006)

def sig_E42(cs1h: list, cs4h: list) -> bool:
    """sig_E42-DB_h12_ht3
    train=64.4% / test=62.2%  n=2255+1940  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12, hist_th=0.003)

def sig_E43(cs1h: list, cs4h: list) -> bool:
    """sig_E43-DB_h12_ht5
    train=64.9% / test=62.2%  n=1835+1572  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12, hist_th=0.005)

def sig_E44(cs1h: list, cs4h: list) -> bool:
    """sig_E44-DB_h12_ht6
    train=64.6% / test=61.9%  n=1670+1446  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12, hist_th=0.006)

def sig_E45(cs1h: list, cs4h: list) -> bool:
    """sig_E45-DB_h10_f56
    train=61.8% / test=61.6%  n=532+552  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10, f_min=0.56)

def sig_E46(cs1h: list, cs4h: list) -> bool:
    """sig_E46-OvrSold_h10_d12_f55: oversold deep bounce LONG
    train=59.6% / test=61.5%  n=515+436  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.012: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.55: return False
    return True

def sig_E47(cs1h: list, cs4h_btc: list) -> bool:
    """sig_E47-BTCLead_b7_h8_f50: BTC lead alt LONG
    train=57.1% / test=61.5%  n=11119+4001  mtf_btc"""
    if len(cs1h) < 11 or len(cs4h_btc) < 8: return False
    if gradient(cs4h_btc, 4) <= 0.007: return False
    if gradient(cs1h, 8) >= -0.002: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.5: return False
    return True

def sig_E48(cs1h: list, cs4h: list) -> bool:
    """sig_E48-DB_h8_f55
    train=59.5% / test=61.4%  n=1563+1208  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, f_min=0.55)

def sig_E49(cs1h: list, cs4h: list) -> bool:
    """sig_E49-DB_h20
    train=62.9% / test=61.4%  n=3386+3354  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=20)

def sig_E50(cs1h: list, cs4h: list) -> bool:
    """sig_E50-OvrSold_h10_d20_f53: oversold deep bounce LONG
    train=57.8% / test=61.3%  n=688+429  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.02: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.53: return False
    return True

def sig_E51(cs1h: list, cs4h: list) -> bool:
    """sig_E51-OvrSold_h10_d10_f55: oversold deep bounce LONG
    train=60.6% / test=61.2%  n=630+523  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.01: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.55: return False
    return True

def sig_E52(cs1h: list, cs4h: list) -> bool:
    """sig_E52-FluxAccel_h10_mac5_f51: flux acceleration LONG
    train=62.8% / test=61.2%  n=9274+5652  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E53(cs1h: list, cs4h: list) -> bool:
    """sig_E53-OvrSold_h10_d15_f55: oversold deep bounce LONG
    train=58.8% / test=61.2%  n=388+327  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.015: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.55: return False
    return True

def sig_E54(cs1h: list, cs4h: list) -> bool:
    """sig_E54-FluxAccel_h10_mac5_f49: flux acceleration LONG
    train=63.8% / test=61.2%  n=17317+8740  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E55(cs1h: list, cs4h: list) -> bool:
    """sig_E55-OvrSold_h10_d8_f55: oversold deep bounce LONG
    train=60.2% / test=61.2%  n=758+641  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.008: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.55: return False
    return True

def sig_E56(cs1h: list, cs4h: list) -> bool:
    """sig_E56-DB_h12_f55
    train=65.4% / test=61.2%  n=1023+1094  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12, f_min=0.55)

def sig_E57(cs1h: list, cs4h: list) -> bool:
    """sig_E57-FluxAccel_h10_mac3_f49: flux acceleration LONG
    train=63.4% / test=61.0%  n=19386+9661  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E58(cs1h: list, cs4h: list) -> bool:
    """sig_E58-DB_h8_ht6
    train=57.1% / test=61.0%  n=2561+1577  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, hist_th=0.006)

def sig_E59(cs1h: list, cs4h: list) -> bool:
    """sig_E59-DB_h10_ht4
    train=60.5% / test=61.0%  n=2731+1917  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10, hist_th=0.004)

def sig_E60(cs1h: list, cs4h: list) -> bool:
    """sig_E60-FluxAccel_h10_mac3_f51: flux acceleration LONG
    train=62.4% / test=60.9%  n=10441+6262  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E61(cs1h: list, cs4h: list) -> bool:
    """sig_E61-FluxAccel_h10_mac2_f49: flux acceleration LONG
    train=63.2% / test=60.9%  n=20568+10162  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E62(cs1h: list, cs4h: list) -> bool:
    """sig_E62-DB_h8_ht5
    train=57.3% / test=60.8%  n=2822+1762  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, hist_th=0.005)

def sig_E63(cs1h: list, cs4h: list) -> bool:
    """sig_E63-OvrSold_h10_d15_f53: oversold deep bounce LONG
    train=57.4% / test=60.8%  n=1030+651  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.015: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.53: return False
    return True

def sig_E64(cs1h: list, cs4h: list) -> bool:
    """sig_E64-FluxAccel_h10_mac2_f51: flux acceleration LONG
    train=62.3% / test=60.7%  n=11089+6581  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E65(cs1h: list, cs4h: list) -> bool:
    """sig_E65-DB_h8_ht3
    train=58.2% / test=60.7%  n=3430+2190  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, hist_th=0.003)

def sig_E66(cs1h: list, cs4h: list) -> bool:
    """sig_E66-OvrSold_h10_d12_f53: oversold deep bounce LONG
    train=59.1% / test=60.6%  n=1340+869  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.012: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.53: return False
    return True

def sig_E67(cs1h: list, cs4h: list) -> bool:
    """sig_E67-DB_h18
    train=64.4% / test=60.6%  n=2666+2844  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=18)

def sig_E68(cs1h: list, cs4h: list) -> bool:
    """sig_E68-DB_h10_ht5
    train=60.1% / test=60.6%  n=2500+1742  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10, hist_th=0.005)

def sig_E69(cs1h: list, cs4h: list) -> bool:
    """sig_E69-DB_h10_ht3
    train=60.7% / test=60.6%  n=3002+2109  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10, hist_th=0.003)

def sig_E70(cs1h: list, cs4h: list) -> bool:
    """sig_E70-OvrSold_h10_d8_f53: oversold deep bounce LONG
    train=59.6% / test=60.5%  n=1912+1285  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.008: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.53: return False
    return True

def sig_E71(cs1h: list, cs4h: list) -> bool:
    """sig_E71-DB_h8_ht4
    train=57.7% / test=60.4%  n=3103+1942  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=8, hist_th=0.004)

def sig_E72(cs1h: list, cs4h: list) -> bool:
    """sig_E72-DB_h10_ht6
    train=59.6% / test=60.3%  n=2293+1566  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10, hist_th=0.006)

def sig_E73(cs1h: list, cs4h: list) -> bool:
    """sig_E73-FluxAccel_h10_mac5_f53: flux acceleration LONG
    train=61.3% / test=60.3%  n=4048+3022  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E74(cs1h: list, cs4h: list) -> bool:
    """sig_E74-OvrSold_h10_d10_f53: oversold deep bounce LONG
    train=60.5% / test=60.2%  n=1595+1044  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.01: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.53: return False
    return True

def sig_E75(cs1h: list, cs4h: list) -> bool:
    """sig_E75-FluxAccel_h8_mac5_f51: flux acceleration LONG
    train=60.5% / test=60.2%  n=11541+6393  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E76(cs1h: list, cs4h: list) -> bool:
    """sig_E76-DB_h10_f55
    train=61.7% / test=60.2%  n=1387+1163  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=10, f_min=0.55)

def sig_E77(cs1h: list, cs4h: list) -> bool:
    """sig_E77-DB_h12_f56
    train=65.2% / test=60.1%  n=399+536  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=12, f_min=0.56)

def sig_E78(cs1h: list, cs4h: list) -> bool:
    """sig_E78-FluxAccel_h8_mac3_f51: flux acceleration LONG
    train=60.1% / test=60.1%  n=12787+7004  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E79(cs1h: list, cs4h: list) -> bool:
    """sig_E79-FluxAccel_h8_mac5_f49: flux acceleration LONG
    train=61.1% / test=60.0%  n=21589+10129  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E80(cs1h: list, cs4h: list) -> bool:
    """sig_E80-FluxAccel_h8_mac2_f51: flux acceleration LONG
    train=60.0% / test=60.0%  n=13455+7342  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E81(cs1h: list, cs4h: list) -> bool:
    """sig_E81-FluxAccel_h10_mac3_f53: flux acceleration LONG
    train=61.0% / test=60.0%  n=4599+3377  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E82(cs1h: list, cs4h: list) -> bool:
    """sig_E82-FluxAccel_h8_mac3_f49: flux acceleration LONG
    train=60.8% / test=60.0%  n=23803+11057  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E83(cs1h: list, cs4h: list) -> bool:
    """sig_E83-FluxAccel_h8_mac2_f49: flux acceleration LONG
    train=60.6% / test=59.9%  n=25059+11573  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E84(cs1h: list, cs4h: list) -> bool:
    """sig_E84-OvrSold_h8_d12_f55: oversold deep bounce LONG
    train=59.2% / test=59.8%  n=573+413  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 8) >= -0.012: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.55: return False
    return True

def sig_E85(cs1h: list, cs4h: list) -> bool:
    """sig_E85-FluxAccel_h10_mac2_f53: flux acceleration LONG
    train=60.8% / test=59.6%  n=4906+3553  mtf_self"""
    if len(cs1h) < 13 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 10) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E86(cs1h: list, cs4h: list) -> bool:
    """sig_E86-FluxAccel_h8_mac3_f53: flux acceleration LONG
    train=58.8% / test=59.3%  n=5569+3741  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E87(cs1h: list, cs4h: list) -> bool:
    """sig_E87-FluxAccel_h8_mac5_f53: flux acceleration LONG
    train=59.0% / test=59.2%  n=4993+3392  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E88(cs1h: list, cs4h: list) -> bool:
    """sig_E88-FluxAccel_h8_mac2_f53: flux acceleration LONG
    train=58.7% / test=59.2%  n=5877+3928  mtf_self"""
    if len(cs1h) < 11 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 8) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E89(cs1h: list, cs4h_btc: list) -> bool:
    """sig_E89-BTCLead_b10_h6_f50: BTC lead alt LONG
    train=57.5% / test=58.9%  n=8937+3032  mtf_btc"""
    if len(cs1h) < 10 or len(cs4h_btc) < 8: return False
    if gradient(cs4h_btc, 4) <= 0.01: return False
    if gradient(cs1h, 6) >= -0.002: return False
    if gradient(cs1h, 2) <= 0: return False
    if flux(cs1h, 2) <= 0.5: return False
    return True

def sig_E90(cs1h: list, cs4h: list) -> bool:
    """sig_E90-DB_h25
    train=61.7% / test=58.7%  n=4821+4505  mtf_self"""
    return _db_long(cs1h, cs4h, h_n=25)

def sig_E91(cs1h: list, cs4h: list) -> bool:
    """sig_E91-FluxAccel_h6_mac5_f49: flux acceleration LONG
    train=60.0% / test=58.3%  n=24434+10908  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E92(cs1h: list, cs4h: list) -> bool:
    """sig_E92-FluxAccel_h6_mac3_f49: flux acceleration LONG
    train=59.7% / test=58.2%  n=26629+11839  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E93(cs1h: list, cs4h: list) -> bool:
    """sig_E93-FluxAccel_h6_mac2_f49: flux acceleration LONG
    train=59.5% / test=58.1%  n=27837+12321  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True

def sig_E94(cs1h: list, cs4h: list) -> bool:
    """sig_E94-FluxAccel_h6_mac5_f53: flux acceleration LONG
    train=57.2% / test=57.8%  n=5523+3530  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.53: return False
    return True

def sig_E95(cs1h: list, cs4h: list) -> bool:
    """sig_E95-FluxAccel_h6_mac3_f51: flux acceleration LONG
    train=58.5% / test=57.8%  n=14160+7373  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.003: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E96(cs1h: list, cs4h: list) -> bool:
    """sig_E96-FluxAccel_h6_mac5_f51: flux acceleration LONG
    train=58.9% / test=57.7%  n=12931+6781  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E97(cs1h: list, cs4h: list) -> bool:
    """sig_E97-FluxAccel_h6_mac2_f51: flux acceleration LONG
    train=58.3% / test=57.6%  n=14801+7684  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if gradient(cs1h, 6) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.51: return False
    return True

def sig_E98(cs1h: list, cs4h: list) -> bool:
    """sig_E98-FluxAccel_h4_mac5_f49: flux acceleration LONG
    train=58.6% / test=57.2%  n=25653+10913  mtf_self"""
    if len(cs1h) < 10 or len(cs4h) < 8: return False
    if gradient(cs4h, 4) <= 0.005: return False
    if gradient(cs1h, 4) >= -0.001: return False
    f2 = flux(cs1h, 2); f4 = flux(cs1h, 4); f8 = flux(cs1h, 8)
    if not (f2 > f4 * 0.97 and f4 > f8 * 0.97): return False
    if f2 <= 0.49: return False
    return True


# ── Alien 系列信号函数（auto_explore_alien.py 四阶段验证通过）─────────────────────

def sig_A1(cs1h: list, cs4h: list) -> bool:
    """A1-SellCapLong: 卖方投降LONG  train=57.6% test=59.5% n=467+420  ev=+0.32%
    4h上行 + 8根卖压>0.55 + 近2根卖压<0.51 => LONG (卖压已耗尽,买方反攻)"""
    if len(cs1h) < 12 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if _sell_saturation(cs1h, 8) <= 0.55: return False
    if _sell_saturation(cs1h, 2) >= 0.51: return False
    return True

def sig_A2(cs1h: list, cs4h: list) -> bool:
    """A2-BuyExhShort: 买方耗竭SHORT  train=58.0% test=58.8% n=333+452  ev=+0.29%
    4h下行 + 4根买压>0.55 + 近2根买压回落 => SHORT (买方推力耗尽,转头做空)"""
    if len(cs1h) < 8 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if _sell_saturation(cs1h, 4) >= 0.45: return False
    if _sell_saturation(cs1h, 2) <= 0.48: return False
    return True

def sig_A3(cs1h: list, cs4h: list) -> bool:
    """A3-MomDecay-l10-r50: 动量衰减SHORT  train=64.1% test=61.7% n=23391+10285  ev=+0.41%
    4h中性 + 1h曾上涨 + 动量比<0.50 + 收盘偏下 => SHORT"""
    if len(cs1h) < 14 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= 0.003: return False
    if gradient(cs1h, 10) <= 0.001: return False
    if _momentum_ratio(cs1h, 2, 10) >= 0.50: return False
    if _spatial_close(cs1h, 3) >= 0.50: return False
    return True

def sig_A4(cs1h: list, cs4h: list) -> bool:
    """A4-MomDecay-l10-r40: 动量衰减SHORT  train=64.1% test=61.6% n=22684+9962  ev=+0.41%"""
    if len(cs1h) < 14 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= 0.003: return False
    if gradient(cs1h, 10) <= 0.001: return False
    if _momentum_ratio(cs1h, 2, 10) >= 0.40: return False
    if _spatial_close(cs1h, 3) >= 0.50: return False
    return True

def sig_A5(cs1h: list, cs4h: list) -> bool:
    """A5-MomDecay-l10-r30: 动量衰减SHORT  train=64.0% test=61.4% n=21766+9572  ev=+0.40%"""
    if len(cs1h) < 14 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= 0.003: return False
    if gradient(cs1h, 10) <= 0.001: return False
    if _momentum_ratio(cs1h, 2, 10) >= 0.30: return False
    if _spatial_close(cs1h, 3) >= 0.50: return False
    return True

def sig_A6(cs1h: list, cs4h: list) -> bool:
    """A6-MomDecay-l8-r50: 动量衰减SHORT  train=61.3% test=59.4% n=25468+10621  ev=+0.36%"""
    if len(cs1h) < 12 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= 0.003: return False
    if gradient(cs1h, 8) <= 0.001: return False
    if _momentum_ratio(cs1h, 2, 8) >= 0.50: return False
    if _spatial_close(cs1h, 3) >= 0.50: return False
    return True

def sig_A7(cs1h: list, cs4h: list) -> bool:
    """A7-MomDecay-l8-r40: 动量衰减SHORT  train=61.2% test=59.3% n=24527+10277  ev=+0.35%"""
    if len(cs1h) < 12 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= 0.003: return False
    if gradient(cs1h, 8) <= 0.001: return False
    if _momentum_ratio(cs1h, 2, 8) >= 0.40: return False
    if _spatial_close(cs1h, 3) >= 0.50: return False
    return True

def sig_A8(cs1h: list, cs4h: list) -> bool:
    """A8-MomDecay-l8-r30: 动量衰减SHORT  train=61.1% test=59.0% n=23420+9829  ev=+0.34%"""
    if len(cs1h) < 12 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= 0.003: return False
    if gradient(cs1h, 8) <= 0.001: return False
    if _momentum_ratio(cs1h, 2, 8) >= 0.30: return False
    if _spatial_close(cs1h, 3) >= 0.50: return False
    return True

def sig_A9(cs1h: list, cs4h: list) -> bool:
    """A9-SpatDivLong: 空间背离LONG  train=57.7% test=57.1% n=31639+14295  ev=+0.29%
    4h上行 + 收盘偏低<0.38 + 卖压不高<0.48 (假跌信号,即将反转) => LONG
    [优化 2026-04-16] 收紧条件减少过度触发(30笔/7天 -> 目标10笔/7天):
      gradient: >0.001 -> >0.002 (4H趋势更明确)
      spatial_close: <0.43 -> <0.38 (收盘更深度偏低)
      sell_saturation: <0.52 -> <0.48 (卖压更明确受抑制)"""
    if len(cs1h) < 9 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.002: return False
    if _spatial_close(cs1h, 5) >= 0.38: return False
    if _sell_saturation(cs1h, 5) >= 0.48: return False
    return True


# ── Batch2 信号：PriceMemory / SatVelocity / TimePressure / FluxMomentum ─────

def _sig_pm_short(cs1h: list, cs4h: list, mem_n: int, mem_hi: float) -> bool:
    """PriceMemory SHORT: 4h下行 + 价格在近期顶部>mem_hi + 最近2根收盘偏低"""
    if len(cs1h) < mem_n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if _price_memory(cs1h, mem_n) <= mem_hi: return False
    if _spatial_close(cs1h, 2) >= 0.50: return False
    return True

def _sig_pm_long(cs1h: list, cs4h: list, mem_n: int, mem_lo: float) -> bool:
    """PriceMemory LONG: 4h上行 + 价格在近期底部<mem_lo + 最近2根收盘偏高"""
    if len(cs1h) < mem_n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if _price_memory(cs1h, mem_n) >= mem_lo: return False
    if _spatial_close(cs1h, 2) <= 0.50: return False
    return True

def _sig_satvel_short(cs1h: list, cs4h: list, n: int, lag: int, vel_th: float) -> bool:
    """SatVelocity SHORT: 4h下行 + 卖压以>vel_th/bar速率上升 + 当前卖压偏低(买方主导)"""
    if len(cs1h) < n + lag + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if _saturation_velocity(cs1h, n, lag) <= vel_th: return False
    if _sell_saturation(cs1h, n) >= 0.50: return False
    return True

def _sig_tp_short(cs1h: list, cs4h: list, pres_n: int, amp_th: float, pres_th: float) -> bool:
    """TimePressure SHORT: 4h下行 + 高度压缩状态(>pres_th占比低振幅)"""
    if len(cs1h) < pres_n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if _time_pressure(cs1h, pres_n, amp_th) <= pres_th: return False
    return True

def _sig_fm_long(cs1h: list, cs4h: list, short_n: int, long_n: int, fm_th: float) -> bool:
    """FluxMomentum LONG: 4h上行 + 1h价格下跌 + flux动量>fm_th(量价背离)"""
    if len(cs1h) < long_n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if gradient(cs1h, 3) >= -0.002: return False
    if _flux_momentum(cs1h, short_n, long_n) <= fm_th: return False
    return True

def _sig_fm_short(cs1h: list, cs4h: list, short_n: int, long_n: int, fm_th: float) -> bool:
    """FluxMomentum SHORT: 4h下行 + 1h价格上涨 + flux动量<fm_th(买压消退)"""
    if len(cs1h) < long_n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if gradient(cs1h, 3) <= 0.002: return False
    if _flux_momentum(cs1h, short_n, long_n) >= fm_th: return False
    return True


# 按测试胜率降序，dispatch 时取第一个命中的
_PM_SHORT_LIST = [
    # (mem_n, mem_hi, strategy_name)
    (20, 0.75, "A10-PriceMem-S-n20-hi75"),  # test 77.1%
    (20, 0.80, "A11-PriceMem-S-n20-hi80"),  # test 75.9%
    (20, 0.85, "A12-PriceMem-S-n20-hi85"),  # test 73.3%
    (14, 0.75, "A13-PriceMem-S-n14-hi75"),  # test 70.2%
    (14, 0.80, "A16-PriceMem-S-n14-hi80"),  # test 68.4%
    (10, 0.85, "A21-PriceMem-S-n10-hi85"),  # test 66.5%
    (10, 0.75, "A22-PriceMem-S-n10-hi75"),  # test 64.9%
    (10, 0.80, "A23-PriceMem-S-n10-hi80"),  # test 64.8%
    (14, 0.85, "A24-PriceMem-S-n14-hi85"),  # test 64.4%
]

_PM_LONG_LIST = [
    # (mem_n, mem_lo, strategy_name)
    (20, 0.15, "A14-PriceMem-L-n20-lo15"),  # test 69.5%
    (20, 0.25, "A15-PriceMem-L-n20-lo25"),  # test 68.6%
    (20, 0.20, "A17-PriceMem-L-n20-lo20"),  # test 68.2%
    (14, 0.25, "A18-PriceMem-L-n14-lo25"),  # test 67.2%
    (14, 0.20, "A19-PriceMem-L-n14-lo20"),  # test 67.0%
    (14, 0.15, "A20-PriceMem-L-n14-lo15"),  # test 66.7%
    (10, 0.25, "A26-PriceMem-L-n10-lo25"),  # test 63.0%
    (10, 0.15, "A27-PriceMem-L-n10-lo15"),  # test 62.8%
    (10, 0.20, "A29-PriceMem-L-n10-lo20"),  # test 62.5%
]

_SATVEL_SHORT_LIST = [
    # (n, lag, vel_th, strategy_name)
    (3, 3, 0.06, "A25-SatVel-S-n3-l3-v6"),  # test 63.9%
    (3, 2, 0.04, "A31-SatVel-S-n3-l2-v4"),  # test 61.4%
    (3, 4, 0.06, "A34-SatVel-S-n3-l4-v6"),  # test 58.9%
    (4, 4, 0.06, "A36-SatVel-S-n4-l4-v6"),  # test 58.6%
    (4, 2, 0.04, "A37-SatVel-S-n4-l2-v4"),  # test 58.6%
    (4, 4, 0.04, "A48-SatVel-S-n4-l4-v4"),  # test 57.6%
]

_TP_SHORT_LIST = [
    # (pres_n, amp_th, pres_th, strategy_name)
    (8,  0.006, 0.75, "A35-TimePres-S-n8-a6-t75"),   # test 58.8%
    (10, 0.006, 0.75, "A38-TimePres-S-n10-a6-t75"),  # test 58.4%
    (10, 0.006, 0.65, "A45-TimePres-S-n10-a6-t65"),  # test 57.8%
]

_FM_SHORT_LIST = [
    # (short_n, long_n, fm_th, strategy_name)
    (3, 6,  -0.03, "A30-FluxMom-S-s3-l6-f3"),  # test 62.2%
    (2, 8,  -0.04, "A33-FluxMom-S-s2-l8-f4"),  # test 59.4%
    (2, 12, -0.05, "A40-FluxMom-S-s2-l12-f5"), # test 58.2%
]

_FM_LONG_LIST = [
    # (short_n, long_n, fm_th, strategy_name)
    (3, 12, 0.05, "A28-FluxMom-L-s3-l12-f5"),  # test 62.6%
    (3, 12, 0.04, "A32-FluxMom-L-s3-l12-f4"),  # test 61.1%
    (2, 8,  0.04, "A39-FluxMom-L-s2-l8-f4"),   # test 58.3%
    (2, 6,  0.03, "A41-FluxMom-L-s2-l6-f3"),   # test 58.2%
    (2, 12, 0.05, "A42-FluxMom-L-s2-l12-f5"),  # test 58.1%
    (2, 6,  0.04, "A43-FluxMom-L-s2-l6-f4"),   # test 58.0%
    (2, 12, 0.03, "A44-FluxMom-L-s2-l12-f3"),  # test 57.9%
    (2, 8,  0.03, "A46-FluxMom-L-s2-l8-f3"),   # test 57.6%
    (3, 6,  0.05, "A47-FluxMom-L-s3-l6-f5"),   # test 57.6%
]

# ── Batch3 信号：OrderFlowDelta / VolMomentum ────────────────────────────────

def _sig_ofd_long(cs1h: list, cs4h: list, n: int, ofd_th: float) -> bool:
    """OFD LONG: 净卖方极端(投降式抛售) + 4h宏观上行"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if _order_flow_delta(cs1h, n) >= -abs(ofd_th): return False
    return True

def _sig_volmom_long(cs1h: list, cs4h: list, n: int, lag: int, vm_th: float) -> bool:
    """VolMom LONG: 价格下跌 + 成交量加速放大 + 4h宏观上行 = 投降式底部"""
    if len(cs1h) < n + lag + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if gradient(cs1h, n) >= -0.002: return False
    if _vol_momentum(cs1h, n, lag) <= vm_th: return False
    return True

# OFD LONG (4阶段验证通过): n5_t15, n8_t10
_OFD_LONG_LIST = [
    # (n, ofd_th, strategy_name)
    (5, 0.15, "OFD_L_n5_t15"),  # 净卖压>15% + 4h上行
    (8, 0.10, "OFD_L_n8_t10"),  # 净卖压>10% + 4h上行
]

# VolMom LONG (4阶段验证通过): 12个策略，按阈值高低排序
_VOLMOM_LONG_LIST = [
    # (n, lag, vm_th, strategy_name)
    (5, 2, 0.40, "VolMom_L_n5_l2_v40"),
    (5, 3, 0.40, "VolMom_L_n5_l3_v40"),
    (5, 4, 0.40, "VolMom_L_n5_l4_v40"),
    (3, 2, 0.40, "VolMom_L_n3_l2_v40"),
    (3, 3, 0.40, "VolMom_L_n3_l3_v40"),
    (5, 2, 0.20, "VolMom_L_n5_l2_v20"),
    (5, 3, 0.20, "VolMom_L_n5_l3_v20"),
    (5, 4, 0.20, "VolMom_L_n5_l4_v20"),
    (3, 2, 0.20, "VolMom_L_n3_l2_v20"),
    (3, 3, 0.20, "VolMom_L_n3_l3_v20"),
    (3, 4, 0.20, "VolMom_L_n3_l4_v20"),
]

# ── Batch4 信号：CloseConsistency ─────────────────────────────────────────────

def _sig_cc_long(cs1h: list, cs4h: list, n: int, cc_lo: float) -> bool:
    """CloseConsistency LONG: 收盘持续偏下半区(超卖) + 4h宏观上行 = 反转做多"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if _close_consistency(cs1h, n) >= cc_lo: return False
    return True

# CC LONG (4阶段验证通过): 去重后4组，按 test_wr 排列
# 触发条件: close_consistency(1h, n) < cc_lo AND gradient(4h, 4) > 0.001
_CC_LONG_LIST = [
    # (n, cc_lo, strategy_name)
    (12, 0.20, "CC_L_n12_lo20"),  # test 63.2%  n=939
    (10, 0.25, "CC_L_n10_lo25"),  # test 60.2%  n=2907
    (12, 0.30, "CC_L_n12_lo30"),  # test 59.7%  n=3274
    ( 8, 0.30, "CC_L_n8_lo30"),   # test 58.1%  n=9933
]

# ── Batch5 信号：PriceVelocityExhaustion ──────────────────────────────────────

def _sig_pvel_long(cs1h: list, cs4h: list, n: int, amp_n: int, pv_th: float) -> bool:
    """PriceVelocityExhaustion LONG: 价格下跌过快(振幅归一化动量极端) + 4h宏观上行 = 耗竭反转"""
    if len(cs1h) < max(n, amp_n) + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    if _price_velocity(cs1h, n, amp_n) >= -pv_th: return False
    return True

# PVel LONG (4阶段验证通过): 按 test_wr 排列，触发条件: price_velocity < -pv_th AND 4h上行
_PVEL_LONG_LIST = [
    # (n, amp_n, pv_th, strategy_name)
    (3, 10, 2.0, "PVel_L_n3_a10_v20"),  # test 60.9%  n=1062
    (3,  6, 2.0, "PVel_L_n3_a6_v20"),   # test 60.7%  n=898
    (5, 10, 1.5, "PVel_L_n5_a10_v15"),  # test 59.2%  n=5619
    (3, 10, 1.5, "PVel_L_n3_a10_v15"),  # test 59.1%  n=3480
    (5, 10, 2.0, "PVel_L_n5_a10_v20"),  # test 58.8%  n=2249
    (3,  6, 1.5, "PVel_L_n3_a6_v15"),   # test 58.7%  n=3444
    (5,  6, 1.5, "PVel_L_n5_a6_v15"),   # test 57.8%  n=6097
]

# ── Batch6 信号：CloseConsistency SHORT ────────────────────────────────────────

def _sig_cc_short(cs1h: list, cs4h: list, n: int, cc_hi: float) -> bool:
    """CloseConsistency SHORT: 收盘持续偏上半区(超买) + 4h宏观下行 = 反转做空"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if _close_consistency(cs1h, n) <= cc_hi: return False
    return True

# CC SHORT (4阶段验证通过): 去重后4组，触发条件: close_consistency > cc_hi AND 4h下行
_CC_SHORT_LIST = [
    # (n, cc_hi, strategy_name)
    (12, 0.75, "CC_S_n12_hi75"),  # test 67.9%  n=302
    (12, 0.70, "CC_S_n12_hi70"),  # test 65.9%  n=1590
    (10, 0.70, "CC_S_n10_hi70"),  # test 65.6%  n=1530
    ( 6, 0.70, "CC_S_n6_hi70"),   # test 59.3%  n=5042
]

# ── Batch7 信号：PriceVelocityExhaustion SHORT ────────────────────────────────

def _sig_pvel_short(cs1h: list, cs4h: list, n: int, amp_n: int, pv_th: float) -> bool:
    """PriceVelocityExhaustion SHORT: 价格上涨过快(振幅归一化动量极端) + 4h宏观下行 = 耗竭做空"""
    if len(cs1h) < max(n, amp_n) + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if _price_velocity(cs1h, n, amp_n) <= pv_th: return False
    return True

# PVel SHORT (4阶段验证通过): 按 test_wr 排列，触发条件: price_velocity > pv_th AND 4h下行
_PVEL_SHORT_LIST = [
    # (n, amp_n, pv_th, strategy_name)
    (3, 10, 2.0, "PVel_S_n3_a10_v20"),  # test 71.1%  n=640
    (3,  6, 2.0, "PVel_S_n3_a6_v20"),   # test 69.8%  n=556
    (5, 10, 3.0, "PVel_S_n5_a10_v30"),  # test 68.5%  n=124
    (5, 10, 2.0, "PVel_S_n5_a10_v20"),  # test 66.4%  n=1337
    (3, 10, 1.5, "PVel_S_n3_a10_v15"),  # test 66.1%  n=2454
    (3,  6, 1.5, "PVel_S_n3_a6_v15"),   # test 64.8%  n=2606
    (5, 10, 1.5, "PVel_S_n5_a10_v15"),  # test 64.6%  n=3697
    (5,  6, 2.0, "PVel_S_n5_a6_v20"),   # test 63.5%  n=1466
    (5,  6, 1.5, "PVel_S_n5_a6_v15"),   # test 62.4%  n=4275
]

# ── Batch8 信号：VolClimaxReversal ────────────────────────────────────────────

def _sig_vol_climax_long(cs1h: list, cs4h: list, n: int, ratio: float) -> bool:
    """VolClimaxReversal LONG: 极端放量阴线(投降抛售) + 4h上行 = 量能底部反转"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    mean_v = sum(c["vol"] for c in cs1h[-n-1:-1]) / n
    if mean_v <= 0 or cs1h[-1]["vol"] < ratio * mean_v: return False
    if cs1h[-1]["close"] >= cs1h[-1]["open"]: return False  # 必须是阴线
    return True

# VolClimax LONG (18/18 四阶段通过): 按 test_wr 排列，极端放量阴线 + 4h上行
_VC_LONG_LIST = [
    # (n, ratio, strategy_name)
    (6,  3.0, "VolClimax_L_n6_r30"),   # test 62.8%  n=811
    (8,  3.0, "VolClimax_L_n8_r30"),   # test 62.5%  n=837
    (6,  2.5, "VolClimax_L_n6_r25"),   # test 62.4%  n=1420
    (8,  2.5, "VolClimax_L_n8_r25"),   # test 61.4%  n=1497
    (12, 3.0, "VolClimax_L_n12_r30"),  # test 60.7%  n=940
    (6,  2.0, "VolClimax_L_n6_r20"),   # test 59.4%  n=2832
]

# ── Batch9 信号：VwapDeviationReturn ─────────────────────────────────────────

def _sig_vwap_dev_short(cs1h: list, cs4h: list, n: int, dev_th: float) -> bool:
    """VwapDeviationReturn SHORT: 价格大幅高于VWAP + 4h下行 = 均值回归做空"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    total_vol = sum(c["vol"] for c in cs1h[-n:])
    if total_vol <= 0: return False
    vwap = sum((c["high"] + c["low"] + c["close"]) / 3 * c["vol"] for c in cs1h[-n:]) / total_vol
    if vwap <= 0: return False
    return (cs1h[-1]["close"] - vwap) / vwap >= dev_th

# VwapDev SHORT (严格阈值优先): n=16 信号极强 test 66-68%
_VWAP_SHORT_LIST = [
    # (n, dev_th, strategy_name)  — 严格阈值先检查（极端情况优先命中）
    (16, 0.020, "VwapDev_S_n16_t20"),  # test 65.9%  n=1392
    (16, 0.015, "VwapDev_S_n16_t15"),  # test 67.3%  n=2596  ★★★
    (16, 0.010, "VwapDev_S_n16_t10"),  # test 68.3%  n=5198  ★★★
    (12, 0.015, "VwapDev_S_n12_t15"),  # test 60.6%  n=3835
    (12, 0.010, "VwapDev_S_n12_t10"),  # test 63.9%  n=7185
]

def _sig_vwap_dev_long(cs1h: list, cs4h: list, n: int, dev_th: float) -> bool:
    """VwapDeviationReturn LONG: 价格大幅低于VWAP + 4h上行 = 均值回归做多"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    total_vol = sum(c["vol"] for c in cs1h[-n:])
    if total_vol <= 0: return False
    vwap = sum((c["high"] + c["low"] + c["close"]) / 3 * c["vol"] for c in cs1h[-n:]) / total_vol
    if vwap <= 0: return False
    return (cs1h[-1]["close"] - vwap) / vwap <= -dev_th

# VwapDev LONG (严格阈值优先): n=16 test 61-64%
_VWAP_LONG_LIST = [
    # (n, dev_th, strategy_name)
    (16, 0.020, "VwapDev_L_n16_t20"),  # test 60.7%  n=2937
    (16, 0.015, "VwapDev_L_n16_t15"),  # test 62.4%  n=4672
    (16, 0.010, "VwapDev_L_n16_t10"),  # test 64.0%  n=7973  ★★★
    (12, 0.015, "VwapDev_L_n12_t15"),  # test 60.4%  n=5120
    (12, 0.010, "VwapDev_L_n12_t10"),  # test 61.9%  n=9124
]

# ── Batch10 信号：WickPressureBalance SHORT ───────────────────────────────────

def _sig_wick_pres_short(cs1h: list, cs4h: list, n: int, wp_th: float) -> bool:
    """WickPressureBalance SHORT: 上影线远大于下影线(卖方承压) + 4h下行 = 顶部反转"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    up_sum = 0.0; dn_sum = 0.0
    for c in cs1h[-n:]:
        rng = c["high"] - c["low"]
        if rng <= 0: continue
        bt = max(c["open"], c["close"])
        bb = min(c["open"], c["close"])
        up_sum += (c["high"] - bt)
        dn_sum += (bb - c["low"])
    return up_sum / (dn_sum + 1e-10) >= wp_th

# WickPres SHORT (2/18通过): 上影线主导 + 4h下行，严格阈值优先
_WP_SHORT_LIST = [
    # (n, wp_th, strategy_name)
    (8, 2.2, "WickPres_S_n8_t22"),  # test 57.2%  n=4975
    (8, 1.8, "WickPres_S_n8_t18"),  # test 57.9%  n=10374
]

# ── Batch11 信号：BodyDecelerationExhaustion SHORT ────────────────────────────

def _sig_body_decel_short(cs1h: list, cs4h: list, near_n: int, far_n: int, decel_th: float) -> bool:
    """BodyDecelerationExhaustion SHORT: 价格上涨但实体缩小 + 4h下行 = 动量耗竭做空"""
    total = near_n + far_n
    if len(cs1h) < total + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    if gradient(cs1h, far_n) <= 0.002: return False  # 近期总体上涨
    near_avg = sum(abs(c["close"] - c["open"]) for c in cs1h[-near_n:]) / near_n
    far_avg  = sum(abs(c["close"] - c["open"]) for c in cs1h[-total:-near_n]) / far_n
    ref = cs1h[-1]["close"]
    if ref <= 0 or far_avg <= 0: return False
    bd = (near_avg / ref) / ((far_avg / ref) + 1e-10)
    return bd < decel_th

# BodyDecel SHORT (3/24通过): 上涨实体减速 + 4h下行，严格阈值优先
_BD_SHORT_LIST = [
    # (near_n, far_n, decel_th, strategy_name)
    (3, 8, 0.55, "BodyDecel_S_nr3_fr8_t55"),  # test 57.7%  n=6071
    (3, 8, 0.65, "BodyDecel_S_nr3_fr8_t65"),  # test 58.2%  n=8103
    (3, 8, 0.75, "BodyDecel_S_nr3_fr8_t75"),  # test 59.0%  n=10243
]

# ── Batch12 信号：VolDirectionAsymmetry ───────────────────────────────────────

def _sig_vol_dir_asym_long(cs1h: list, cs4h: list, n: int, lo_th: float) -> bool:
    """VolDirAsymmetry LONG: 下跌K线量远大于上涨K线量(卖方耗竭) + 4h上行"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    up_v = [c["vol"] for c in cs1h[-n:] if c["close"] >= c["open"]]
    dn_v = [c["vol"] for c in cs1h[-n:] if c["close"] <  c["open"]]
    if not up_v or not dn_v: return False
    vda = (sum(up_v) / len(up_v)) / (sum(dn_v) / len(dn_v))
    return vda < lo_th

# VolDirAsym LONG (8/18通过): 卖方量主导 + 4h上行 (test 57-59%)
_VDA_LONG_LIST = [
    # (n, lo_th, strategy_name)
    (8,  0.60, "VolDirAsym_L_n8_t60"),   # test 59.0%  n=7316
    (6,  0.50, "VolDirAsym_L_n6_t50"),   # test 58.8%  n=4440
    (8,  0.67, "VolDirAsym_L_n8_t67"),   # test 58.3%  n=11604
    (6,  0.60, "VolDirAsym_L_n6_t60"),   # test 57.8%  n=8799
    (12, 0.60, "VolDirAsym_L_n12_t60"),  # test 57.5%  n=5273
]

def _sig_vol_dir_asym_short(cs1h: list, cs4h: list, n: int, hi_th: float) -> bool:
    """VolDirAsymmetry SHORT: 上涨K线量远大于下跌K线量(买方耗竭) + 4h下行"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    up_v = [c["vol"] for c in cs1h[-n:] if c["close"] >= c["open"]]
    dn_v = [c["vol"] for c in cs1h[-n:] if c["close"] <  c["open"]]
    if not up_v or not dn_v: return False
    vda = (sum(up_v) / len(up_v)) / (sum(dn_v) / len(dn_v))
    return vda > hi_th

# VolDirAsym SHORT (3/18通过): 买方量主导 + 4h下行 (test 59-61%)
_VDA_SHORT_LIST = [
    # (n, hi_th, strategy_name)
    (12, 2.00, "VolDirAsym_S_n12_t200"),  # test 59.2%  n=1154
    (12, 1.70, "VolDirAsym_S_n12_t170"),  # test 60.4%  n=2776
    (12, 1.50, "VolDirAsym_S_n12_t150"),  # test 60.5%  n=5637
]

# ── Batch13 信号：PriceGravityWell ────────────────────────────────────────────
# 引力井突破: 价格被某价位反复吸引(高密度聚集) + 4h方向 = 临界逃逸爆发
# 物理类比: 引力势阱 — 粒子反复回到同一位置，临界逃逸能量积累后急速远离

def _sig_grav_well_long(cs1h: list, cs4h: list, n: int, zone_pct: float, cluster_th: float) -> bool:
    """GravityWell LONG: 价格密集聚集于此 + 4h上行 = 引力逃逸向上"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) <= 0.001: return False
    ref = cs1h[-1]["close"]
    if ref <= 0: return False
    zone = ref * zone_pct
    count = sum(1 for c in cs1h[-n - 1:-1] if abs(c["close"] - ref) <= zone)
    return count / n >= cluster_th

def _sig_grav_well_short(cs1h: list, cs4h: list, n: int, zone_pct: float, cluster_th: float) -> bool:
    """GravityWell SHORT: 价格密集聚集于此 + 4h下行 = 引力逃逸向下"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    ref = cs1h[-1]["close"]
    if ref <= 0: return False
    zone = ref * zone_pct
    count = sum(1 for c in cs1h[-n - 1:-1] if abs(c["close"] - ref) <= zone)
    return count / n >= cluster_th

# GravWell SHORT (8/41选): 价格引力聚集 + 4h下行，严格到宽松排列
_GW_SHORT_LIST = [
    # (n, zone_pct, cluster_th, strategy_name)
    (20, 0.004, 0.45, "GravWell_S_n20_z4_c45"),  # test 65.9%  n=11659  ★★★
    (16, 0.004, 0.45, "GravWell_S_n16_z4_c45"),  # test 65.2%  n=11900  ★★★
    (20, 0.004, 0.35, "GravWell_S_n20_z4_c35"),  # test 64.5%  n=21005
    (20, 0.006, 0.45, "GravWell_S_n20_z6_c45"),  # test 64.2%  n=24411
    (16, 0.006, 0.45, "GravWell_S_n16_z6_c45"),  # test 63.4%  n=24742
    (16, 0.004, 0.35, "GravWell_S_n16_z4_c35"),  # test 63.3%  n=23110
    (20, 0.008, 0.45, "GravWell_S_n20_z8_c45"),  # test 63.0%  n=36649
    (12, 0.004, 0.45, "GravWell_S_n12_z4_c45"),  # test 61.9%  n=16927
]

# GravWell LONG (8/41选): 价格引力聚集 + 4h上行，严格到宽松排列
_GW_LONG_LIST = [
    # (n, zone_pct, cluster_th, strategy_name)
    (20, 0.004, 0.45, "GravWell_L_n20_z4_c45"),  # test 61.6%  n=12257  ★★
    (16, 0.004, 0.45, "GravWell_L_n16_z4_c45"),  # test 61.6%  n=12298  ★★
    (20, 0.006, 0.45, "GravWell_L_n20_z6_c45"),  # test 60.4%  n=25566
    (20, 0.004, 0.35, "GravWell_L_n20_z4_c35"),  # test 60.4%  n=21835
    (16, 0.006, 0.45, "GravWell_L_n16_z6_c45"),  # test 60.1%  n=25489
    (16, 0.004, 0.35, "GravWell_L_n16_z4_c35"),  # test 59.8%  n=23847
    (20, 0.008, 0.45, "GravWell_L_n20_z8_c45"),  # test 59.7%  n=38011
    (12, 0.004, 0.45, "GravWell_L_n12_z4_c45"),  # test 58.9%  n=17506
]


def _sig_vol_climax_short(cs1h: list, cs4h: list, n: int, ratio: float) -> bool:
    """VolClimaxReversal SHORT: 极端放量阳线(高潮买入) + 4h下行 = 量能顶部反转"""
    if len(cs1h) < n + 4 or len(cs4h) < 6: return False
    if gradient(cs4h, 4) >= -0.001: return False
    mean_v = sum(c["vol"] for c in cs1h[-n-1:-1]) / n
    if mean_v <= 0 or cs1h[-1]["vol"] < ratio * mean_v: return False
    if cs1h[-1]["close"] <= cs1h[-1]["open"]: return False  # 必须是阳线
    return True

# VolClimax SHORT (18/18 四阶段通过): 按 test_wr 排列，极端放量阳线 + 4h下行
_VC_SHORT_LIST = [
    # (n, ratio, strategy_name)
    (8,  2.0, "VolClimax_S_n8_r20"),   # test 65.7%  n=2261  ★★★
    (12, 2.0, "VolClimax_S_n12_r20"),  # test 65.2%  n=2290  ★★★
    (12, 2.5, "VolClimax_S_n12_r25"),  # test 64.9%  n=1093
    (8,  2.5, "VolClimax_S_n8_r25"),   # test 64.4%  n=1067
    (12, 3.0, "VolClimax_S_n12_r30"),  # test 64.6%  n=588
    (6,  2.5, "VolClimax_S_n6_r25"),   # test 63.5%  n=1089
]


def compute_signal(symbol: str,
                   cs1h: list,
                   cs_btc4h: list | None = None,
                   cs4h_self: list | None = None) -> dict | None:
    """
    对单个 symbol 尝试全部7个策略，返回第一个触发的信号，或 None。
    cs_btc4h: BTC 4h数据（D3/E3 需要）
    cs4h_self: symbol自身 4h数据（D1a/E1/E2/E4 需要）
    """
    price = cs1h[-1]["close"] if cs1h else 0
    if price == 0:
        return None

    # ── BTC 即时方向闸门（最高优先级，覆盖 Big4） ───────────────────────────
    # 两路信号：
    #   (A) 技术闸门：BTC 1h+15m+5m gradient 阈值（每轮扫描刷新，秒级）
    #   (B) Gemini 闸门：每 5 分钟问 Gemini 的定性判断（带 confidence）
    #
    # 两种工作模式：
    #   · 反向屏蔽模式（_REQUIRE_GEMINI_POSITIVE=False）：默认允许开单，
    #     只有 BTC 明确反向才禁某方向（OR 并集）。
    #   · 正向许可模式（_REQUIRE_GEMINI_POSITIVE=True）：默认不开单，
    #     必须 Gemini 明确同向（STRONG_LONG/SHORT + conf 达标）才放行。
    #     NEUTRAL/过期 → 两方向都不开（宁空仓不乱开）。
    _gm_block_long, _gm_block_short = _gemini_blocks()
    _block_long  = _BTC_REGIME.get("block_long",  False) or _gm_block_long
    _block_short = _BTC_REGIME.get("block_short", False) or _gm_block_short
    _LONG_OK  = _ALLOW_LONG  and not _block_long
    _SHORT_OK = _ALLOW_SHORT and not _block_short

    if _REQUIRE_GEMINI_POSITIVE:
        _gm_allow_long, _gm_allow_short = _gemini_allows()
        _LONG_OK  = _LONG_OK  and _gm_allow_long
        _SHORT_OK = _SHORT_OK and _gm_allow_short

    if not _LONG_OK and not _SHORT_OK:
        return None

    # ── 趋势SHORT：_ALLOW_SHORT 开关由 Big4 信号推导，不再按 score 分级 ─────
    # E1-E15/D系列/MomDecay：Big4 允许做空即开，不再额外屏蔽
    if _SHORT_OK:
        # D4b + D1a 只跑 Big4
        if symbol in set(BIG4):
            if sig_D4b(cs1h):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig("D4b-FluxQuality", "SHORT", price, sl_pct, tp_pct, cs1h)

            if cs4h_self and sig_D1a(cs1h, cs4h_self):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig("D1a-MTF-HDecay", "SHORT", price, sl_pct, tp_pct, cs1h)

        # D3: 精选 41 个山寨，需要 BTC 4h
        if symbol in _d3_set and cs_btc4h and sig_D3(cs1h, cs_btc4h):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("D3-AltLag", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E2-MTFDivergentExhaust（双向，SHORT + LONG）
        if cs4h_self:
            e2_dir = sig_E2(cs1h, cs4h_self)
            if e2_dir == "SHORT":
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig("E2-MTFDivergentExhaust", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E1-DecelFluxShort
        if cs4h_self and sig_E1(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E1-DecelFluxShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E4-KineticEfficiencyShort
        if cs4h_self and sig_E4(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E4-KineticEfficiencyShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E5-TripleFluxEntropy
        if cs4h_self and sig_E5(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E5-TripleFluxEntropy", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E6-InertialFrictionShort
        if cs4h_self and sig_E6(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E6-InertialFrictionShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E8-HollowGrowthShort
        if cs4h_self and sig_E8(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E8-HollowGrowthShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E9-WickSaturationShort
        if cs4h_self and sig_E9(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E9-WickSaturationShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E10-FrictionDecouplingShort
        if cs4h_self and sig_E10(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E10-FrictionDecouplingShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E12-NonLinearExhaustion
        if cs4h_self and sig_E12(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E12-NonLinearExhaustion", "SHORT", price, sl_pct, tp_pct, cs1h)

        # E14-InertialDragShort
        if cs4h_self and sig_E14(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E14-InertialDragShort", "SHORT", price, sl_pct, tp_pct, cs1h)

        # ── Alien SHORT Batch1（A2 BuyExhaustion + A3-A8 MomentumDecay）──────────
        if cs4h_self and symbol in _long_set:
            # A2-BuyExhShort: 买方耗竭 test=58.8%
            if sig_A2(cs1h, cs4h_self):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig("A2-BuyExhShort", "SHORT", price, sl_pct, tp_pct, cs1h)
            # A3-A8-MomDecayShort: 动量衰减，按测试胜率排序
            for _fn, _nm in [
                (sig_A3, "A3-MomDecay-l10-r50"),  # test 61.7%
                (sig_A4, "A4-MomDecay-l10-r40"),  # test 61.6%
                (sig_A5, "A5-MomDecay-l10-r30"),  # test 61.4%
                (sig_A7, "A7-MomDecay-l8-r40"),   # test 59.3%
                (sig_A8, "A8-MomDecay-l8-r30"),   # test 59.0%
            ]:
                if _fn(cs1h, cs4h_self):
                    sl_pct, tp_pct = sl_tp_from(cs1h)
                    return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)

    # ── Alien SHORT Batch2：Big4 允许做空即可 ───────────────────────────────
    # PM_S/SatVel/TimePres/FM_S：结构性高胜率做空，Big4 门槛外不再细分
    if _SHORT_OK and cs4h_self and symbol in _long_set:
        # PriceMemory SHORT: 价格在近期顶部 + 4h下行 (test 64-77%)
        for _mn, _mh, _nm in _PM_SHORT_LIST:
            if _sig_pm_short(cs1h, cs4h_self, _mn, _mh):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # SaturationVelocity SHORT: 卖压加速上升 (test 58-64%)
        for _n, _lag, _vth, _nm in _SATVEL_SHORT_LIST:
            if _sig_satvel_short(cs1h, cs4h_self, _n, _lag, _vth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # TimePressure SHORT: 低波动压缩 + 4h下行 (test 58-59%)
        for _pn, _ath, _pth, _nm in _TP_SHORT_LIST:
            if _sig_tp_short(cs1h, cs4h_self, _pn, _ath, _pth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # FluxMomentum SHORT: 量价背离 price up + flux down (test 58-62%)
        for _sn, _ln, _fth, _nm in _FM_SHORT_LIST:
            if _sig_fm_short(cs1h, cs4h_self, _sn, _ln, _fth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # CloseConsistency SHORT: 收盘偏上半区(超买) + 4h下行 (test 59-68%)
        for _n, _hi, _nm in _CC_SHORT_LIST:
            if _sig_cc_short(cs1h, cs4h_self, _n, _hi):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # PriceVelocity SHORT: 价格上涨过快(振幅归一化) + 4h下行 (test 62-71%)
        for _n, _an, _vth, _nm in _PVEL_SHORT_LIST:
            if _sig_pvel_short(cs1h, cs4h_self, _n, _an, _vth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # VolClimax SHORT: 极端放量阳线 + 4h下行 (test 63-66%)
        for _n, _r, _nm in _VC_SHORT_LIST:
            if _sig_vol_climax_short(cs1h, cs4h_self, _n, _r):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # VwapDeviation SHORT: 价格高于VWAP均值回归 + 4h下行 (test 61-68%)
        for _n, _th, _nm in _VWAP_SHORT_LIST:
            if _sig_vwap_dev_short(cs1h, cs4h_self, _n, _th):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # WickPressure SHORT: 上影线主导(卖方承压) + 4h下行 (test 57-58%)
        for _n, _wp, _nm in _WP_SHORT_LIST:
            if _sig_wick_pres_short(cs1h, cs4h_self, _n, _wp):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # BodyDecel SHORT: 上涨实体减速耗竭 + 4h下行 (test 58-59%)
        for _nn, _fn, _dt, _nm in _BD_SHORT_LIST:
            if _sig_body_decel_short(cs1h, cs4h_self, _nn, _fn, _dt):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # VolDirAsym SHORT: 买方量主导 + 4h下行 (test 59-61%)
        for _n, _hi, _nm in _VDA_SHORT_LIST:
            if _sig_vol_dir_asym_short(cs1h, cs4h_self, _n, _hi):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # GravityWell SHORT: 价格引力聚集 + 4h下行，临界逃逸向下 (test 62-66%)
        for _n, _z, _c, _nm in _GW_SHORT_LIST:
            if _sig_grav_well_short(cs1h, cs4h_self, _n, _z, _c):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "SHORT", price, sl_pct, tp_pct, cs1h)
        # Alien5 SHORT: auto_explore_alien5 部署的爆发前蓄力类做空策略
        for _fn5, _nm5 in _ALIEN5_SHORT:
            try:
                if _fn5(cs1h, cs4h_self) == "SHORT":
                    sl_pct, tp_pct = sl_tp_from(cs1h)
                    return _build_sig(_nm5, "SHORT", price, sl_pct, tp_pct, cs1h)
            except Exception:
                continue

        # Gemini theme-probe SHORT: 由 gemini_theme_probe.py 产出的 Gemini 假设策略
        for _fng, _nmg in _GEMINI_SHORT:
            try:
                if _fng(cs1h, cs4h_self) == "SHORT":
                    sl_pct, tp_pct = sl_tp_from(cs1h)
                    return _build_sig(_nmg, "SHORT", price, sl_pct, tp_pct, cs1h)
            except Exception:
                continue

    # ── 反转LONG：Big4 允许做多即可 ─────────────────────────────────────────
    # 超卖反转类策略，Big4 门槛外不再细分；内部信号仍需 4h 上行确认
    if _LONG_OK and cs4h_self and symbol in _long_set:
        # A1-SellCapLong: 卖方投降 test=59.5%
        if sig_A1(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("A1-SellCapLong", "LONG", price, sl_pct, tp_pct, cs1h)
        # A9-SpatDivLong: 空间背离 test=57.1%
        if sig_A9(cs1h, cs4h_self):
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("A9-SpatDivLong", "LONG", price, sl_pct, tp_pct, cs1h)
        # OrderFlowDelta LONG: 净卖方极端 + 4h上行
        for _n, _th, _nm in _OFD_LONG_LIST:
            if _sig_ofd_long(cs1h, cs4h_self, _n, _th):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # VolMomentum LONG: 价跌量增 + 4h上行 = 投降式底部
        for _n, _lag, _vth, _nm in _VOLMOM_LONG_LIST:
            if _sig_volmom_long(cs1h, cs4h_self, _n, _lag, _vth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # CloseConsistency LONG: 收盘偏下半区(超卖) + 4h上行 (test 58-63%)
        for _n, _lo, _nm in _CC_LONG_LIST:
            if _sig_cc_long(cs1h, cs4h_self, _n, _lo):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # PriceVelocity LONG: 价格下跌过快(振幅归一化) + 4h上行 (test 58-61%)
        for _n, _an, _vth, _nm in _PVEL_LONG_LIST:
            if _sig_pvel_long(cs1h, cs4h_self, _n, _an, _vth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # VolClimax LONG: 极端放量阴线(投降底部) + 4h上行 (test 59-63%)
        for _n, _r, _nm in _VC_LONG_LIST:
            if _sig_vol_climax_long(cs1h, cs4h_self, _n, _r):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # VwapDeviation LONG: 价格低于VWAP均值回归 + 4h上行 (test 59-64%)
        for _n, _th, _nm in _VWAP_LONG_LIST:
            if _sig_vwap_dev_long(cs1h, cs4h_self, _n, _th):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # VolDirAsym LONG: 卖方量主导(卖方耗竭) + 4h上行 (test 57-59%)
        for _n, _lo, _nm in _VDA_LONG_LIST:
            if _sig_vol_dir_asym_long(cs1h, cs4h_self, _n, _lo):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # GravityWell LONG: 价格引力聚集 + 4h上行，临界逃逸向上 (test 59-62%)
        for _n, _z, _c, _nm in _GW_LONG_LIST:
            if _sig_grav_well_long(cs1h, cs4h_self, _n, _z, _c):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # Alien5 LONG: auto_explore_alien5 部署的爆发前蓄力类做多策略
        for _fn5, _nm5 in _ALIEN5_LONG:
            try:
                if _fn5(cs1h, cs4h_self) == "LONG":
                    sl_pct, tp_pct = sl_tp_from(cs1h)
                    return _build_sig(_nm5, "LONG", price, sl_pct, tp_pct, cs1h)
            except Exception:
                continue

        # Gemini theme-probe LONG: 由 gemini_theme_probe.py 产出的 Gemini 假设策略
        for _fng, _nmg in _GEMINI_LONG:
            try:
                if _fng(cs1h, cs4h_self) == "LONG":
                    sl_pct, tp_pct = sl_tp_from(cs1h)
                    return _build_sig(_nmg, "LONG", price, sl_pct, tp_pct, cs1h)
            except Exception:
                continue

    # ── 一般LONG：Big4 允许做多即可 ─────────────────────────────────────────
    # 趋势/震荡类多头策略；Big4 门槛外不再细分，包括 PM_L/FM_L/E-DB 系列
    if not _LONG_OK:
        return None

    # E2-MTFDivergentExhaust LONG 方向
    if cs4h_self:
        e2_dir = sig_E2(cs1h, cs4h_self)
        if e2_dir == "LONG":
            sl_pct, tp_pct = sl_tp_from(cs1h)
            return _build_sig("E2-MTFDivergentExhaust", "LONG", price, sl_pct, tp_pct, cs1h)

    if cs_btc4h and symbol in _long_set and sig_E40(cs1h, cs_btc4h):
        sl_pct, tp_pct = sl_tp_from(cs1h)
        return _build_sig("sig_E40-BTCLead_b10_h8_f50", "LONG", price, sl_pct, tp_pct, cs1h)
    if cs_btc4h and symbol in _long_set and sig_E89(cs1h, cs_btc4h):
        sl_pct, tp_pct = sl_tp_from(cs1h)
        return _build_sig("sig_E89-BTCLead_b10_h6_f50", "LONG", price, sl_pct, tp_pct, cs1h)

    # E3-AltDipRecovery（精选山寨，LONG）
    if symbol in _e3_set and cs_btc4h and sig_E3(cs1h, cs_btc4h):
        sl_pct, tp_pct = sl_tp_from(cs1h)
        return _build_sig("E3-AltDipRecovery", "LONG", price, sl_pct, tp_pct, cs1h)

    # ── Alien LONG Batch2（PriceMemory + FluxMomentum）──────────────────────────
    # 趋势顺势多头：价格在近期底部且4h宏观上行，需要中性以上市场
    if cs4h_self and symbol in _long_set:
        # PriceMemory LONG: 价格在近期底部 + 4h上行 (test 63-70%)
        for _mn, _ml, _nm in _PM_LONG_LIST:
            if _sig_pm_long(cs1h, cs4h_self, _mn, _ml):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)
        # FluxMomentum LONG: 量价背离 price down + flux up (test 57-63%)
        for _sn, _ln, _fth, _nm in _FM_LONG_LIST:
            if _sig_fm_long(cs1h, cs4h_self, _sn, _ln, _fth):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(_nm, "LONG", price, sl_pct, tp_pct, cs1h)

    # ── E16-E30: DecelBounce LONG 家族（全量标的，需要 self 4h）────────────────
    # 按测试胜率从高到低排列；第一个匹配即返回，避免重复开仓
    if cs4h_self and symbol in _long_set:
        _db_checks = [
            (sig_E29, "E29-DB-h14"),           # test 69.0%
            (sig_E28, "E28-DB-h12"),           # test 67.7%
            (sig_E30, "E30-DB-h7-DeepDecline"),# test 65.3%
            (sig_E24, "E24-DB-DeepLowAmp"),    # test 64.1%
            (sig_E21, "E21-DB-h7"),            # test 64.1%
            (sig_E17, "E17-DecelBounce-Deep"), # test 63.9%
            (sig_E22, "E22-DB-h10"),           # test 63.1%
            (sig_E26, "E26-DB-3macroN"),       # test 62.4%
            (sig_E19, "E19-DB-h4"),            # test 59.6%
            (sig_E18, "E18-DecelBounce-LowAmp"),# test 59.3%
            (sig_E25, "E25-DB-AmpMacro"),      # test 59.1%
            (sig_E23, "E23-DB-AmpMed"),        # test 59.0%
            (sig_E16, "E16-DecelBounce"),      # test 58.3%
            (sig_E20, "E20-DB-h5"),            # test 58.3%

            # ── E31-E98 自动部署策略（2026-04-14 今日做多，全部启用）──
            (sig_E31, "sig_E31-DB_h16"),  # test 63.9%
            (sig_E32, "sig_E32-DB_h14_ht3"),  # test 63.8%
            (sig_E33, "sig_E33-DB_h14_f55"),  # test 63.7%
            (sig_E34, "sig_E34-DB_h15"),  # test 63.6%
            (sig_E35, "sig_E35-DB_h14_ht4"),  # test 63.4%
            (sig_E36, "sig_E36-DB_h8_f56"),  # test 63.3%
            (sig_E37, "sig_E37-DB_h14_ht5"),  # test 62.9%
            (sig_E38, "sig_E38-DB_h14_f56"),  # test 62.6%
            (sig_E39, "sig_E39-DB_h12_ht4"),  # test 62.6%
            (sig_E41, "sig_E41-DB_h14_ht6"),  # test 62.3%
            (sig_E42, "sig_E42-DB_h12_ht3"),  # test 62.2%
            (sig_E43, "sig_E43-DB_h12_ht5"),  # test 62.2%
            (sig_E44, "sig_E44-DB_h12_ht6"),  # test 61.9%
            (sig_E45, "sig_E45-DB_h10_f56"),  # test 61.6%
            (sig_E46, "sig_E46-OvrSold_h10_d12_f55"),  # test 61.5%
            (sig_E48, "sig_E48-DB_h8_f55"),  # test 61.4%
            (sig_E49, "sig_E49-DB_h20"),  # test 61.4%
            (sig_E50, "sig_E50-OvrSold_h10_d20_f53"),  # test 61.3%
            (sig_E51, "sig_E51-OvrSold_h10_d10_f55"),  # test 61.2%
            (sig_E52, "sig_E52-FluxAccel_h10_mac5_f51"),  # test 61.2%
            (sig_E53, "sig_E53-OvrSold_h10_d15_f55"),  # test 61.2%
            (sig_E54, "sig_E54-FluxAccel_h10_mac5_f49"),  # test 61.2%
            (sig_E55, "sig_E55-OvrSold_h10_d8_f55"),  # test 61.2%
            (sig_E56, "sig_E56-DB_h12_f55"),  # test 61.2%
            (sig_E57, "sig_E57-FluxAccel_h10_mac3_f49"),  # test 61.0%
            (sig_E58, "sig_E58-DB_h8_ht6"),  # test 61.0%
            (sig_E59, "sig_E59-DB_h10_ht4"),  # test 61.0%
            (sig_E60, "sig_E60-FluxAccel_h10_mac3_f51"),  # test 60.9%
            (sig_E61, "sig_E61-FluxAccel_h10_mac2_f49"),  # test 60.9%
            (sig_E62, "sig_E62-DB_h8_ht5"),  # test 60.8%
            (sig_E63, "sig_E63-OvrSold_h10_d15_f53"),  # test 60.8%
            (sig_E64, "sig_E64-FluxAccel_h10_mac2_f51"),  # test 60.7%
            (sig_E65, "sig_E65-DB_h8_ht3"),  # test 60.7%
            (sig_E66, "sig_E66-OvrSold_h10_d12_f53"),  # test 60.6%
            (sig_E67, "sig_E67-DB_h18"),  # test 60.6%
            (sig_E68, "sig_E68-DB_h10_ht5"),  # test 60.6%
            (sig_E69, "sig_E69-DB_h10_ht3"),  # test 60.6%
            (sig_E70, "sig_E70-OvrSold_h10_d8_f53"),  # test 60.5%
            (sig_E71, "sig_E71-DB_h8_ht4"),  # test 60.4%
            (sig_E72, "sig_E72-DB_h10_ht6"),  # test 60.3%
            (sig_E73, "sig_E73-FluxAccel_h10_mac5_f53"),  # test 60.3%
            (sig_E74, "sig_E74-OvrSold_h10_d10_f53"),  # test 60.2%
            (sig_E75, "sig_E75-FluxAccel_h8_mac5_f51"),  # test 60.2%
            (sig_E76, "sig_E76-DB_h10_f55"),  # test 60.2%
            (sig_E77, "sig_E77-DB_h12_f56"),  # test 60.1%
            (sig_E78, "sig_E78-FluxAccel_h8_mac3_f51"),  # test 60.1%
            (sig_E79, "sig_E79-FluxAccel_h8_mac5_f49"),  # test 60.0%
            (sig_E80, "sig_E80-FluxAccel_h8_mac2_f51"),  # test 60.0%
            (sig_E81, "sig_E81-FluxAccel_h10_mac3_f53"),  # test 60.0%
            (sig_E82, "sig_E82-FluxAccel_h8_mac3_f49"),  # test 60.0%
            (sig_E83, "sig_E83-FluxAccel_h8_mac2_f49"),  # test 59.9%
            (sig_E84, "sig_E84-OvrSold_h8_d12_f55"),  # test 59.8%
            (sig_E85, "sig_E85-FluxAccel_h10_mac2_f53"),  # test 59.6%
            (sig_E86, "sig_E86-FluxAccel_h8_mac3_f53"),  # test 59.3%
            (sig_E87, "sig_E87-FluxAccel_h8_mac5_f53"),  # test 59.2%
            (sig_E88, "sig_E88-FluxAccel_h8_mac2_f53"),  # test 59.2%
            (sig_E90, "sig_E90-DB_h25"),  # test 58.7%
            (sig_E91, "sig_E91-FluxAccel_h6_mac5_f49"),  # test 58.3%
            (sig_E92, "sig_E92-FluxAccel_h6_mac3_f49"),  # test 58.2%
            (sig_E93, "sig_E93-FluxAccel_h6_mac2_f49"),  # test 58.1%
            (sig_E94, "sig_E94-FluxAccel_h6_mac5_f53"),  # test 57.8%
            (sig_E95, "sig_E95-FluxAccel_h6_mac3_f51"),  # test 57.8%
            (sig_E96, "sig_E96-FluxAccel_h6_mac5_f51"),  # test 57.7%
            (sig_E97, "sig_E97-FluxAccel_h6_mac2_f51"),  # test 57.6%
            (sig_E98, "sig_E98-FluxAccel_h4_mac5_f49"),  # test 57.2%
        ]
        for fn, name in _db_checks:
            if fn(cs1h, cs4h_self):
                sl_pct, tp_pct = sl_tp_from(cs1h)
                return _build_sig(name, "LONG", price, sl_pct, tp_pct, cs1h)

    return None


def _build_sig(strategy: str, direction: str, price: float,
               sl_pct: float, tp_pct: float, cs1h: list) -> dict:
    # 用回测优化参数覆盖振幅动态参数
    p = _strat_params(strategy)
    if p["sl_pct"] is not None:
        sl_pct = p["sl_pct"]
    if p["tp_pct"] is not None:
        tp_pct = p["tp_pct"]
    hold_minutes = p["hold_h"] * 60

    if direction == "SHORT":
        sl = _pround(price * (1 + sl_pct), price)
        tp = _pround(price * (1 - tp_pct), price)
    else:  # LONG
        sl = _pround(price * (1 - sl_pct), price)
        tp = _pround(price * (1 + tp_pct), price)
    return {
        "direction":        direction,
        "strategy":         strategy,
        "price":            price,
        "sl":               sl,
        "tp":               tp,
        "sl_pct":           round(sl_pct, 5),
        "tp_pct":           round(tp_pct, 5),
        "amp":              round(amplitude(cs1h, 6), 5),
        "g6":               round(gradient(cs1h, 6), 5),
        "candle_ts":        cs1h[-1]["t"],
        "max_hold_minutes": hold_minutes,
    }


# ── API 工具 ───────────────────────────────────────────────────────────────────

def _api(method: str, path: str, **kwargs) -> dict:
    resp = req.request(method, f"{API_BASE}{path}", timeout=15, **kwargs)
    resp.raise_for_status()
    return resp.json()


def _get_live_price(symbol: str) -> float | None:
    """从交易所 API 获取实时价格（非K线收盘价），失败返回 None。"""
    try:
        sym = symbol.replace("/", "")
        data = _api("GET", f"/api/futures/price/{sym}")
        if isinstance(data, dict):
            price = data.get("price") or (data.get("data") or {}).get("price")
            if price:
                return float(price)
    except Exception as _e:
        logger.debug(f"[{symbol}] _get_live_price failed: {_e}")
    return None


def open_position(symbol: str, direction: str, qty: float,
                  sl: float, tp: float, strategy: str,
                  max_hold_minutes: int = MAX_HOLD_HOURS * 60,
                  dry_run: bool = False) -> dict:
    payload = {
        "account_id":        ACCOUNT_ID,
        "symbol":            symbol,
        "position_side":     direction,
        "quantity":          round(qty, 6),
        "leverage":          LEVERAGE,
        "stop_loss_price":   sl,
        "take_profit_price": tp,
        "max_hold_minutes":  max_hold_minutes,
        "source":            f"dimension_trader:{strategy}",
    }
    if dry_run:
        logger.info(f"[DRY-RUN] {json.dumps(payload)}")
        return {"success": True, "data": {"position_id": -1}}
    return _api("POST", "/api/futures/open", json=payload)

def get_position_detail(pid: int) -> dict | None:
    try:
        data = _api("GET", f"/api/futures/positions/{pid}")
        if isinstance(data, dict) and data.get("success"):
            return data.get("data")
        return data if isinstance(data, dict) else None
    except Exception:
        return None

def close_position(pid: int, reason: str = "dimension_max_hold") -> dict:
    return _api("POST", f"/api/futures/close/{pid}", json={"reason": reason})


# ── 仓位监控 ──────────────────────────────────────────────────────────────────

def _monitor_position(trade: dict, dry_run: bool) -> None:
    pid      = trade.get("position_id")
    symbol   = trade["symbol"]
    deadline = trade["deadline"]   # Unix timestamp (time.time() + N*3600)

    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            logger.info(f"[{symbol}] Max hold reached, force closing pid={pid}")
            if not dry_run and pid and pid > 0:
                try:
                    close_position(pid)
                except Exception as e:
                    logger.error(f"[{symbol}] Force close failed: {e}")
            return
        if pid and pid > 0:
            d = get_position_detail(pid)
            if d and d.get("status") != "open":
                logger.info(f"[{symbol}] Position {pid} closed by SL/TP")
                return
        time.sleep(min(60, remaining))


# ── 主循环 ────────────────────────────────────────────────────────────────────

def _get_open_dim_positions() -> tuple[set, dict]:
    """
    查询 DB 中所有未平仓的 dimension_trader 仓位。
    返回:
      open_set  — {(symbol, direction), ...}  用于同标的同方向去重
      strat_cnt — {strategy_name: count}       用于单策略持仓上限
    """
    try:
        conn = pymysql.connect(**_DB_CFG)
        try:
            with conn.cursor() as c:
                c.execute(
                    "SELECT symbol, position_side, source FROM futures_positions "
                    "WHERE status='open' AND source LIKE 'dimension_trader:%'"
                )
                rows = c.fetchall()
        finally:
            conn.close()
        open_set  = {(r[0], r[1]) for r in rows}
        strat_cnt: dict[str, int] = {}
        for r in rows:
            strat = str(r[2]).replace("dimension_trader:", "", 1)
            strat_cnt[strat] = strat_cnt.get(strat, 0) + 1
        return open_set, strat_cnt
    except Exception as e:
        logger.error(f"_get_open_dim_positions failed: {e}")
        return set(), {}


# ── Symbol 黑名单（trading_symbol_rating）──────────────────────────────────
# level 0=白 / 1=1级(小仓) / 2=2级(更小仓) / 3=3级(永禁)
# 我们只屏蔽 level >= _SYMBOL_BLACKLIST_MIN_LEVEL 的
_SYMBOL_BLACKLIST_MIN_LEVEL = 3
_symbol_blacklist: set = set()
_symbol_blacklist_last_reload: float = 0.0
_SYMBOL_BLACKLIST_RELOAD_INTERVAL = 1800  # 30 分钟


def _load_symbol_blacklist() -> set:
    """从 trading_symbol_rating 表读取 level >= _SYMBOL_BLACKLIST_MIN_LEVEL 的 symbol。"""
    global _symbol_blacklist, _symbol_blacklist_last_reload
    try:
        conn = pymysql.connect(**_DB_CFG)
        try:
            with conn.cursor() as c:
                c.execute(
                    "SELECT symbol FROM trading_symbol_rating WHERE rating_level >= %s",
                    (_SYMBOL_BLACKLIST_MIN_LEVEL,)
                )
                rows = c.fetchall()
        finally:
            conn.close()
        # 归一化：表里有些没带 "/USDT"，不匹配就忽略
        new_set = {str(r[0]).strip() for r in rows if r and r[0]}
        added   = new_set - _symbol_blacklist
        removed = _symbol_blacklist - new_set
        if added or removed or not _symbol_blacklist_last_reload:
            logger.info(
                f"[SYMBOL-BLACKLIST] reloaded: {len(new_set)} symbols @ level>={_SYMBOL_BLACKLIST_MIN_LEVEL}"
                + (f"  +added: {sorted(added)}" if added else "")
                + (f"  -removed: {sorted(removed)}" if removed else "")
            )
        _symbol_blacklist = new_set
        _symbol_blacklist_last_reload = time.time()
        return new_set
    except Exception as e:
        logger.warning(f"[SYMBOL-BLACKLIST] reload failed: {e}")
        return _symbol_blacklist


def _apply_symbol_blacklist(symbols: list[str]) -> list[str]:
    """从扫描池剔除黑名单 symbol（精确匹配，避免误伤）。

    说明：DB 里历史上有没斜杠的污染 entries（如 'BTCUSDT'、'DYDX'），
    如果做去斜杠 fuzzy 匹配，会把正常的 'BTC/USDT'（level=0）误伤。
    因此**只做精确字符串匹配**，把奇葩 entries 当成无关数据忽略。
    """
    if not _symbol_blacklist:
        return symbols
    filtered = [s for s in symbols if s not in _symbol_blacklist]
    if len(filtered) != len(symbols):
        blocked = [s for s in symbols if s in _symbol_blacklist]
        logger.warning(
            f"[SYMBOL-BLACKLIST] blocked {len(blocked)}/{len(symbols)}: {sorted(blocked)}"
        )
    return filtered


def run(symbols: list[str] = None, dry_run: bool = False) -> None:
    if symbols is None:
        symbols = ALL_SYMBOLS

    # 启动时应用一次 symbol 黑名单过滤
    _load_symbol_blacklist()
    symbols = _apply_symbol_blacklist(list(symbols))

    logger.info(f"Dimension Trader started.  symbols={len(symbols)}  dry_run={dry_run}")
    logger.info("Direction flags controlled by system_settings (allow_long / allow_short)")
    logger.info("SHORT: D1a|D4b|D3|E1-E15(+E2)  LONG: E2|E3|E16-E30|E31-E98|E40/E47/E89")
    logger.info(f"Account={ACCOUNT_ID}  Margin={MARGIN_PER_TRADE}  Leverage={LEVERAGE}x  MaxHold={MAX_HOLD_HOURS}h")

    # 启动时从 DB 加载最新策略参数
    _load_strategy_params_from_db()

    # 启动 BTC Gemini 后台探测线程（每 15 分钟问一次）
    _start_btc_gemini_worker()
    _btc_gemini_tick()  # 启动时同步跑一次，避免首次扫描时 Gemini 状态为空

    last_signal_ts: dict[str, object] = {sym: None for sym in symbols}
    _PARAMS_RELOAD_INTERVAL = 1800  # 每30分钟刷新一次策略参数+Big4 regime

    while True:
        logger.info(f"--- Scan [{datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)] ---")
        # 定期从 DB 刷新策略参数
        if time.time() - _params_last_reload > _PARAMS_RELOAD_INTERVAL:
            _load_strategy_params_from_db()

        # 定期从 DB 刷新 symbol 黑名单（运行中可热更新）
        if time.time() - _symbol_blacklist_last_reload > _SYMBOL_BLACKLIST_RELOAD_INTERVAL:
            _load_symbol_blacklist()

        # 每轮扫描前查一次已有 dimension_trader 仓位，避免同方向重复开仓 + 策略持仓上限
        open_dim, strat_cnt = _get_open_dim_positions()

        # BTC 4h 数据（D1a/D3 共用）
        try:
            cs_btc4h = load_candles_db("BTC/USDT", "4h", CANDLE_LOAD_4H)
        except Exception as e:
            logger.error(f"Failed to load BTC 4h: {e}")
            cs_btc4h = []

        # BTC 即时方向闸门：每轮扫描（60s）刷新，5m/15m/1h 三周期共振
        try:
            _cs_btc1h_gate  = load_candles_db("BTC/USDT", "1h",  _BTC_GATE_1H_N + 2)
            _cs_btc15m_gate = load_candles_db("BTC/USDT", "15m", _BTC_GATE_15M_N + 2)
            _cs_btc5m_gate  = load_candles_db("BTC/USDT", "5m",  _BTC_GATE_5M_N + 2)
            _update_btc_regime(_cs_btc1h_gate, _cs_btc15m_gate, _cs_btc5m_gate)
        except Exception as e:
            logger.warning(f"BTC gate refresh failed: {e}")

        # 汇总两路闸门状态，便于观察本轮是否屏蔽了 LONG/SHORT
        _gm_bl, _gm_bs = _gemini_blocks()
        _eff_block_long  = _BTC_REGIME.get("block_long",  False) or _gm_bl
        _eff_block_short = _BTC_REGIME.get("block_short", False) or _gm_bs
        _gemini_verdict = _BTC_GEMINI_REGIME.get("verdict", "?")
        _gemini_conf    = _BTC_GEMINI_REGIME.get("confidence", 0)
        if _REQUIRE_GEMINI_POSITIVE:
            _al, _as = _gemini_allows()
            # 正向许可模式：每轮都打印许可状态（帮助理解"为什么都不开单"）
            logger.info(
                f"[GATES/POSITIVE] allow_long={_al} allow_short={_as}  "
                f"gemini={_gemini_verdict}@{_gemini_conf:.2f}  "
                f"tech=({_BTC_REGIME.get('reason','-')})"
            )
        elif _eff_block_long or _eff_block_short:
            logger.info(
                f"[GATES] block_long={_eff_block_long} block_short={_eff_block_short}  "
                f"tech=({_BTC_REGIME.get('reason','-')})  "
                f"gemini={_gemini_verdict}@{_gemini_conf:.2f}"
            )

        # 趋势反转紧急平仓（每轮扫描检查一次，内部有 10min 冷却）
        try:
            _check_and_emergency_exit()
        except Exception as e:
            logger.warning(f"emergency-exit in main loop failed: {e}")

        for sym in symbols:
            # 运行时黑名单二次保险（精确匹配，支持热更新后立即屏蔽）
            if sym in _symbol_blacklist:
                continue
            try:
                cs1h = load_candles_db(sym, "1h", CANDLE_LOAD_1H)
                cs4h_self = load_candles_db(sym, "4h", CANDLE_LOAD_4H)
            except Exception as e:
                logger.error(f"[{sym}] load_candles failed: {e}")
                continue

            if len(cs1h) < 10:
                logger.warning(f"[{sym}] not enough 1h candles, skip")
                continue

            # 新鲜度校验：最新 K线超过 3h 未更新，数据陈旧，跳过
            _candle_age_h = (datetime.now() - cs1h[-1]["t"]).total_seconds() / 3600
            if _candle_age_h > 3:
                logger.warning(f"[{sym}] candle data stale ({_candle_age_h:.1f}h old), skip")
                continue

            # 去重：同一根 1h K线只触发一次
            latest_ts = cs1h[-1]["t"]
            if latest_ts == last_signal_ts[sym]:
                logger.debug(f"[{sym}] same candle, skip")
                continue

            sig = compute_signal(sym, cs1h, cs_btc4h, cs4h_self)
            if sig is None:
                logger.info(f"[{sym}] no signal (price={cs1h[-1]['close']:.4f})")
                continue

            # 去重：同方向已有 dimension_trader 仓位则跳过
            if (sym, sig["direction"]) in open_dim:
                logger.info(f"[{sym}] {sig['direction']} already open, skip ({sig['strategy']})")
                last_signal_ts[sym] = latest_ts  # 更新 ts，避免下轮继续打印
                continue

            # 策略持仓上限：每种策略最多 MAX_POSITIONS_PER_STRATEGY 个同时持仓
            strat_name = sig["strategy"]
            if strat_cnt.get(strat_name, 0) >= MAX_POSITIONS_PER_STRATEGY:
                logger.info(
                    f"[{sym}] {strat_name} at limit "
                    f"({strat_cnt[strat_name]}/{MAX_POSITIONS_PER_STRATEGY}), skip"
                )
                last_signal_ts[sym] = latest_ts
                continue

            logger.info(
                f"[{sym}] SIGNAL {sig['strategy']} | "
                f"price={sig['price']:.4f}  SL={sig['sl']:.4f}({sig['sl_pct']*100:.2f}%)  "
                f"TP={sig['tp']:.4f}({sig['tp_pct']*100:.2f}%)  "
                f"hold={sig.get('max_hold_minutes', MAX_HOLD_HOURS*60)//60}h  "
                f"amp={sig['amp']:.4f}  g6={sig['g6']:+.5f}"
            )

            # 开仓前校验：取实时交易所价格，防止信号价格与市场价偏差导致开仓即止损
            try:
                live_price = _get_live_price(sym)
                if live_price is None:
                    # 降级：用 K 线收盘价
                    cs_now = load_candles_db(sym, "1h", 2)
                    live_price = cs_now[-1]["close"] if cs_now else sig["price"]
                sl_breached = (
                    (sig["direction"] == "LONG"  and live_price <= sig["sl"]) or
                    (sig["direction"] == "SHORT" and live_price >= sig["sl"])
                )
                if sl_breached:
                    logger.warning(
                        f"[{sym}] live_price={live_price} already past SL={sig['sl']} "
                        f"({sig['direction']}), signal stale, skip"
                    )
                    last_signal_ts[sym] = latest_ts
                    continue

                # 最小价差保护：live_price 距 SL 太近则跳过（手续费无法覆盖）
                sl_gap = abs(live_price - sig["sl"]) / live_price
                if sl_gap < MIN_SL_GAP_PCT:
                    logger.warning(
                        f"[{sym}] sl_gap={sl_gap:.4%} < MIN={MIN_SL_GAP_PCT:.4%} "
                        f"(live={live_price} sl={sig['sl']} {sig['direction']}), skip"
                    )
                    last_signal_ts[sym] = latest_ts
                    continue
            except Exception as _e:
                logger.warning(f"[{sym}] live price check failed: {_e}, proceed anyway")

            _margin = MARGIN_OVERRIDE.get(sig["strategy"], MARGIN_PER_TRADE)
            # 数量和止损止盈均基于实时价格计算，不使用K线信号价
            qty = (_margin * LEVERAGE) / live_price
            if sig["direction"] == "LONG":
                actual_sl = _pround(live_price * (1 - GLOBAL_SL_PCT), live_price)
                actual_tp = _pround(live_price * (1 + GLOBAL_TP_PCT), live_price)
            else:
                actual_sl = _pround(live_price * (1 + GLOBAL_SL_PCT), live_price)
                actual_tp = _pround(live_price * (1 - GLOBAL_TP_PCT), live_price)
            logger.info(
                f"[{sym}] live_price={live_price} SL={actual_sl}(-{GLOBAL_SL_PCT*100:.0f}%) "
                f"TP={actual_tp}(+{GLOBAL_TP_PCT*100:.0f}%)"
            )
            try:
                hold_min = sig.get("max_hold_minutes", MAX_HOLD_HOURS * 60)
                res = open_position(sym, sig["direction"], qty,
                                    actual_sl, actual_tp, sig["strategy"],
                                    max_hold_minutes=hold_min,
                                    dry_run=dry_run)
            except Exception as e:
                logger.error(f"[{sym}] open_position failed: {e}")
                continue

            if res.get("success"):
                data     = res.get("data", {})
                pid      = (data.get("position_id") or data.get("id")) if isinstance(data, dict) else None
                hold_min = sig.get("max_hold_minutes", MAX_HOLD_HOURS * 60)
                deadline = time.time() + hold_min * 60  # 按策略优化持仓时间
                logger.info(f"[{sym}] opened pid={pid}")
                last_signal_ts[sym] = latest_ts
                # 新仓位加入本轮去重集合，防止同次扫描内对同标的反复开仓
                open_dim.add((sym, sig["direction"]))
                # 同步更新策略计数，防止同一轮扫描内同策略超开
                strat_cnt[strat_name] = strat_cnt.get(strat_name, 0) + 1

                t = threading.Thread(
                    target=_monitor_position,
                    args=({"symbol": sym, "position_id": pid, "deadline": deadline}, dry_run),
                    daemon=True,
                )
                t.start()
            else:
                err = res.get("message") or res.get("error") or str(res)
                logger.error(f"[{sym}] open failed: {err}")

        time.sleep(CHECK_INTERVAL)


# ── 只扫信号 ──────────────────────────────────────────────────────────────────

def scan_once(symbols: list[str] = None) -> None:
    if symbols is None:
        symbols = ALL_SYMBOLS

    try:
        cs_btc4h = load_candles_db("BTC/USDT", "4h", CANDLE_LOAD_4H)
    except Exception as e:
        cs_btc4h = []
        logger.error(f"BTC 4h load failed: {e}")

    # 刷新 BTC 即时方向闸门
    try:
        _update_btc_regime()
    except Exception as e:
        logger.warning(f"BTC gate refresh failed: {e}")

    sep = "=" * 78
    print(f"\n{sep}")
    print(f"  DIMENSION SIGNAL SCAN  [{datetime.now(UTC8).strftime('%Y-%m-%d %H:%M:%S')} (UTC+8)]")
    if _BTC_REGIME.get("block_long") or _BTC_REGIME.get("block_short"):
        print(f"  [BTC-GATE] {_BTC_REGIME.get('reason','')}")
    print(f"  SHORT: D1a | D4b | D3 | E1-E15  |  LONG: E2(partial) | E3 | E16-E30(DecelBounce)")
    print(sep)
    print(f"  {'Symbol':14s}  {'Strategy':20s}  {'Price':>10}  {'SL':>10}  {'TP':>10}")
    print(f"  {'-'*72}")

    fired = 0
    for sym in symbols:
        try:
            cs1h     = load_candles_db(sym, "1h", CANDLE_LOAD_1H)
            cs4h_self = load_candles_db(sym, "4h", CANDLE_LOAD_4H)
        except Exception as e:
            print(f"  {sym:14s}  ERROR: {e}")
            continue
        sig = compute_signal(sym, cs1h, cs_btc4h, cs4h_self)
        if sig:
            print(f"  {sym:14s}  {sig['strategy']:20s}  "
                  f"{sig['price']:>10.4f}  {sig['sl']:>10.4f}  {sig['tp']:>10.4f}")
            fired += 1
        else:
            price = cs1h[-1]["close"] if cs1h else 0
            print(f"  {sym:14s}  {'--':20s}  {price:>10.4f}  {'--':>10}  {'--':>10}")
    print(sep)
    print(f"  {fired}/{len(symbols)} signals fired\n")


# ── 入口 ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Dimension Trader")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--scan", action="store_true", help="One-time scan and exit")
    parser.add_argument("--symbols", nargs="+", default=None)
    args = parser.parse_args()

    # PID 锁：防止重复启动
    import atexit
    _pid_file = os.path.join(os.path.dirname(__file__), "dimension_trader.pid")
    if not args.scan:
        if os.path.exists(_pid_file):
            with open(_pid_file) as f:
                old_pid = f.read().strip()
            # 检查旧进程是否还活着
            try:
                os.kill(int(old_pid), 0)
                logger.error(f"dimension_trader already running (PID {old_pid}), exit.")
                raise SystemExit(1)
            except (ProcessLookupError, ValueError, OSError):
                pass  # 旧进程已死或Windows不支持signal 0，继续
        with open(_pid_file, "w") as f:
            f.write(str(os.getpid()))
        atexit.register(lambda: os.path.exists(_pid_file) and os.remove(_pid_file))

    syms = args.symbols or ALL_SYMBOLS

    if args.scan:
        scan_once(syms)
        return

    run(syms, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
