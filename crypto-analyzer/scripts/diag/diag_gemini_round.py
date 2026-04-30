"""
诊断 strategy_bigmid Gemini 决策流程: 单 symbol 不下单, 仅
  - 拉市场数据 (15 天日线 / 4 天 1h / 8h 15m+1h)
  - 构造 prompt
  - 调 Gemini API
  - 打印返回 JSON

只读 dimesion 库, 不改 DB, 不下单.
用法:
  python scripts/diag/diag_gemini_round.py [SYMBOL]
  默认 SYMBOL=BTC/USDT
"""
import sys
import os
from pathlib import Path

# 让本脚本能直接 import 项目根的 strategy_bigmid 模块
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
load_dotenv(ROOT / '.env')

import strategy_bigmid as sb


def main():
    sym = sys.argv[1] if len(sys.argv) > 1 else "BTC/USDT"
    print(f"诊断 Gemini 决策流程 - symbol={sym}")
    print("=" * 80)

    # 1. 加载配置
    sb._load_bigmid_config()

    # 2. 初始化 Gemini
    model = sb._init_gemini_client()
    if not model:
        print("ERROR: Gemini 客户端初始化失败, 检查 GEMINI_API_KEY 和 google-generativeai 安装")
        return

    # 3. 拉市场数据
    conn = sb._db_conn()
    cur = conn.cursor()
    try:
        data = sb._fetch_market_data(cur, sym)
    finally:
        cur.close()
        conn.close()
    if not data:
        print(f"ERROR: {sym} 市场数据不足或 K 线数据缺失")
        return

    print(f"\n[市场数据摘要]")
    print(f"  current_price: {data['current_price']}")
    print(f"  24h change: {data['change_24h_pct']}%")
    print(f"  RSI(14, 1h): {data['rsi_1h']}")
    print(f"  RSI(14, daily): {data['rsi_daily']}")
    print(f"  daily 15d 根数: {len(data['daily_15d'])}")
    print(f"  1h_4d 根数: {len(data['h1_4d'])}")
    print(f"  15m_8h 根数: {len(data['m15_8h'])}")
    print(f"  1h_8h 根数: {len(data['h1_8h'])}")

    # 4. 构造 prompt
    prompt = sb._build_gemini_prompt(data)
    print(f"\n[Prompt 长度] {len(prompt)} 字符")
    print(f"[Prompt 前 500 字符]:\n{prompt[:500]}\n...")
    print(f"[Prompt 末 500 字符]:\n...{prompt[-500:]}")

    # 5. 调 Gemini
    print(f"\n[调用 Gemini model={sb.GEMINI_MODEL_NAME}, timeout={sb.GEMINI_API_TIMEOUT_S}s]")
    import time
    t0 = time.time()
    signal = sb._call_gemini(model, prompt)
    t1 = time.time()
    print(f"[耗时 {t1-t0:.2f}s]")

    if not signal:
        print("ERROR: Gemini 返回 None (查 log 看具体错误)")
        return

    print(f"\n[Gemini 决策]")
    print(f"  direction: {signal['direction']}")
    print(f"  expected_pnl_pct: {signal['expected_pnl_pct']:.4f} ({signal['expected_pnl_pct']*100:.2f}%)")
    print(f"  confidence: {signal['confidence']:.2f}")
    print(f"  reason: {signal['reason']}")

    print(f"\n[决策对照]")
    if signal['direction'] == 'skip':
        print(f"  -> SKIP (不下单)")
    elif signal['expected_pnl_pct'] < sb.GEMINI_MIN_PNL_PCT:
        print(f"  -> 预期 {signal['expected_pnl_pct']*100:.2f}% < {sb.GEMINI_MIN_PNL_PCT*100:.0f}% (不下单)")
    else:
        side = 'LONG' if signal['direction'] == 'long' else 'SHORT'
        cur_p = data['current_price']
        if side == 'LONG':
            lp = cur_p * (1 - sb.GEMINI_LIMIT_OFFSET_PCT)
            tp = cur_p * (1 + sb.GEMINI_HARD_TP_PCT)
            sl = cur_p * (1 - sb.GEMINI_SL_PCT)
        else:
            lp = cur_p * (1 + sb.GEMINI_LIMIT_OFFSET_PCT)
            tp = cur_p * (1 - sb.GEMINI_HARD_TP_PCT)
            sl = cur_p * (1 + sb.GEMINI_SL_PCT)
        print(f"  -> 会下单: {side} @ cur={cur_p} lp={lp:.6f} TP={tp:.6f} SL={sl:.6f} hold={sb.GEMINI_HOLD_MIN//60}h")


if __name__ == '__main__':
    main()
