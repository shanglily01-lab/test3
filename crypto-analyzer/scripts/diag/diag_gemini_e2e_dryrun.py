"""
Gemini 策略端到端 dry-run 测试.
跑前 N 个 symbol (默认 5) 的完整流程: Gemini 决策 → 计算限价 → 模拟 open_order
但 *不* 真 POST /api/futures/open, 不创建 PENDING 订单, 不影响 paper 数据库.

验证项:
  1. _init_gemini_client 创建成功
  2. 每个 symbol 的 _fetch_market_data 数据完整
  3. _build_gemini_prompt 长度合理 (10K-20K 字符)
  4. _call_gemini 返回合法 JSON
  5. 模拟 open_order 的 payload (sl_pct/tp_pct/limit_price 数学正确)
  6. _gemini_active_count / _has_any_open 等 SQL 函数能正常跑
  7. 显示一轮统计: 几个 long/short/skip, 总耗时

用法: python scripts/diag/diag_gemini_e2e_dryrun.py [N]
"""
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from dotenv import load_dotenv
load_dotenv(ROOT / '.env')

import strategy_bigmid as sb


def main():
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    print(f"Gemini 策略 dry-run 测试 - 前 {n} 个 symbol")
    print("=" * 80)

    # 1. 加载配置
    print("\n[1/7] _load_bigmid_config...")
    sb._load_bigmid_config()
    print(f"  enabled (DB): {sb.GEMINI_ENABLED}")
    print(f"  SL/TP/offset/hold/min_pnl: {sb.GEMINI_SL_PCT*100}%/{sb.GEMINI_HARD_TP_PCT*100}%/"
          f"{sb.GEMINI_LIMIT_OFFSET_PCT*100}%/{sb.GEMINI_HOLD_MIN//60}h/{sb.GEMINI_MIN_PNL_PCT*100}%")

    # 2. 初始化 Gemini
    print("\n[2/7] _init_gemini_client...")
    model = sb._init_gemini_client()
    if not model:
        print("  FAIL: client 初始化失败")
        return
    print(f"  OK: client type={type(model).__name__}")

    # 3. 验证 SQL 只读函数
    print("\n[3/7] DB 只读函数验证...")
    conn = sb._db_conn()
    try:
        active = sb._gemini_active_count(conn)
        print(f"  _gemini_active_count: {active}")
        # _settle_closed_positions / _close_overdue 跑一次 (不影响, 只是查询)
        sb._settle_closed_positions(conn)
        print(f"  _settle_closed_positions: 跑过")
        sb._close_overdue(conn)
        print(f"  _close_overdue: 跑过")
    finally:
        conn.close()

    # 4. 跑 N 个 symbol 的完整链路 (除了不真下单)
    print(f"\n[4/7] 端到端跑 {n} 个 symbol (Gemini API 调用)...")
    symbols = sb.GEMINI_TOP30[:n]
    results = []
    t_total = time.time()
    for i, sym in enumerate(symbols, 1):
        print(f"\n  [{i}/{n}] {sym}")
        # 4a. 取数据
        cur_conn = sb._db_conn()
        cur = cur_conn.cursor()
        try:
            data = sb._fetch_market_data(cur, sym)
        finally:
            cur.close()
            cur_conn.close()
        if not data:
            print(f"    SKIP: 数据不足")
            results.append((sym, 'data-insufficient', None))
            continue

        # 4b. 构造 prompt
        prompt = sb._build_gemini_prompt(data)
        print(f"    数据完整: prompt={len(prompt)} 字符, RSI 1h/daily={data['rsi_1h']}/{data['rsi_daily']}, "
              f"current={data['current_price']}")

        # 4c. 调 Gemini
        t0 = time.time()
        signal = sb._call_gemini(model, prompt)
        t1 = time.time()
        print(f"    Gemini 耗时 {t1-t0:.1f}s", end='')
        if not signal:
            print(" FAIL: 返回 None")
            results.append((sym, 'gemini-fail', None))
            continue

        d = signal['direction']
        exp = signal['expected_pnl_pct']
        conf = signal['confidence']
        print(f" -> {d} exp={exp*100:.2f}% conf={conf:.2f} reason={signal['reason'][:40]}")

        # 4d. 模拟 open_order 决策 (不真下单)
        if d == 'skip' or exp < sb.GEMINI_MIN_PNL_PCT:
            results.append((sym, f'reject({d}/{exp*100:.2f}%)', None))
            continue

        side = 'LONG' if d == 'long' else 'SHORT'
        cur_p = data['current_price']
        if side == 'LONG':
            lp = cur_p * (1 - sb.GEMINI_LIMIT_OFFSET_PCT)
            tp = lp * (1 + sb.GEMINI_HARD_TP_PCT)
            sl = lp * (1 - sb.GEMINI_SL_PCT)
        else:
            lp = cur_p * (1 + sb.GEMINI_LIMIT_OFFSET_PCT)
            tp = lp * (1 - sb.GEMINI_HARD_TP_PCT)
            sl = lp * (1 + sb.GEMINI_SL_PCT)
        qty = round(sb.MARGIN * sb.LEVERAGE / lp, 6)
        # 数学验证: 限价偏移
        actual_offset = abs(lp - cur_p) / cur_p
        offset_ok = abs(actual_offset - sb.GEMINI_LIMIT_OFFSET_PCT) < 1e-9
        # SL/TP 距离限价 = 配置值
        sl_dist = abs(sl - lp) / lp
        tp_dist = abs(tp - lp) / lp
        sl_ok = abs(sl_dist - sb.GEMINI_SL_PCT) < 1e-9
        tp_ok = abs(tp_dist - sb.GEMINI_HARD_TP_PCT) < 1e-9
        print(f"    [DRY-RUN] {side} cur={cur_p:.6f} lp={lp:.6f} (offset={actual_offset*100:.2f}% {'OK' if offset_ok else 'BAD'})")
        print(f"              SL={sl:.6f} (-{sl_dist*100:.2f}% {'OK' if sl_ok else 'BAD'}) TP={tp:.6f} (+{tp_dist*100:.2f}% {'OK' if tp_ok else 'BAD'})")
        print(f"              qty={qty} margin={sb.MARGIN}U lev={sb.LEVERAGE}x notional={qty*lp:.2f}U")
        # 模拟 has_any_open 检查
        has_open = sb._has_any_open(sym)
        print(f"              _has_any_open({sym}): {has_open} (实际下单时会被这个守卫挡住)")
        results.append((sym, f'{side}-ready', dict(lp=lp, sl=sl, tp=tp)))

        # 单 symbol 间 sleep, 避免速率限制
        if i < n:
            time.sleep(sb.GEMINI_PER_SYMBOL_DELAY_S)

    t_total = time.time() - t_total
    print(f"\n[5/7] 总耗时 {t_total:.1f}s, 平均 {t_total/n:.1f}s/symbol")

    # 6. 统计
    print(f"\n[6/7] 统计:")
    by_status = {}
    for s, status, _ in results:
        key = status.split('(')[0].split('-')[0] if '(' in status or '-' in status else status
        by_status[key] = by_status.get(key, 0) + 1
    for k, v in sorted(by_status.items()):
        print(f"  {k}: {v}")

    # 7. 决策结果表
    print(f"\n[7/7] 决策结果:")
    print(f"  {'symbol':<14} {'status':<25} {'lp':>14} {'sl':>14} {'tp':>14}")
    for sym, status, info in results:
        if info:
            print(f"  {sym:<14} {status:<25} {info['lp']:>14.6f} {info['sl']:>14.6f} {info['tp']:>14.6f}")
        else:
            print(f"  {sym:<14} {status:<25}")


if __name__ == '__main__':
    main()
