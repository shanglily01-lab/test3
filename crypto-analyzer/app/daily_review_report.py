#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日复盘报告
每天 00:00 UTC 自动运行，分析前一天的市场走势和交易表现，通过 Telegram 推送。

四个分析模块:
  1. 全市场涨跌 TOP 榜
  2. 我们的开单表现（胜率/PnL/亏损单诊断）
  3. 错过的机会（大涨大跌但未开仓）
  4. 策略诊断（近7日参数问题建议）
"""

import sys
import os
import json
import time
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv()

import pymysql
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


# ─── DB 配置（复用 12h_retrospective_analysis 同一模式） ───────────────────────
def _db_config() -> dict:
    return {
        'host':     os.getenv('DB_HOST', 'localhost'),
        'port':     int(os.getenv('DB_PORT', 3306)),
        'user':     os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'binance-data'),
        'charset':  'utf8mb4',
    }


# ─── Telegram 直发（不依赖 TradeNotifier 复杂初始化） ───────────────────────────
def _send_telegram(message: str) -> bool:
    token   = os.getenv('TELEGRAM_BOT_TOKEN', '')
    chat_id = os.getenv('TELEGRAM_CHAT_ID', '')
    if not token or not chat_id:
        print("[WARN] Telegram 未配置 (TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID)")
        return False
    try:
        url  = f"https://api.telegram.org/bot{token}/sendMessage"
        resp = requests.post(url, data={
            'chat_id':    chat_id,
            'text':       message,
            'parse_mode': 'HTML',
        }, timeout=15)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[ERROR] Telegram 发送失败: {e}")
        return False


class DailyReviewReport:
    """每日复盘报告生成器"""

    def __init__(self, period_start: Optional[datetime] = None, period_end: Optional[datetime] = None):
        self.db_cfg = _db_config()
        # 默认：昨天 00:00 ~ 今天 00:00 UTC
        now = datetime.now()
        self.period_end   = period_end   or now.replace(hour=0, minute=0, second=0, microsecond=0)
        self.period_start = period_start or (self.period_end - timedelta(days=1))
        self.date_label   = self.period_end.strftime('%Y-%m-%d')

    def _conn(self):
        return pymysql.connect(**self.db_cfg, cursorclass=pymysql.cursors.DictCursor)

    # ──────────────────────────────────────────────────────────────────────────
    # 模块1: 全市场涨跌 TOP 榜
    # ──────────────────────────────────────────────────────────────────────────
    def analyze_market_movers(self, traded_symbols: set) -> dict:
        """
        从 kline_data 1h K线计算全市场24h涨跌幅，
        返回 TOP8 涨幅 / TOP8 跌幅 及市场格局统计。
        price_stats_24h 数据过时且覆盖不全，不再使用。
        """
        import time as _time
        now_ms     = int(_time.time() * 1000)
        ts_2h_ago  = now_ms - 2  * 3600 * 1000   # 取最新1h收盘
        ts_22h_ago = now_ms - 22 * 3600 * 1000   # 24h前窗口上限
        ts_26h_ago = now_ms - 26 * 3600 * 1000   # 24h前窗口下限

        conn = self._conn()
        try:
            cur = conn.cursor()
            # 最新1h K线（每个symbol取最大open_time）
            cur.execute("""
                SELECT k.symbol, k.close_price as current_price, k.volume as volume_1h
                FROM kline_data k
                INNER JOIN (
                    SELECT symbol, MAX(open_time) as max_ot
                    FROM kline_data
                    WHERE timeframe='1h' AND open_time >= %s
                    GROUP BY symbol
                ) lat ON k.symbol=lat.symbol AND k.open_time=lat.max_ot AND k.timeframe='1h'
            """, (ts_2h_ago,))
            new_rows = {r['symbol']: r for r in cur.fetchall()}

            # 约24h前的1h K线（22h~26h前窗口内取最近的）
            cur.execute("""
                SELECT k.symbol, k.close_price as price_24h_ago
                FROM kline_data k
                INNER JOIN (
                    SELECT symbol, MAX(open_time) as max_ot
                    FROM kline_data
                    WHERE timeframe='1h' AND open_time >= %s AND open_time <= %s
                    GROUP BY symbol
                ) old ON k.symbol=old.symbol AND k.open_time=old.max_ot AND k.timeframe='1h'
            """, (ts_26h_ago, ts_22h_ago))
            old_rows = {r['symbol']: r for r in cur.fetchall()}
        finally:
            conn.close()

        if not new_rows:
            return {'top_gainers': [], 'top_losers': [], 'up_count': 0, 'down_count': 0, 'total': 0}

        all_coins = []
        for sym, nr in new_rows.items():
            or_ = old_rows.get(sym)
            if not or_:
                continue  # 没有24h前数据，跳过
            cur_price  = float(nr['current_price'] or 0)
            old_price  = float(or_['price_24h_ago'] or 0)
            if old_price <= 0:
                continue
            chg = (cur_price - old_price) / old_price * 100
            all_coins.append({
                'symbol':        sym,
                'current_price': cur_price,
                'change_pct':    round(chg, 2),
                'has_trade':     sym in traded_symbols,
            })

        all_coins.sort(key=lambda x: x['change_pct'], reverse=True)
        up_count   = sum(1 for c in all_coins if c['change_pct'] > 0)
        down_count = sum(1 for c in all_coins if c['change_pct'] < 0)

        # 只取真正下跌的币种，按跌幅绝对值降序，最多8个
        losers = [c for c in all_coins if c['change_pct'] < 0]
        top_losers = list(reversed(losers[-8:])) if len(losers) >= 8 else list(reversed(losers))

        return {
            'top_gainers': all_coins[:8],
            'top_losers':  top_losers,
            'up_count':    up_count,
            'down_count':  down_count,
            'total':       len(all_coins),
        }

    # ──────────────────────────────────────────────────────────────────────────
    # 模块2: 开单表现
    # ──────────────────────────────────────────────────────────────────────────
    def analyze_our_trades(self) -> dict:
        """分析分析区间内的开单表现（已平仓 + 仍在持仓）"""
        conn = self._conn()
        try:
            cur = conn.cursor()
            # 昨天开仓的所有单（含还未平仓的）
            cur.execute("""
                SELECT symbol, position_side, entry_price, realized_pnl,
                       unrealized_pnl, entry_score, signal_components,
                       notes, open_time, close_time, holding_hours,
                       status, account_id
                FROM futures_positions
                WHERE open_time >= %s AND open_time < %s
                  AND account_id IN (2, 3)
                ORDER BY open_time ASC
            """, (self.period_start, self.period_end))
            trades = cur.fetchall()
        finally:
            conn.close()

        if not trades:
            return {
                'total': 0, 'closed': 0, 'open': 0,
                'wins': 0, 'losses': 0, 'win_rate': 0,
                'total_pnl': 0.0,
                'long_total': 0, 'long_wins': 0,
                'short_total': 0, 'short_wins': 0,
                'best_trade': None, 'worst_trade': None,
                'avg_hold_h': 0.0,
                'loss_list': [], 'win_list': [],
                'traded_symbols': set(),
            }

        closed = [t for t in trades if t['status'] == 'closed']
        open_p  = [t for t in trades if t['status'] != 'closed']

        def pnl(t):
            return float(t['realized_pnl'] or 0) + (float(t['unrealized_pnl'] or 0) if t['status'] != 'closed' else 0)

        total_pnl = sum(pnl(t) for t in trades)
        wins      = [t for t in closed if pnl(t) >= 0]
        losses    = [t for t in closed if pnl(t) < 0]
        win_rate  = (len(wins) / len(closed) * 100) if closed else 0

        longs  = [t for t in closed if t['position_side'] == 'LONG']
        shorts = [t for t in closed if t['position_side'] == 'SHORT']
        long_wins  = sum(1 for t in longs  if pnl(t) >= 0)
        short_wins = sum(1 for t in shorts if pnl(t) >= 0)

        def _hold_h(t):
            h = float(t.get('holding_hours') or 0)
            if h > 0:
                return h
            ot = t.get('open_time')
            ct = t.get('close_time')
            if ot and ct:
                delta = (ct - ot).total_seconds() if hasattr(ct - ot, 'total_seconds') else 0
                return delta / 3600.0
            return 0.0

        hold_hours = [_hold_h(t) for t in closed]
        hold_hours = [h for h in hold_hours if h > 0]
        avg_hold_h = sum(hold_hours) / len(hold_hours) if hold_hours else 0

        # 最大盈/亏单
        if closed:
            best  = max(closed, key=pnl)
            worst = min(closed, key=pnl)
        else:
            best = worst = None

        # 亏损单列表（附诊断信息）
        loss_list = []
        for t in sorted(losses, key=pnl):
            comps = {}
            try:
                comps = json.loads(t['signal_components'] or '{}')
            except Exception:
                pass
            # 从 notes 推断平仓原因
            note = (t['notes'] or '').lower()
            reason = ''
            if 'stop_loss' in note or 'sl' in note:
                reason = 'stop_loss'
            elif 'take_profit' in note or 'tp' in note:
                reason = 'take_profit'
            elif 'timeout' in note or '超时' in note:
                reason = 'timeout'
            loss_list.append({
                'symbol':     t['symbol'],
                'side':       t['position_side'],
                'pnl':        pnl(t),
                'score':      t['entry_score'],
                'reason':     reason,
                'open_time':  t['open_time'],
                'components': list(comps.keys()),
            })

        win_list = []
        for t in sorted(wins, key=pnl, reverse=True)[:5]:
            win_list.append({
                'symbol': t['symbol'],
                'side':   t['position_side'],
                'pnl':    pnl(t),
            })

        return {
            'total':        len(trades),
            'closed':       len(closed),
            'open':         len(open_p),
            'wins':         len(wins),
            'losses':       len(losses),
            'win_rate':     win_rate,
            'total_pnl':    total_pnl,
            'long_total':   len(longs),
            'long_wins':    long_wins,
            'short_total':  len(shorts),
            'short_wins':   short_wins,
            'best_trade':   {'symbol': best['symbol'], 'side': best['position_side'], 'pnl': pnl(best)} if best else None,
            'worst_trade':  {'symbol': worst['symbol'], 'side': worst['position_side'], 'pnl': pnl(worst)} if worst else None,
            'avg_hold_h':   avg_hold_h,
            'loss_list':    loss_list,
            'win_list':     win_list,
            'traded_symbols': {t['symbol'] for t in trades},
        }

    # ──────────────────────────────────────────────────────────────────────────
    # 模块3: 错过的机会
    # ──────────────────────────────────────────────────────────────────────────
    def analyze_missed_opportunities(self, movers: dict, traded_symbols: set) -> list:
        """
        涨跌幅 >5% 但当天没有开仓的币种。
        返回列表，按绝对涨跌幅降序。
        """
        threshold = 5.0
        missed = []
        for coin in movers['top_gainers'] + movers['top_losers']:
            if abs(coin['change_pct']) < threshold:
                continue
            if coin['symbol'] in traded_symbols:
                continue
            # 去重（top_gainers 和 top_losers 可能重叠）
            if coin not in missed:
                missed.append(coin)

        missed.sort(key=lambda x: abs(x['change_pct']), reverse=True)
        return missed

    # ──────────────────────────────────────────────────────────────────────────
    # 模块4: 策略诊断（近7日）
    # ──────────────────────────────────────────────────────────────────────────
    def diagnose_strategy(self, missed_count: int) -> dict:
        """
        基于近7日已平仓订单，统计胜率、止损比例、各信号组件胜率，生成文字建议。
        """
        conn = self._conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                SELECT position_side, realized_pnl, entry_score,
                       signal_components, holding_hours, notes,
                       open_time, close_time
                FROM futures_positions
                WHERE close_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
                  AND status = 'closed'
                  AND account_id IN (2, 3)
            """)
            rows = cur.fetchall()
        finally:
            conn.close()

        if not rows:
            return {
                'total': 0, 'win_rate': 0,
                'long_win_rate': 0, 'short_win_rate': 0,
                'avg_hold_h': 0, 'stop_loss_pct': 0,
                'comp_stats': {}, 'tips': ['近7日暂无已平仓数据'],
            }

        def pnl(r): return float(r['realized_pnl'] or 0)

        wins   = [r for r in rows if pnl(r) >= 0]
        losses = [r for r in rows if pnl(r) < 0]
        total  = len(rows)
        win_rate = len(wins) / total * 100

        longs  = [r for r in rows if r['position_side'] == 'LONG']
        shorts = [r for r in rows if r['position_side'] == 'SHORT']
        long_win_rate  = sum(1 for r in longs  if pnl(r) >= 0) / len(longs)  * 100 if longs  else 0
        short_win_rate = sum(1 for r in shorts if pnl(r) >= 0) / len(shorts) * 100 if shorts else 0

        def _hold_hours(r):
            h = float(r.get('holding_hours') or 0)
            if h > 0:
                return h
            # fallback: 用 open_time/close_time 计算
            ot = r.get('open_time')
            ct = r.get('close_time')
            if ot and ct:
                delta = (ct - ot).total_seconds() if hasattr(ct - ot, 'total_seconds') else 0
                return delta / 3600.0
            return 0.0

        hold_h = [_hold_hours(r) for r in rows]
        hold_h = [h for h in hold_h if h > 0]
        avg_hold_h = sum(hold_h) / len(hold_h) if hold_h else 0

        # 止损单识别：从 notes 字段推断
        stop_loss_count = sum(1 for r in rows
                              if ('stop_loss' in (r['notes'] or '').lower() or
                                  'sl ' in (r['notes'] or '').lower()))
        stop_loss_pct   = stop_loss_count / total * 100

        # 信号组件胜率统计
        comp_wins  = {}  # comp -> wins count
        comp_total = {}  # comp -> total count
        for r in rows:
            try:
                comps = json.loads(r['signal_components'] or '{}')
            except Exception:
                comps = {}
            for comp in comps:
                comp_total[comp] = comp_total.get(comp, 0) + 1
                if pnl(r) >= 0:
                    comp_wins[comp] = comp_wins.get(comp, 0) + 1

        comp_stats = {}
        for comp, cnt in comp_total.items():
            wr = comp_wins.get(comp, 0) / cnt * 100
            comp_stats[comp] = (cnt, wr)

        # 生成建议文字
        tips = []
        if short_win_rate < 40 and len(shorts) >= 5:
            tips.append(f"SHORT胜率偏低({short_win_rate:.0f}%)，建议提高SHORT阈值5-10分")
        if long_win_rate < 40 and len(longs) >= 5:
            tips.append(f"LONG胜率偏低({long_win_rate:.0f}%)，建议检查开多条件或降低阈值")
        if stop_loss_pct > 45:
            tips.append(f"止损占比{stop_loss_pct:.0f}%偏高，建议检查入场时机或适当放宽止损距离")
        if avg_hold_h < 2 and total >= 5:
            tips.append(f"平均持仓{avg_hold_h:.1f}h过短，可能入场位置不佳或止损过紧")
        for comp, (cnt, wr) in comp_stats.items():
            if cnt >= 5 and wr < 30:
                tips.append(f"{comp} 信号胜率仅{wr:.0f}%({cnt}次)，建议降低权重")
        if missed_count >= 5:
            tips.append(f"昨天错过{missed_count}个大行情，市场活跃，可检查入场阈值是否偏高")
        if not tips:
            tips.append("近期策略参数表现正常，无明显异常")

        return {
            'total':           total,
            'win_rate':        win_rate,
            'long_win_rate':   long_win_rate,
            'short_win_rate':  short_win_rate,
            'avg_hold_h':      avg_hold_h,
            'stop_loss_pct':   stop_loss_pct,
            'comp_stats':      comp_stats,
            'tips':            tips,
        }

    # ──────────────────────────────────────────────────────────────────────────
    # 消息拼装
    # ──────────────────────────────────────────────────────────────────────────
    def _build_msg1_market(self, movers: dict) -> str:
        up = movers['up_count']
        dn = movers['down_count']
        total = movers['total']
        if total == 0:
            return "<b>市场数据暂不可用</b>"

        if up > dn * 2:
            sentiment = "强势多头"
        elif up > dn:
            sentiment = "多头"
        elif dn > up * 2:
            sentiment = "强势空头"
        elif dn > up:
            sentiment = "空头"
        else:
            sentiment = "多空分歧"

        lines = []
        lines.append(f"<b>📊 每日复盘 {self.date_label}</b>")
        lines.append(f"区间: {self.period_start.strftime('%m/%d %H:%M')} ~ {self.period_end.strftime('%m/%d %H:%M')} UTC\n")
        lines.append(f"<b>🌐 市场格局:</b> {sentiment}  |  上涨{up}家 / 下跌{dn}家 / 共{total}家\n")

        lines.append("<b>📈 涨幅 TOP8:</b>")
        for c in movers['top_gainers']:
            tag  = "  [有仓✓]" if c['has_trade'] else ""
            sym  = c['symbol'].replace('/USDT', '').replace('/USD', '')
            lines.append(f"  {sym:<10} <b>+{c['change_pct']:.1f}%</b>{tag}")

        lines.append("")
        lines.append("<b>📉 跌幅 TOP8:</b>")
        for c in movers['top_losers']:
            tag  = "  [有仓✓]" if c['has_trade'] else ""
            sym  = c['symbol'].replace('/USDT', '').replace('/USD', '')
            lines.append(f"  {sym:<10} <b>{c['change_pct']:.1f}%</b>{tag}")

        return "\n".join(lines)

    def _build_msg2_trades(self, trades: dict) -> str:
        lines = []
        lines.append(f"<b>💼 开单复盘 ({self.period_start.strftime('%m/%d')})</b>")

        if trades['total'] == 0:
            lines.append("昨日无开仓记录")
            return "\n".join(lines)

        pnl_str = f"+{trades['total_pnl']:.1f}U" if trades['total_pnl'] >= 0 else f"{trades['total_pnl']:.1f}U"
        wr_str  = f"{trades['win_rate']:.0f}%" if trades['closed'] > 0 else "—"
        lines.append(f"共 {trades['total']}笔  |  胜率 {wr_str}  |  总盈亏 <b>{pnl_str}</b>")
        if trades['open'] > 0:
            lines.append(f"(已平{trades['closed']}笔，持仓中{trades['open']}笔)")

        # LONG / SHORT 分拆
        if trades['long_total'] > 0 or trades['short_total'] > 0:
            lines.append("")
            if trades['long_total'] > 0:
                lwr = f"{trades['long_wins']}/{trades['long_total']} ({trades['long_wins']/trades['long_total']*100:.0f}%)"
                lines.append(f"LONG  {lwr}")
            if trades['short_total'] > 0:
                swr = f"{trades['short_wins']}/{trades['short_total']} ({trades['short_wins']/trades['short_total']*100:.0f}%)"
                lines.append(f"SHORT {swr}")

        if trades['best_trade']:
            b = trades['best_trade']
            lines.append(f"\n最佳: {b['symbol']} {b['side']} <b>+{b['pnl']:.1f}U</b>")
        if trades['worst_trade'] and trades['worst_trade']['pnl'] < 0:
            w = trades['worst_trade']
            lines.append(f"最差: {w['symbol']} {w['side']} <b>{w['pnl']:.1f}U</b>")

        if trades['avg_hold_h'] > 0:
            lines.append(f"平均持仓: {trades['avg_hold_h']:.1f}h")

        # 亏损单诊断
        if trades['loss_list']:
            lines.append(f"\n<b>亏损单诊断 ({trades['losses']}笔):</b>")
            for t in trades['loss_list'][:8]:
                sym   = t['symbol'].replace('/USDT', '').replace('/USD', '')
                score  = f"score={t['score']}" if t['score'] else ""
                reason = t['reason']
                comps  = ",".join(t['components'][:3]) if t['components'] else ""
                detail = "  |  ".join(filter(None, [score, reason, comps]))
                lines.append(f"  ❌ {sym} {t['side']} <b>{t['pnl']:.1f}U</b>  {detail}")

        return "\n".join(lines)

    def _build_msg3_missed_strategy(self, missed: list, diag: dict) -> str:
        lines = []

        # 错过的机会
        lines.append(f"<b>🚨 错过的机会 (涨跌>5%未开仓)</b>")
        if not missed:
            lines.append("  无——昨日大波动品种已全部覆盖")
        else:
            lines.append(f"共 {len(missed)} 个:\n")
            for c in missed[:10]:
                sym = c['symbol'].replace('/USDT', '').replace('/USD', '')
                sign = "+" if c['change_pct'] > 0 else ""
                lines.append(f"  {sym:<10} {sign}{c['change_pct']:.1f}%")
            if len(missed) > 10:
                lines.append(f"  ...还有 {len(missed)-10} 个")

        # 策略诊断
        lines.append("")
        lines.append(f"<b>🔧 策略诊断 (近7日，共{diag['total']}笔)</b>")
        if diag['total'] == 0:
            lines.append("  暂无数据")
        else:
            lines.append(f"整体胜率: {diag['win_rate']:.0f}%  |  LONG: {diag['long_win_rate']:.0f}%  |  SHORT: {diag['short_win_rate']:.0f}%")
            lines.append(f"平均持仓: {diag['avg_hold_h']:.1f}h  |  止损占比: {diag['stop_loss_pct']:.0f}%")

            # 信号组件胜率（只展示出现≥5次的）
            notable = [(c, cnt, wr) for c, (cnt, wr) in diag['comp_stats'].items() if cnt >= 5]
            notable.sort(key=lambda x: x[1], reverse=True)
            if notable:
                lines.append("\n信号组件表现:")
                for comp, cnt, wr in notable[:6]:
                    bar = "✅" if wr >= 50 else ("⚠️" if wr >= 35 else "❌")
                    lines.append(f"  {bar} {comp:<25} {wr:.0f}% ({cnt}次)")

        # 建议
        if diag['tips']:
            lines.append("\n<b>建议:</b>")
            for tip in diag['tips']:
                lines.append(f"  → {tip}")

        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────────────────────
    # 数据持久化
    # ──────────────────────────────────────────────────────────────────────────
    def save_to_db(self, movers: dict, trades: dict, missed: list, diag: dict):
        """将分析结果写入 daily_review_reports + daily_review_signal_analysis"""
        date_key = self.period_start.date()

        # capture metrics（只统计涨跌>5%的大行情）
        big_movers = [c for c in movers['top_gainers'] + movers['top_losers']
                      if abs(c['change_pct']) > 5]
        # 去重（top_gainers/top_losers 可能重叠）
        seen = set()
        unique_big = []
        for c in big_movers:
            if c['symbol'] not in seen:
                seen.add(c['symbol'])
                unique_big.append(c)
        total_opp = len(unique_big)
        captured  = total_opp - len(missed)
        rate      = (captured / total_opp * 100) if total_opp > 0 else 100.0

        # report_json（与 auto_parameter_optimizer.py 兼容结构）
        report_json = {
            'date':                str(date_key),
            'capture_rate':        round(rate, 2),
            'total_opportunities': total_opp,
            'captured_count':      captured,
            'missed_count':        len(missed),
            'missed_opportunities': [
                {
                    'symbol':          c['symbol'],
                    'price_change_pct': c['change_pct'],
                    'move_type':       'pump' if c['change_pct'] > 0 else 'dump',
                    'timeframe':       '1d',
                }
                for c in missed[:20]
            ],
            'signal_performances': [
                {
                    'signal_type': comp,
                    'trade_count': cnt,
                    'win_rate':    round(wr / 100.0, 4),
                    'total_pnl':   0,
                }
                for comp, (cnt, wr) in diag['comp_stats'].items()
            ],
            'market_overview': {
                'up_count':   movers['up_count'],
                'down_count': movers['down_count'],
                'total':      movers['total'],
            },
            'trading_summary': {
                'total':           trades['total'],
                'win_rate':        round(trades['win_rate'], 2),
                'total_pnl':       round(trades['total_pnl'], 2),
                'long_total':      trades.get('long_total', 0),
                'short_total':     trades.get('short_total', 0),
                'long_win_rate':   round(trades['long_wins'] / trades['long_total'] * 100, 2)
                                   if trades.get('long_total') else 0,
                'short_win_rate':  round(trades['short_wins'] / trades['short_total'] * 100, 2)
                                   if trades.get('short_total') else 0,
            },
            'strategy_diagnosis': diag['tips'],
        }

        conn = self._conn()
        try:
            cur = conn.cursor()

            # 主表 upsert
            rj = json.dumps(report_json, ensure_ascii=False)
            cur.execute("""
                INSERT INTO daily_review_reports
                    (date, report_json, total_opportunities, captured_count, missed_count, capture_rate)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    report_json=%s, total_opportunities=%s,
                    captured_count=%s, missed_count=%s, capture_rate=%s, updated_at=NOW()
            """, (
                date_key, rj, total_opp, captured, len(missed), rate,
                rj, total_opp, captured, len(missed), rate,
            ))

            # 信号组件明细
            if diag['comp_stats']:
                cur.execute("DELETE FROM daily_review_signal_analysis WHERE review_date = %s", (date_key,))
                for comp, (cnt, wr) in diag['comp_stats'].items():
                    wins = round(cnt * wr / 100)
                    cur.execute("""
                        INSERT INTO daily_review_signal_analysis
                            (review_date, signal_type, total_trades, win_trades, loss_trades,
                             win_rate, avg_pnl, long_trades, short_trades,
                             avg_holding_minutes, captured_opportunities)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (date_key, comp, cnt, wins, cnt - wins, round(wr / 100.0, 4), 0.0, 0, 0, 0.0, 0))

            conn.commit()
            print(f"  [DB] 复盘数据存储完成 (date={date_key}, opp={total_opp}, captured={captured}, rate={rate:.1f}%)")
        except Exception as e:
            print(f"  [DB] 存储失败: {e}")
        finally:
            conn.close()

    # ──────────────────────────────────────────────────────────────────────────
    # 自动权重优化
    # ──────────────────────────────────────────────────────────────────────────
    def run_weight_optimization(self) -> dict:
        """调用 ScoringWeightOptimizer，基于近7日数据自动调整信号组件权重"""
        try:
            from app.services.scoring_weight_optimizer import ScoringWeightOptimizer
            # 不传 charset，避免与 optimizer 内部 connect() 重复传参
            opt_cfg = {k: v for k, v in self.db_cfg.items() if k != 'charset'}
            optimizer = ScoringWeightOptimizer(opt_cfg)
            result = optimizer.adjust_weights(dry_run=False)
            print(f"  [OPT] 权重优化完成: 调整{len(result.get('adjusted', []))}个, 跳过{len(result.get('skipped', []))}个")
            return result
        except Exception as e:
            print(f"  [OPT] 权重优化失败: {e}")
            return {'adjusted': [], 'skipped': [], 'error': str(e)}

    def _build_msg4_optimization(self, opt_result: dict) -> str:
        import html as _html
        lines = ["<b>信号权重自动优化</b>"]
        adjusted = opt_result.get('adjusted', [])
        skipped  = opt_result.get('skipped', [])
        error    = opt_result.get('error')

        if error:
            lines.append(f"优化失败: {_html.escape(str(error)[:200])}")
            return "\n".join(lines)

        n_analyzed = len(adjusted) + len(skipped)
        lines.append(f"分析了 {n_analyzed} 个信号组件（近7日数据）\n")

        if adjusted:
            lines.append(f"<b>调整 {len(adjusted)} 个组件权重:</b>")
            for item in adjusted[:12]:
                comp  = _html.escape(str(item.get('component', '')))
                side  = item.get('side', '')
                old_w = item.get('old_weight', 0)
                new_w = item.get('new_weight', 0)
                perf  = item.get('performance_score', 0)
                wr    = item.get('win_rate', 0)
                arrow = "[+]" if new_w > old_w else "[-]"
                lines.append(
                    f"  {arrow} {comp} ({side}) "
                    f"{old_w:.0f} -&gt; <b>{new_w:.0f}</b>  "
                    f"win={wr*100:.0f}% score={perf:.0f}"
                )
        else:
            lines.append("本次无权重变动（数据不足或各组件表现正常）")

        if skipped:
            lines.append(f"\n跳过 {len(skipped)} 个组件（近7日样本不足5笔）")

        return "\n".join(lines)

    # ──────────────────────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────────────────────
    def run(self):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} UTC] 每日复盘报告开始生成...")

        # Step 1: 开单分析（先拿交易过的 symbols）
        trades = self.analyze_our_trades()
        traded_symbols = trades['traded_symbols']
        print(f"  交易记录: {trades['total']}笔 (昨日)")

        # Step 2: 市场概览
        movers = self.analyze_market_movers(traded_symbols)
        print(f"  市场数据: {movers['total']}个币种")

        # Step 3: 错过的机会
        missed = self.analyze_missed_opportunities(movers, traded_symbols)
        print(f"  错过机会: {len(missed)}个")

        # Step 4: 策略诊断
        diag = self.diagnose_strategy(len(missed))
        print(f"  策略诊断: 近7日{diag['total']}笔")

        # Step 5: 持久化到数据库（供历史学习使用）
        self.save_to_db(movers, trades, missed, diag)

        # Step 6: 自动权重优化（基于近7日信号组件表现）
        opt_result = self.run_weight_optimization()

        # 组装并发送4条消息
        msg1 = self._build_msg1_market(movers)
        msg2 = self._build_msg2_trades(trades)
        msg3 = self._build_msg3_missed_strategy(missed, diag)
        msg4 = self._build_msg4_optimization(opt_result)

        ok1 = _send_telegram(msg1)
        time.sleep(1)
        ok2 = _send_telegram(msg2)
        time.sleep(1)
        ok3 = _send_telegram(msg3)
        time.sleep(1)
        ok4 = _send_telegram(msg4)

        status = "成功" if (ok1 and ok2 and ok3 and ok4) else \
            f"部分失败(msg1={ok1} msg2={ok2} msg3={ok3} msg4={ok4})"
        print(f"[完成] Telegram 推送 {status}")


if __name__ == '__main__':
    report = DailyReviewReport()
    report.run()
