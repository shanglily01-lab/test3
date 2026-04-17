"""
均值回归预测器（重构版）
核心逻辑：RSI极值 + 资金费率极端 + EMA50偏离 + 成交量背离
触发条件：评分 >= 65（至少2个信号叠加）
每3小时由 app/main.py 调度运行一次，每次最多新开 5 仓，总持仓上限 8
"""
import pymysql
import pymysql.cursors
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from loguru import logger


class MarketPredictor:
    def __init__(self, db_config: dict, ws_price_service=None):
        self.db_config = db_config
        self.ws_service = ws_price_service

    def _get_conn(self):
        return pymysql.connect(**self.db_config, cursorclass=pymysql.cursors.DictCursor)

    def _get_max_hold_hours(self) -> int:
        """从 system_settings 读取最大持仓时间（小时），默认4小时"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='max_hold_hours'")
            row = cur.fetchone()
            cur.close(); conn.close()
            if row:
                return max(1, int(row['setting_value']))
            return 4
        except Exception:
            return 4

    def _get_trade_switches(self) -> dict:
        """读取 allow_long / allow_short / predictor_max_positions"""
        defaults = {'allow_long': True, 'allow_short': True, 'predictor_max_positions': 15}
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT setting_key, setting_value FROM system_settings "
                "WHERE setting_key IN ('allow_long','allow_short','predictor_max_positions')"
            )
            rows = {r['setting_key']: r['setting_value'] for r in cur.fetchall()}
            cur.close(); conn.close()
            defaults['allow_long'] = str(rows.get('allow_long', '1')) in ('1', 'true', 'True')
            defaults['allow_short'] = str(rows.get('allow_short', '1')) in ('1', 'true', 'True')
            defaults['predictor_max_positions'] = int(rows.get('predictor_max_positions', 15))
        except Exception as e:
            logger.warning(f"[预测器] 读取开关失败，使用默认值: {e}")
        return defaults

    def _get_btc_direction(self, cursor) -> str:
        """
        获取 BTC 宏观方向（基于 1H K线）。
        返回: 'BULLISH' / 'BEARISH' / 'NEUTRAL'
        用于门控山寨币信号方向。
        """
        try:
            k1h = self._fetch_klines(cursor, 'BTC/USDT', '1h', 48)
            if len(k1h) < 26:
                return 'NEUTRAL'
            closes = [float(k['close_price']) for k in k1h]
            highs  = [float(k['high_price'])  for k in k1h]
            lows   = [float(k['low_price'])   for k in k1h]

            ema9  = self._calc_ema(closes, 9)
            ema26 = self._calc_ema(closes, 26)
            rsi   = self._calc_rsi(closes, 14)
            adx   = self._calc_adx(highs, lows, closes, 14)
            macd_hist = self._calc_macd_hist(closes, 8, 21, 5)

            ema_bull = ema9[-1] > ema26[-1] if ema9 and ema26 else False
            ema_bear = ema9[-1] < ema26[-1] if ema9 and ema26 else False
            macd_bull = len(macd_hist) >= 2 and macd_hist[-1] > 0 and macd_hist[-1] >= macd_hist[-2]
            macd_bear = len(macd_hist) >= 2 and macd_hist[-1] < 0 and macd_hist[-1] <= macd_hist[-2]

            # 趋势强度要求：ADX > 18
            trend_ok = adx > 18

            if ema_bull and macd_bull and trend_ok and rsi > 45:
                return 'BULLISH'
            elif ema_bear and macd_bear and trend_ok and rsi < 55:
                return 'BEARISH'
            else:
                return 'NEUTRAL'
        except Exception as e:
            logger.warning(f"[预测器] BTC方向获取失败: {e}")
            return 'NEUTRAL'

    # ──────────────────────────────────────────
    # 指标计算（参考 market_regime_detector.py）
    # ──────────────────────────────────────────

    def _calc_ema(self, closes: List[float], period: int) -> List[float]:
        if len(closes) < period:
            return []
        k = 2 / (period + 1)
        ema = [sum(closes[:period]) / period]
        for price in closes[period:]:
            ema.append(price * k + ema[-1] * (1 - k))
        return ema

    def _calc_rsi(self, closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 50.0
        gains, losses = [], []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            gains.append(max(d, 0))
            losses.append(max(-d, 0))
        ag = sum(gains[-period:]) / period
        al = sum(losses[-period:]) / period
        if al == 0:
            return 100.0
        return 100 - 100 / (1 + ag / al)

    def _calc_adx(self, highs: List[float], lows: List[float],
                  closes: List[float], period: int = 14) -> float:
        if len(closes) < period + 1:
            return 25.0
        tr_list, plus_dm, minus_dm = [], [], []
        for i in range(1, len(closes)):
            tr = max(highs[i] - lows[i],
                     abs(highs[i] - closes[i - 1]),
                     abs(lows[i] - closes[i - 1]))
            tr_list.append(tr)
            pdm = max(0, highs[i] - highs[i - 1]) if highs[i] - highs[i - 1] > lows[i - 1] - lows[i] else 0
            mdm = max(0, lows[i - 1] - lows[i]) if lows[i - 1] - lows[i] > highs[i] - highs[i - 1] else 0
            plus_dm.append(pdm)
            minus_dm.append(mdm)
        if len(tr_list) < period:
            return 25.0
        atr = sum(tr_list[-period:]) / period
        if atr == 0:
            return 25.0
        pdi = sum(plus_dm[-period:]) / period / atr * 100
        mdi = sum(minus_dm[-period:]) / period / atr * 100
        ds = pdi + mdi
        return abs(pdi - mdi) / ds * 100 if ds > 0 else 0.0

    def _calc_macd_hist(self, closes: List[float],
                        fast: int = 8, slow: int = 21, signal: int = 5) -> List[float]:
        """返回MACD柱状图序列（最新在末尾）"""
        if len(closes) < slow + signal:
            return []
        ema_f = self._calc_ema(closes, fast)
        ema_s = self._calc_ema(closes, slow)
        min_len = min(len(ema_f), len(ema_s))
        macd_line = [ema_f[-(min_len - i)] - ema_s[-(min_len - i)] for i in range(min_len)]
        sig_line = self._calc_ema(macd_line, signal)
        hist = [macd_line[-(len(sig_line) - i)] - sig_line[-(len(sig_line) - i)]
                for i in range(len(sig_line))]
        return hist

    # ──────────────────────────────────────────
    # K线数据获取
    # ──────────────────────────────────────────

    def _fetch_klines(self, cursor, symbol: str, timeframe: str, limit: int) -> List[Dict]:
        cursor.execute(
            "SELECT open_price, high_price, low_price, close_price, volume "
            "FROM kline_data "
            "WHERE symbol=%s AND timeframe=%s AND exchange='binance_futures' "
            "ORDER BY open_time DESC LIMIT %s",
            (symbol, timeframe, limit)
        )
        rows = cursor.fetchall()
        return list(reversed(rows))  # 时间从旧到新

    def _get_funding_rate(self, symbol: str, cursor=None) -> Optional[float]:
        """获取最新资金费率（funding_rate_data 表）"""
        try:
            _own_conn = None
            if cursor is None:
                _own_conn = self._get_conn()
                cursor = _own_conn.cursor()
            # 同时匹配 BTC/USDT 和 BTCUSDT 两种格式
            sym2 = symbol.replace('/', '')
            cursor.execute(
                "SELECT funding_rate FROM funding_rate_data "
                "WHERE symbol IN (%s, %s) ORDER BY timestamp DESC LIMIT 1",
                (symbol, sym2)
            )
            row = cursor.fetchone()
            if _own_conn:
                cursor.close(); _own_conn.close()
            return float(row['funding_rate']) if row else None
        except Exception:
            return None

    # ──────────────────────────────────────────
    # 单币分析（均值回归）
    # ──────────────────────────────────────────

    def analyze(self, symbol: str, cursor=None) -> Optional[Dict]:
        """
        均值回归分析：
          信号1  RSI 1H  极值（>72超买/< 28超卖）
          信号2  RSI 15M 极值
          信号3  资金费率极端（>0.08% SHORT / <-0.08% LONG）
          信号4  EMA50 偏离度（>6% SHORT / <-6% LONG）
          信号5  高位/低位量萎缩（成交量背离）
          信号6  RSI 转向确认
        评分 >= 65 且至少2个信号触发才开仓
        """
        _own_conn = None
        try:
            if cursor is None:
                _own_conn = self._get_conn()
                cursor = _own_conn.cursor()

            k1h  = self._fetch_klines(cursor, symbol, '1h', 100)
            k15m = self._fetch_klines(cursor, symbol, '15m', 96)
            funding = self._get_funding_rate(symbol, cursor)

            if _own_conn:
                cursor.close(); _own_conn.close(); cursor = None

            if len(k1h) < 52 or len(k15m) < 20:
                return None

            h1_c = [float(k['close_price']) for k in k1h]
            h1_v = [float(k['volume'])      for k in k1h]
            m15_c = [float(k['close_price']) for k in k15m]

            rsi_1h  = self._calc_rsi(h1_c, 14)
            rsi_15m = self._calc_rsi(m15_c, 14)
            ema50   = self._calc_ema(h1_c, 50)
            current = h1_c[-1]

            score_long:  int = 0
            score_short: int = 0
            hits_long:   int = 0   # 触发的独立信号数
            hits_short:  int = 0
            rl: List[str] = []
            rs: List[str] = []

            # ── 信号1：RSI 1H ──
            if rsi_1h < 20:
                score_long += 50; hits_long += 1; rl.append(f"RSI1H={rsi_1h:.0f}极度超卖")
            elif rsi_1h < 28:
                score_long += 35; hits_long += 1; rl.append(f"RSI1H={rsi_1h:.0f}超卖")
            elif rsi_1h > 80:
                score_short += 50; hits_short += 1; rs.append(f"RSI1H={rsi_1h:.0f}极度超买")
            elif rsi_1h > 72:
                score_short += 35; hits_short += 1; rs.append(f"RSI1H={rsi_1h:.0f}超买")

            # ── 信号2：RSI 15M ──
            if rsi_15m < 20:
                score_long += 25; hits_long += 1; rl.append(f"RSI15M={rsi_15m:.0f}极度超卖")
            elif rsi_15m < 28:
                score_long += 15; hits_long += 1; rl.append(f"RSI15M={rsi_15m:.0f}超卖")
            elif rsi_15m > 80:
                score_short += 25; hits_short += 1; rs.append(f"RSI15M={rsi_15m:.0f}极度超买")
            elif rsi_15m > 72:
                score_short += 15; hits_short += 1; rs.append(f"RSI15M={rsi_15m:.0f}超买")

            # ── 信号3：资金费率 ──
            if funding is not None:
                if funding > 0.0015:
                    score_short += 40; hits_short += 1; rs.append(f"资金费率={funding*100:.3f}%极高")
                elif funding > 0.0008:
                    score_short += 22; hits_short += 1; rs.append(f"资金费率={funding*100:.3f}%偏高")
                elif funding < -0.0015:
                    score_long += 40; hits_long += 1; rl.append(f"资金费率={funding*100:.3f}%极低")
                elif funding < -0.0008:
                    score_long += 22; hits_long += 1; rl.append(f"资金费率={funding*100:.3f}%偏低")

            # ── 信号4：EMA50 偏离度 ──
            if ema50:
                dev = (current - ema50[-1]) / ema50[-1] * 100
                if dev > 10:
                    score_short += 30; hits_short += 1; rs.append(f"偏离EMA50={dev:.1f}%极度偏高")
                elif dev > 6:
                    score_short += 18; hits_short += 1; rs.append(f"偏离EMA50={dev:.1f}%偏高")
                elif dev < -10:
                    score_long += 30; hits_long += 1; rl.append(f"偏离EMA50={dev:.1f}%极度偏低")
                elif dev < -6:
                    score_long += 18; hits_long += 1; rl.append(f"偏离EMA50={dev:.1f}%偏低")

            # ── 信号5：高/低位量萎缩（成交量背离）──
            if len(h1_v) >= 8 and len(h1_c) >= 49:
                recent_vol = sum(h1_v[-4:]) / 4
                prev_vol   = sum(h1_v[-8:-4]) / 4
                if prev_vol > 0:
                    vol_ratio = recent_vol / prev_vol
                    hi48 = max(h1_c[-49:-1])
                    lo48 = min(h1_c[-49:-1])
                    if current >= hi48 * 0.997 and vol_ratio < 0.7:
                        score_short += 22; hits_short += 1; rs.append(f"高位量萎缩({vol_ratio:.2f}x)")
                    elif current <= lo48 * 1.003 and vol_ratio < 0.7:
                        score_long  += 22; hits_long  += 1; rl.append(f"低位量萎缩({vol_ratio:.2f}x)")

            # ── 信号6：RSI 转向确认 ──
            if len(h1_c) > 16:
                rsi_prev = self._calc_rsi(h1_c[:-1], 14)
                if rsi_1h > 70 and rsi_prev > rsi_1h:
                    score_short += 15; hits_short += 1; rs.append(f"RSI顶部转头({rsi_prev:.0f}->{rsi_1h:.0f})")
                elif rsi_1h < 30 and rsi_prev < rsi_1h:
                    score_long  += 15; hits_long  += 1; rl.append(f"RSI底部回升({rsi_prev:.0f}->{rsi_1h:.0f})")

            # ── 决策：评分 >= 65 且至少触发 2 个独立信号 ──
            THRESHOLD = 65
            if score_long >= THRESHOLD and hits_long >= 2 and score_long > score_short:
                direction  = 'BULLISH'
                confidence = min(score_long, 100)
                reasoning  = ' | '.join(rl)
            elif score_short >= THRESHOLD and hits_short >= 2 and score_short > score_long:
                direction  = 'BEARISH'
                confidence = min(score_short, 100)
                reasoning  = ' | '.join(rs)
            else:
                direction  = 'NEUTRAL'
                confidence = max(score_long, score_short)
                reasoning  = f"long={score_long}({hits_long}signals) short={score_short}({hits_short}signals)"

            support    = round(min(h1_c[-24:]), 8) if len(h1_c) >= 24 else 0.0
            resistance = round(max(h1_c[-24:]), 8) if len(h1_c) >= 24 else 0.0

            return {
                'symbol': symbol,
                'direction': direction,
                'confidence': confidence,
                'reasoning': reasoning,
                'trend_1h': direction if direction != 'NEUTRAL' else 'NEUTRAL',
                'trend_15m': 'NEUTRAL',
                'rsi_1h': round(rsi_1h, 2),
                'adx_1h': 0.0,
                'key_level_support': support,
                'key_level_resistance': resistance,
            }

        except Exception as e:
            logger.error(f"[均值回归] {symbol} 分析失败: {e}")
            return None

    # ──────────────────────────────────────────
    # 虚拟回测：平仓 / 开仓
    # ──────────────────────────────────────────

    def _get_current_price(self, cursor, symbol: str) -> Optional[float]:
        """获取当前价格：WebSocket实时价优先，fallback kline_data（30分钟内有效）"""
        # 优先用 WebSocket 实时价
        if self.ws_service:
            p = self.ws_service.get_price(symbol)
            if p and float(p) > 0:
                return float(p)
        # fallback：kline_data，要求数据在30分钟内
        for tf in ('5m', '15m', '1h'):
            cursor.execute(
                "SELECT close_price, open_time FROM kline_data "
                "WHERE symbol=%s AND timeframe=%s AND exchange='binance_futures' "
                "ORDER BY open_time DESC LIMIT 1",
                (symbol, tf)
            )
            row = cursor.fetchone()
            if row:
                age_minutes = (datetime.now().timestamp() - row['open_time'] / 1000) / 60
                if age_minutes <= 30:
                    return float(row['close_price'])
        return None

    def _close_open_backtests(self, cursor, now: datetime) -> int:
        """结算所有超过2.5小时的OPEN虚拟单，计算P&L"""
        cutoff = now - timedelta(hours=5, minutes=30)
        cursor.execute(
            "SELECT id, symbol, direction, entry_price FROM prediction_backtest "
            "WHERE status='OPEN' AND entry_time <= %s",
            (cutoff,)
        )
        rows = cursor.fetchall()
        closed = 0
        for row in rows:
            exit_price = self._get_current_price(cursor, row['symbol'])
            if exit_price is None:
                continue
            entry = float(row['entry_price'])
            if row['direction'] == 'BULLISH':
                pnl_pct = (exit_price - entry) / entry * 5 * 100
            else:
                pnl_pct = (entry - exit_price) / entry * 5 * 100
            pnl_usdt = pnl_pct / 100 * 100  # 100U 本金
            cursor.execute(
                "UPDATE prediction_backtest SET status='CLOSED', exit_price=%s, exit_time=%s, "
                "pnl_pct=%s, pnl_usdt=%s WHERE id=%s",
                (exit_price, now, round(pnl_pct, 4), round(pnl_usdt, 4), row['id'])
            )
            closed += 1
        return closed

    def _open_new_backtests(self, cursor, results: List[Dict], now: datetime) -> int:
        """对 BULLISH/BEARISH 且 confidence>=40 的每个交易对各开一个虚拟单"""
        opened = 0
        for r in results:
            if r['direction'] == 'NEUTRAL' or r['confidence'] < 40:
                continue
            entry_price = self._get_current_price(cursor, r['symbol'])
            if entry_price is None:
                continue
            try:
                cursor.execute(
                    "INSERT INTO prediction_backtest "
                    "(symbol, direction, confidence, entry_price, entry_time) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (r['symbol'], r['direction'], r['confidence'], entry_price, now)
                )
                opened += 1
            except Exception as e:
                logger.error(f"[回测] {r['symbol']} 开虚拟单失败: {e}")
        return opened

    # ──────────────────────────────────────────
    # 真实模拟单开仓（接入 futures_positions）
    # ──────────────────────────────────────────

    def _close_expired_paper_trades(self, cursor, now: datetime) -> int:
        """平掉已到计划平仓时间的 PREDICTOR 模拟单，计算实际P&L"""
        # 优先用 planned_close_time 字段（开仓时按 max_hold_hours 写入，随系统设置变化）
        # fallback：open_time 超过 max_hold_hours 的旧单（兼容 planned_close_time 为空的情况）
        max_hours = self._get_max_hold_hours()
        cutoff = now - timedelta(hours=max_hours)
        cursor.execute(
            "SELECT id, symbol, position_side, entry_price, margin, leverage "
            "FROM futures_positions "
            "WHERE account_id=2 AND status='open' AND source='PREDICTOR' "
            "AND (planned_close_time IS NOT NULL AND NOW() >= planned_close_time "
            "     OR planned_close_time IS NULL AND open_time <= %s)",
            (cutoff,)
        )
        rows = cursor.fetchall()
        closed = 0
        for row in rows:
            exit_price = self._get_current_price(cursor, row['symbol'])
            if not exit_price:
                continue
            entry = float(row['entry_price'])
            margin = float(row['margin'])
            lev = int(row['leverage'])
            if row['position_side'] == 'LONG':
                pnl = (exit_price - entry) / entry * margin * lev
            else:
                pnl = (entry - exit_price) / entry * margin * lev
            cursor.execute(
                "UPDATE futures_positions SET status='closed', close_time=NOW(), "
                "mark_price=%s, realized_pnl=%s, notes='预测器6H到期平仓' WHERE id=%s",
                (exit_price, round(pnl, 4), row['id'])
            )
            # 同步更新关联实盘记录 + 调用交易引擎平仓
            self._sync_close_live(row['id'], row['symbol'], row['position_side'], exit_price)
            logger.info(f"[预测下单] 平仓 {row['symbol']} {row['position_side']} pnl={pnl:+.2f}U")
            closed += 1
        return closed

    def _sync_close_live(self, paper_id: int, symbol: str, side: str, exit_price: float):
        """模拟单平仓时同步平掉对应实盘"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute(
                "SELECT setting_value FROM system_settings WHERE setting_key='live_trading_enabled'"
            )
            r = cur.fetchone()
            live_enabled = r and str(r.get('setting_value', '0')).lower() in ('1', 'true', 'yes')
            if not live_enabled:
                cur.close(); conn.close()
                return
            # 更新DB记录
            cur.execute("""
                UPDATE live_futures_positions
                SET status='CLOSED', close_time=NOW(),
                    notes=CONCAT(IFNULL(notes,''), '|predictor_expire_close')
                WHERE paper_position_id=%s AND status='OPEN'
            """, (paper_id,))
            conn.commit()
            cur.close(); conn.close()
            # 调用交易引擎真实平仓
            from app.services.api_key_service import APIKeyService
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            svc = APIKeyService(self.db_config)
            for ak in svc.get_all_active_api_keys('binance'):
                try:
                    engine = BinanceFuturesEngine(self.db_config, api_key=ak['api_key'], api_secret=ak['api_secret'])
                    res = engine.close_position_by_symbol(symbol=symbol, position_side=side, reason='predictor_6h_expire')
                    if res.get('success'):
                        logger.info(f"[预测下单] 实盘平仓 {ak['account_name']} {symbol} {side} OK")
                    else:
                        logger.warning(f"[预测下单] 实盘平仓 {ak['account_name']} {symbol}: {res.get('error','')}")
                except Exception as e:
                    logger.error(f"[预测下单] 实盘平仓异常 {ak.get('account_name','')} {symbol}: {e}")
        except Exception as e:
            logger.error(f"[预测下单] _sync_close_live 异常: {e}")

    def _open_real_paper_trades(self, cursor, results: List[Dict], now: datetime,
                                btc_direction: str = 'NEUTRAL') -> int:
        """
        取 confidence>=75 的预测，按置信度排序，开真实模拟单到 futures_positions
        - BTC宏观方向门控：BTC BEARISH 时只开空，BTC BULLISH 时只开多，NEUTRAL 多空都开但置信度要求更高
        - 持仓上限：predictor_max_positions（默认15）
        - 读取 allow_long / allow_short 开关
        - 模拟盘 400U x5（1级黑名单 100U，2级黑名单 50U），止损2%，止盈6%
        - source='PREDICTOR' 标识来源
        """
        DEFAULT_MARGIN = 400
        RESTRICTED_MARGIN_L1 = 200  # rating_level=1 限制交易对
        RESTRICTED_MARGIN_L2 = 100  # rating_level=2 严格限制
        MAX_NEW_OPENS = 5            # 每轮最多开 5 仓，防止批量押注
        LEVERAGE = 5
        ACCOUNT_ID = 2
        # 从 system_settings 读取止损止盈，默认 2%/5%
        try:
            _sc = self._get_conn()
            _scur = _sc.cursor()
            _scur.execute("SELECT setting_key, setting_value FROM system_settings WHERE setting_key IN ('stop_loss_pct','take_profit_pct')")
            _rows = {r['setting_key']: r['setting_value'] for r in _scur.fetchall()}
            _scur.close(); _sc.close()
            SL_PCT = float(_rows.get('stop_loss_pct', 0.02))
            TP_PCT = float(_rows.get('take_profit_pct', 0.03))
        except Exception as _e:
            logger.warning(f"[预测器] 读取SL/TP配置失败，使用默认值: {_e}")
            SL_PCT = 0.02
            TP_PCT = 0.05

        # 查询黑名单交易对（1级和2级，分别限制保证金）
        try:
            cursor.execute(
                "SELECT symbol, rating_level FROM trading_symbol_rating WHERE rating_level IN (1, 2)"
            )
            restricted_symbols = {r['symbol']: r['rating_level'] for r in cursor.fetchall()}
        except Exception:
            restricted_symbols = {}

        # 读取开关
        switches = self._get_trade_switches()
        allow_long  = switches['allow_long']
        allow_short = switches['allow_short']
        max_positions = switches['predictor_max_positions']

        # 已开过的持仓
        cursor.execute(
            "SELECT symbol, position_side FROM futures_positions "
            "WHERE account_id=%s AND status='open' AND source='PREDICTOR'",
            (ACCOUNT_ID,)
        )
        existing = {r['symbol']: r['position_side'] for r in cursor.fetchall()}
        current_count = len(existing)

        if current_count >= max_positions:
            logger.info(f"[预测下单] 已达持仓上限 {current_count}/{max_positions}，跳过开新仓")
            return 0

        # 置信度门槛（均值回归：评分>=65即可，NEUTRAL时略高）
        if btc_direction == 'NEUTRAL':
            min_confidence = 65
        else:
            min_confidence = 60

        # 候选：按置信度降序，过滤方向与BTC宏观冲突的信号
        raw_candidates = [
            r for r in results
            if r['direction'] != 'NEUTRAL' and r['confidence'] >= min_confidence
        ]

        filtered_candidates = []
        for r in raw_candidates:
            direction_side = 'LONG' if r['direction'] == 'BULLISH' else 'SHORT'
            # allow_long / allow_short 开关
            if direction_side == 'LONG' and not allow_long:
                continue
            if direction_side == 'SHORT' and not allow_short:
                continue
            # BTC 宏观软门控（均值回归可逆势，但极强信号才允许，confidence>=80）
            if btc_direction == 'BEARISH' and direction_side == 'LONG' and r['confidence'] < 80:
                continue
            if btc_direction == 'BULLISH' and direction_side == 'SHORT' and r['confidence'] < 80:
                continue
            filtered_candidates.append(r)

        candidates = sorted(filtered_candidates, key=lambda x: x['confidence'], reverse=True)

        # 按剩余可开仓数量截断，同时每轮最多新开 MAX_NEW_OPENS 仓
        slots_available = max_positions - current_count
        candidates = candidates[:min(slots_available, MAX_NEW_OPENS)]

        logger.info(
            f"[预测下单] BTC方向={btc_direction}  置信度门槛={min_confidence}"
            f"  候选={len(raw_candidates)}  过滤后={len(filtered_candidates)}"
            f"  可开槽={slots_available}  实际候选={len(candidates)}"
        )

        opened = 0
        for r in candidates:
            symbol = r['symbol']
            direction = 'LONG' if r['direction'] == 'BULLISH' else 'SHORT'

            # 已有同向仓：跳过
            if existing.get(symbol) == direction:
                continue

            # 根据评级确定保证金：1级=100U，2级=50U，默认=400U
            _level = restricted_symbols.get(symbol, 0)
            if _level == 1:
                MARGIN = RESTRICTED_MARGIN_L1
            elif _level == 2:
                MARGIN = RESTRICTED_MARGIN_L2
            else:
                MARGIN = DEFAULT_MARGIN

            # 获取当前价格
            entry_price = self._get_current_price(cursor, symbol)
            if not entry_price:
                continue

            if direction == 'LONG':
                sl = round(entry_price * (1 - SL_PCT), 8)
                tp = round(entry_price * (1 + TP_PCT), 8)
            else:
                sl = round(entry_price * (1 + SL_PCT), 8)
                tp = round(entry_price * (1 - TP_PCT), 8)

            notional = MARGIN * LEVERAGE
            qty = round(notional / entry_price, 6)

            try:
                max_hold_hours = self._get_max_hold_hours()
                planned_close = now + timedelta(hours=max_hold_hours)
                max_hold_minutes = max_hold_hours * 60
                cursor.execute("""
                    INSERT INTO futures_positions
                        (account_id, symbol, position_side, leverage, quantity, notional_value,
                         margin, entry_price, mark_price, stop_loss_price, take_profit_price,
                         stop_loss_pct, take_profit_pct, status, source, entry_reason,
                         open_time, planned_close_time, max_hold_minutes, unrealized_pnl, unrealized_pnl_pct)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'open','PREDICTOR',%s,NOW(),%s,%s,0,0)
                """, (
                    ACCOUNT_ID, symbol, direction, LEVERAGE, qty, round(notional, 2),
                    MARGIN, entry_price, entry_price, sl, tp,
                    SL_PCT * 100, TP_PCT * 100,
                    f"预测器 confidence={r['confidence']} {r['direction']}",
                    planned_close, max_hold_minutes
                ))
                logger.info(f"[预测下单] {symbol} {direction} @ {entry_price:.6g}  "
                            f"SL={sl:.6g}  TP={tp:.6g}  margin={MARGIN}U  confidence={r['confidence']}")
                # 获取刚插入的 paper_position_id，用于实盘同步
                paper_id = cursor.lastrowid
                opened += 1
                # 同步实盘
                self._sync_live(symbol, direction, entry_price, sl, tp,
                                LEVERAGE, MARGIN, paper_id, r['confidence'])
            except Exception as e:
                logger.error(f"[预测下单] {symbol} 开单失败: {e}")

        return opened

    def _sync_live(self, symbol: str, direction: str, entry_price: float,
                   sl: float, tp: float, leverage: int, margin: float,
                   paper_pos_id: int, confidence: int):
        """同步到实盘账号（调用交易引擎真实下单）"""
        try:
            conn = self._get_conn()
            cur = conn.cursor()
            cur.execute("SELECT setting_value FROM system_settings WHERE setting_key='live_trading_enabled'")
            row = cur.fetchone()
            if not (row and str(row['setting_value']) in ('1', 'true')):
                cur.close(); conn.close()
                return
            cur.execute("SELECT COUNT(*) as cnt FROM top_performing_symbols WHERE symbol=%s", (symbol,))
            cnt_row = cur.fetchone()
            cur.close(); conn.close()
            if not (cnt_row and cnt_row['cnt'] > 0):
                logger.debug(f"[预测下单] {symbol} 不在TOP50，跳过实盘同步")
                return
        except Exception as e:
            logger.warning(f"[预测下单] 查询实盘开关/TOP50失败: {e}")
            return

        try:
            from app.services.api_key_service import APIKeyService
            from app.trading.binance_futures_engine import BinanceFuturesEngine
            from decimal import Decimal
            svc = APIKeyService(self.db_config)
            active_keys = svc.get_all_active_api_keys('binance')
        except Exception as e:
            logger.error(f"[预测下单] 获取实盘账号失败: {e}")
            return

        for ak in active_keys:
            try:
                act_margin = float(ak.get('max_position_value') or margin)
                act_lev = int(ak.get('max_leverage') or leverage)
                notional = act_margin * act_lev
                qty = Decimal(str(round(notional / entry_price, 6)))

                engine = BinanceFuturesEngine(
                    self.db_config,
                    api_key=ak['api_key'],
                    api_secret=ak['api_secret']
                )
                result = engine.open_position(
                    account_id=ak['id'],
                    symbol=symbol,
                    position_side=direction,
                    quantity=qty,
                    leverage=act_lev,
                    stop_loss_price=Decimal(str(sl)),
                    take_profit_price=Decimal(str(tp)),
                    source='PREDICTOR',
                    paper_position_id=paper_pos_id
                )
                if result.get('success'):
                    logger.info(f"[预测下单] ✅ 实盘下单成功 {ak['account_name']} {symbol} {direction} confidence={confidence}")
                    try:
                        from app.services.trade_notifier import get_trade_notifier
                        notifier = get_trade_notifier()
                        if notifier:
                            notifier.notify_open_position(
                                symbol=symbol, direction=direction,
                                quantity=float(qty), entry_price=entry_price,
                                leverage=act_lev, stop_loss_price=sl, take_profit_price=tp,
                                margin=act_margin,
                                strategy_name=f'预测器[{ak["account_name"]}] confidence={confidence}'
                            )
                    except Exception: pass
                else:
                    logger.error(f"[预测下单] ❌ 实盘下单失败 {ak['account_name']} {symbol}: {result.get('error','')}")
            except Exception as e:
                logger.error(f"[预测下单] 实盘下单异常 {ak.get('account_name','')} {symbol}: {e}")

    # ──────────────────────────────────────────
    # 批量运行 + 存储
    # ──────────────────────────────────────────

    def run_all(self, symbols: List[str]) -> int:
        # 检查系统开关
        try:
            _chk_conn = self._get_conn()
            _chk_cur = _chk_conn.cursor()
            _chk_cur.execute(
                "SELECT setting_key, setting_value FROM system_settings "
                "WHERE setting_key IN ('predictor_enabled', 'u_futures_trading_enabled')"
            )
            _rows = {r['setting_key']: str(r['setting_value']) for r in _chk_cur.fetchall()}
            _chk_cur.close(); _chk_conn.close()
            if _rows.get('predictor_enabled') in ('0', 'false', 'False'):
                logger.info("[预测] predictor_enabled=0，本轮跳过")
                return 0
            if _rows.get('u_futures_trading_enabled') in ('0', 'false', 'False'):
                logger.info("[预测] u_futures_trading_enabled=0，本轮跳过")
                return 0
        except Exception as e:
            logger.warning(f"[预测] 读取系统开关失败，默认继续: {e}")

        now = datetime.now()
        valid_until = now + timedelta(hours=6)
        saved = 0
        all_results = []

        conn = self._get_conn()
        cursor = conn.cursor()

        # 加载 Level3 永久禁止交易对
        try:
            cursor.execute("SELECT symbol FROM trading_symbol_rating WHERE rating_level >= 3")
            banned = {r['symbol'] for r in cursor.fetchall()}
            symbols = [s for s in symbols if s not in banned]
            if banned:
                logger.debug(f"[预测] Level3黑名单过滤，排除{len(banned)}个交易对")
        except Exception as e:
            logger.warning(f"[预测] 获取Level3黑名单失败: {e}")
            banned = set()

        # ① 先平掉到期的真实模拟单（source=PREDICTOR，持仓超5.5小时）
        try:
            pc = self._close_expired_paper_trades(cursor, now)
            if pc:
                conn.commit()
                logger.info(f"[预测下单] 平仓{pc}个到期模拟单")
        except Exception as e:
            logger.error(f"[预测下单] 平仓到期单失败: {e}")

        # ① 先结算上一轮到期的虚拟单
        try:
            closed = self._close_open_backtests(cursor, now)
            if closed:
                conn.commit()
                # 打印近期回测统计
                cursor.execute(
                    "SELECT COUNT(*) AS cnt, "
                    "SUM(CASE WHEN pnl_usdt>0 THEN 1 ELSE 0 END) AS wins, "
                    "SUM(pnl_usdt) AS total_pnl "
                    "FROM prediction_backtest WHERE status='CLOSED' "
                    "AND exit_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)"
                )
                stat = cursor.fetchone()
                cnt = stat['cnt'] or 0
                wins = stat['wins'] or 0
                total_pnl = stat['total_pnl'] or 0.0
                win_rate = wins / cnt * 100 if cnt > 0 else 0
                logger.info(
                    f"[回测] 结算{closed}单 | 近7日: {cnt}单 胜率{win_rate:.1f}% 总PnL={total_pnl:+.2f}U"
                )
        except Exception as e:
            logger.error(f"[回测] 结算虚拟单失败: {e}")

        # ② 获取 BTC 宏观方向（用于门控山寨币信号）
        btc_direction = self._get_btc_direction(cursor)
        logger.info(f"[预测] BTC宏观方向={btc_direction}，将用于门控开仓方向")

        # ③ 运行预测（复用同一连接，避免每币新开连接耗尽 MySQL 连接数）
        for symbol in symbols:
            result = self.analyze(symbol, cursor=cursor)
            if not result:
                continue
            all_results.append(result)
            try:
                cursor.execute("""
                    INSERT INTO market_prediction
                        (symbol, prediction_time, direction, confidence, reasoning,
                         trend_1h, trend_15m, rsi_1h, adx_1h,
                         key_level_support, key_level_resistance, valid_until)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE
                        prediction_time=%s, direction=%s, confidence=%s, reasoning=%s,
                        trend_1h=%s, trend_15m=%s, rsi_1h=%s, adx_1h=%s,
                        key_level_support=%s, key_level_resistance=%s, valid_until=%s
                """, (
                    symbol, now, result['direction'], result['confidence'], result['reasoning'],
                    result['trend_1h'], result['trend_15m'], result['rsi_1h'], result['adx_1h'],
                    result['key_level_support'], result['key_level_resistance'], valid_until,
                    now, result['direction'], result['confidence'], result['reasoning'],
                    result['trend_1h'], result['trend_15m'], result['rsi_1h'], result['adx_1h'],
                    result['key_level_support'], result['key_level_resistance'], valid_until,
                ))
                saved += 1
            except Exception as e:
                logger.error(f"[预测] {symbol} 存储失败: {e}")

        # ③ 开新的虚拟单
        try:
            opened = self._open_new_backtests(cursor, all_results, now)
            if opened:
                logger.info(f"[回测] 新开{opened}个虚拟单（100U x5）")
        except Exception as e:
            logger.error(f"[回测] 开虚拟单失败: {e}")

        # ④ 开真实模拟单（confidence>=75，上限15仓，BTC宏观门控，带止损2%/止盈6%）
        try:
            real_opened = self._open_real_paper_trades(cursor, all_results, now, btc_direction)
            if real_opened:
                logger.info(f"[预测下单] 新开{real_opened}个模拟单（400U x5，SL2% TP6%）")
        except Exception as e:
            logger.error(f"[预测下单] 开单失败: {e}")

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(f"[预测] 完成 {saved}/{len(symbols)} 个交易对分析，有效期至 {valid_until.strftime('%H:%M')} UTC")
        return saved
