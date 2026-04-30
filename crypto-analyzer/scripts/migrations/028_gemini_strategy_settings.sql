-- 028_gemini_strategy_settings.sql (2026-04-30)
-- 在 system_settings 表插入 strategy_bigmid (Gemini AI 决策版) 的默认参数.
--
-- 背景:
--   2026-04-30 strategy_bigmid 整体改造为 Gemini LLM 决策策略.
--   原 BIG (whale-entry) + MID (chase/dump) 全删, 改为每 6h 给 top-30 大币种发结构化
--   市场数据 (15 天日线 / 4 天 1h / 8h 15m+1h, RSI/成交量), 让 Google Gemini 给
--   long/short/skip 建议 + 预期 PnL. 满足条件下限价单 cur ± 0.5%, 持仓 6h, TP 3% / SL 2%.
--
-- 读取方:
--   strategy_bigmid.py 的 _load_bigmid_config 在进程启动时一次性读 (改完需重启策略进程).
--
-- 灰度顺序:
--   1. 跑 migration (默认 enabled=0, 新代码运行但 6h 询问轮不触发, 仅持仓监控)
--   2. 重启 strategy_bigmid 进程拉新代码
--   3. 跑 diag_gemini_round.py BTC/USDT 单 symbol 验证 Gemini API 可达 / JSON 输出正常
--   4. UPDATE system_settings SET setting_value='1' WHERE setting_key='gemini_strategy_enabled', 重启
--   5. 观察 log: 启动后立即跑一轮, 看 28 个 symbol 的 Gemini 输出. paper 跑 3-7 天看 PnL
--
-- 幂等:
--   ON DUPLICATE KEY UPDATE 只刷 description / updated_by, 不动 setting_value.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('gemini_strategy_enabled', '0',
   'strategy_bigmid Gemini AI 策略总开关. 0=只跑持仓监控, 6h 询问轮不触发; 1=每 6h 调 Gemini 决策入场. 默认 OFF, 改完重启策略进程.',
   'migration_028'),
  ('gemini_sl_pct', '0.02',
   'Gemini 策略止损比例 (开仓价的 2%).',
   'migration_028'),
  ('gemini_tp_pct', '0.03',
   'Gemini 策略硬止盈比例 (开仓价的 3%).',
   'migration_028'),
  ('gemini_limit_offset_pct', '0.005',
   'Gemini 策略限价偏移: LONG 挂在 cur*(1-0.005), SHORT 挂在 cur*(1+0.005).',
   'migration_028'),
  ('gemini_hold_hours', '6',
   'Gemini 策略持仓上限 (小时, 默认 6). 超时由 _close_overdue 主动平仓.',
   'migration_028'),
  ('gemini_min_pnl_pct', '0.01',
   'Gemini 预期 PnL 阈值 (默认 0.01 = 1%). LLM 返回的 expected_pnl_pct 低于此值则跳过不下单.',
   'migration_028'),
  ('gemini_max_open_positions', '5',
   'Gemini 策略全局同时持仓上限 (默认 5). 达到后本轮不再开新仓.',
   'migration_028'),
  ('gemini_symbol_cooldown_hours', '24',
   'Gemini 同 symbol 入场后冷却时长 (小时, 默认 24). 平仓后此时间内同 symbol 不重新询问 Gemini.',
   'migration_028')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
