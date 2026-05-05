-- 034_bigmid_bottom_reversal.sql (2026-05-04)
-- bigmid (Gemini) 子策略改造为"抄底反转专项".
--
-- 背景:
--   过去 7 天 paper, bigmid 8 笔 -25 U / 胜率 37.5%, 大量 timeout 平仓.
--   原 prompt 让 Gemini 自由选 long/short, 数据只覆盖 1h x 4 天, 缺 7 天低点信息.
--   用户决定改造方向: 只做多, 专门抄底反转, 喂 1h x 7 天数据 + 显式 7d 高低点.
--
-- 代码改动 (同 commit):
--   strategy_bigmid.py:
--     - _fetch_market_data: 1h 数据 96 -> 168 根 (4d -> 7d), 去掉冗余的 1h x 8h
--                            新增 low_7d / high_7d / pos_in_7d_band / dist_from_low_7d_pct
--                            新增 RSI(14, 15m)
--     - _build_gemini_prompt: 只允许 direction="long" | "skip", 强化抄底反转引导
--     - _call_gemini: 收到 "short" 降级为 "skip"
--     - gemini_round 下单分支: 防御性拒绝非 long
--
-- 仓位参数改动 (本 migration):
--   - gemini_tp_pct      0.03 -> 0.05  (反转抄底需要更长跑道, TP 3% / 6h 抓不到)
--   - gemini_hold_hours  6    -> 12    (同上)
--
-- 不动的:
--   - gemini_sl_pct           = 0.02
--   - gemini_limit_offset_pct = 0.002  (用户保留)
--   - gemini_min_pnl_pct      = 0.01
--   - gemini_max_open_positions / cooldown
--
-- 启用: 改完观察一轮 (paper 6h+) 确认 prompt 输出合理后, 再:
--   UPDATE system_settings SET setting_value='1' WHERE setting_key='gemini_strategy_enabled';

UPDATE `system_settings`
   SET setting_value = '0.05',
       updated_by    = 'migration_034'
 WHERE setting_key = 'gemini_tp_pct';

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('gemini_hold_hours', '12',
   'bigmid Gemini 持仓时长 (小时). 抄底反转改造后 12h 适配 TP 5%.',
   'migration_034')
ON DUPLICATE KEY UPDATE
  setting_value = VALUES(setting_value),
  description   = VALUES(description),
  updated_by    = VALUES(updated_by);
