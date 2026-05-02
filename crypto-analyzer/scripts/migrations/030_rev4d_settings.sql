-- 030_rev4d_settings.sql (2026-05-02)
-- strategy_whale 新增 REV4D 子策略 (4 天 4H 极值反转) 默认参数.
--
-- 背景:
--   2026-05-02 用户判定 chase/dump 方向反, 关掉让反转型策略接管. REV4D 是又一个
--   反转型 — 用 4 天 4H K 线极值找反弹/见顶机会. 与 longhold (1h × 14 天) 互补,
--   覆盖中短线反转空间.
--
-- 入场:
--   触底法: cur_p ≤ 4d_low × (1+0.005) → LONG (24h 跌幅 < 25%)
--   触顶法: cur_p ≥ 4d_high × (1-0.005) → SHORT (24h 涨幅 < 30%)
--
-- 风控: SL 2% / TP 5% / hold 24h / 同 symbol 48h 冷却 / 全局 3 笔上限
--
-- 默认 enabled=0, 60s 内动态生效, 不需重启进程.
-- 改完前端 toggle / SQL UPDATE 都行.
--
-- 读取方: strategy_whale.py 的 _load_whale_config (主循环每 60s reload)
-- 幂等: ON DUPLICATE KEY UPDATE 只刷 description.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('rev4d_enabled', '0',
   'strategy_whale REV4D 子策略 (4 天 4H 极值反转) 总开关. 0=不触发, 1=启用. 60s 动态生效.',
   'migration_030'),
  ('rev4d_threshold_pct', '0.005',
   'REV4D 距 4 天极值阈值. 默认 0.005=0.5%. cur_p 距 4d_low/high 在此范围内即触发反转入场.',
   'migration_030'),
  ('rev4d_sl_pct', '0.02',
   'REV4D 止损比例. 默认 0.02=2% (反转策略经典紧 SL).',
   'migration_030'),
  ('rev4d_tp_pct', '0.05',
   'REV4D 硬止盈比例. 默认 0.05=5%.',
   'migration_030'),
  ('rev4d_hold_hours', '24',
   'REV4D 持仓上限 (小时, 默认 24). 4 天周期的 1/4 时间, 不达 SL/TP 即超时平.',
   'migration_030'),
  ('rev4d_cooldown_hours', '48',
   'REV4D 同 symbol 入场后冷却 (小时, 默认 48). 大于 hold, 等下个 4 天周期.',
   'migration_030')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
