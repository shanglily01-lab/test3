-- 032_swan_strategy_settings.sql (2026-05-03)
-- SWAN 子策略 (strategy_whale 子模块): 按 gemini_swan_verdicts 自动下限价单.
--
-- 背景:
--   红黑天鹅榜 (gemini_swan_runs/verdicts) 每 2h 自动跑一轮, 但只在前端展示.
--   本 migration 加 7 个 system_settings 行, 让 strategy_whale.py 主循环里新增
--   的 swan_strategy_tick() 能读 STRONG verdict, 自动顺势开限价单
--   (red_swan -> LONG, black_swan -> SHORT). 持仓 12h 自动到期平仓.
--
-- 入仓库: dimesion (跟 gemini_swan_* 一致).
-- 默认全关 (swan_strategy_enabled=0), 部署后用户手动 UPDATE 才生效.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('swan_strategy_enabled', '0',
   'SWAN 子策略总开关 (strategy_whale 内). 0=关 (默认), 1=开. 60s 动态生效.',
   'migration_032'),
  ('swan_min_confidence',   '0.7',
   'SWAN 最低 avg_confidence 门槛, 默认 0.7 (1.0 = 3/3 轮全是该类别且 Gemini 高置信).',
   'migration_032'),
  ('swan_position_usdt',    '500',
   'SWAN 单笔本金 USDT (跟 strategy_whale 全局 MARGIN 同源, 当前一致).',
   'migration_032'),
  ('swan_leverage',         '5',
   'SWAN 杠杆倍数 (跟 strategy_whale 全局 LEVERAGE 同源, 当前一致).',
   'migration_032'),
  ('swan_max_open',         '5',
   'SWAN 子策略同时持仓数上限.',
   'migration_032'),
  ('swan_hold_minutes',     '720',
   'SWAN 持仓时长 (分钟), 720=12h. 由 _close_overdue 自动到期平仓.',
   'migration_032'),
  ('swan_cooldown_hours',   '12',
   '同 symbol 平仓后冷却小时数. 防止 2h 一次 swan run 反复开同一个 symbol.',
   'migration_032')
ON DUPLICATE KEY UPDATE
  setting_value = VALUES(setting_value),
  description   = VALUES(description),
  updated_by    = VALUES(updated_by);

-- swan_last_run_id 是策略代码自动维护的进度游标 (per-write 由 strategy_whale 写),
-- 这里只做兜底 (确保 key 存在, 值=0). 部署后第一次 swan_strategy_tick 会用它过滤.
INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('swan_last_run_id', '0',
   'SWAN 已处理的最大 gemini_swan_runs.run_id. 由 strategy_whale 自动写, 无需手改.',
   'migration_032')
ON DUPLICATE KEY UPDATE
  description = VALUES(description);
