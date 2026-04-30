-- 027_signal_wait_settings.sql (2026-04-30)
-- 在 system_settings 表插入 strategy_live dump-entry / topshort 的 30min 入场信号等待期参数.
--
-- 背景:
--   2026-04-30 paper_period_compare 发现 LOCAL 比 REMOTE 近 3 天多赚 +664U,
--   主要差距在 strategy_live:dump-entry (+462U) 和 topshort 上.
--   SIREN 等共同 symbol 同入场点差几十分钟, REMOTE breakeven-sl LOCAL trail-tp.
--   推测震荡市突破瞬间 = 假突破, 慢半拍入场 = 顺势 trail-tp.
--   加 30min 信号等待期 (SIG_WAIT 状态), 等待期满重判信号仍触发才下单, 期间反向 >2% 信号失效.
--
-- 读取方:
--   strategy_live.py 的 _load_live_config 在进程启动时一次性读 (改完需重启策略进程).
--
-- 写入方:
--   暂无 UI; 用户通过 SQL 直接 UPDATE system_settings 调试.
--
-- 灰度顺序:
--   1. migration 上线 (默认全部 OFF, 行为不变)
--   2. 重启 strategy_live 进程拉新代码
--   3. 单独开 dump_signal_wait_enabled=1, paper 跑 5-7 天
--   4. 跑 diag_signal_wait_observe.py 看 SIG_WAIT 转移分布
--   5. 通过则开 topshort_signal_wait_enabled, 同样 5 天观察
--
-- 幂等:
--   ON DUPLICATE KEY UPDATE 只刷 description / updated_by, 不动 setting_value.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('dump_signal_wait_enabled', '0',
   'strategy_live dump-entry 入场前 30min 信号等待期总开关. 0=立即下单(原行为), 1=进 SIG_WAIT 等待重判. 默认 OFF, 改完重启策略进程.',
   'migration_027'),
  ('dump_signal_wait_min', '30',
   'dump-entry SIG_WAIT 等待时长(分钟, 默认 30). 等待期满重跑 _check_dump_signal 函数, 仍触发才下单.',
   'migration_027'),
  ('dump_signal_adverse_pct', '0.02',
   'dump-entry SIG_WAIT 等待期反向阈值. SHORT 信号期间, 当前价 >= sig_p × (1 + 此值) 则信号失效, 退回 IDLE.',
   'migration_027'),
  ('topshort_signal_wait_enabled', '0',
   'strategy_live topshort 入场前 30min 信号等待期总开关. 0=立即下单, 1=进 SIG_WAIT 等待重判. 仅影响 standard topshort, climax 路径不受影响.',
   'migration_027'),
  ('topshort_signal_wait_min', '30',
   'topshort SIG_WAIT 等待时长(分钟, 默认 30).',
   'migration_027'),
  ('topshort_signal_adverse_pct', '0.02',
   'topshort SIG_WAIT 等待期反向阈值 (默认 0.02 = 2%).',
   'migration_027')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
