-- 026_longhold_settings.sql (2026-04-29)
-- 在 system_settings 表插入 longhold 子策略 (W底/M顶 2 周窗口长持) 的默认参数.
--
-- 背景:
--   2026-04-29 strategy_whale.py 内新增 longhold-w / longhold-m 子策略:
--     - 1h K 线 x 14 天窗口扫描 W 底/M 顶
--     - 限价单 cur_price ± 3% (LH_LIMIT_OFFSET_PCT)
--     - TP 20% / SL 4% / 持仓上限 168h (1 周)
--     - 同 symbol 与 whale/w-bottom/m-top 互斥
--     - 限价单 24h 未成交自动撤
--   默认 longhold_enabled='0' (OFF), 上线先纸面观察, 用户审完命中样本再手动开启.
--
-- 读取方:
--   strategy_whale.py 的 _load_whale_config 在进程启动时一次性读 (改完需重启).
--
-- 写入方:
--   暂无 UI; 用户通过 SQL 直接 UPDATE system_settings 调试,
--   或后续在 templates/system_settings.html 增加 "longhold 子策略" 区.
--
-- 幂等:
--   ON DUPLICATE KEY UPDATE 只刷 description / updated_by,
--   不动 setting_value, 避免覆盖用户已经调过的值.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('longhold_enabled', '0',
   'longhold 子策略总开关: 1=启用 W底做多/M顶做空 长持子策略, 0=完全跳过 tick. 默认 OFF, 改完重启策略进程生效.',
   'migration_026'),
  ('longhold_sl_pct', '0.04',
   'longhold 子策略止损比例 (开仓价的 4%). 改完重启策略进程生效.',
   'migration_026'),
  ('longhold_tp_pct', '0.20',
   'longhold 子策略硬止盈比例 (开仓价的 20%). 改完重启策略进程生效.',
   'migration_026'),
  ('longhold_limit_offset_pct', '0.03',
   'longhold 限价单偏移: LONG 挂在 cur_price * (1 - 0.03), SHORT 挂在 cur_price * (1 + 0.03). 改完重启生效.',
   'migration_026'),
  ('longhold_hold_hours', '168',
   'longhold 持仓上限 (小时, 默认 168 = 1 周). 超时由 _close_overdue 平仓. 改完重启生效.',
   'migration_026'),
  ('longhold_rebound_pct', '0.08',
   'longhold W底颈线反弹/M顶颈线回撤的最小百分比 (默认 8%). 14 天窗口波动比 3.5 天大, 阈值放大. 改完重启生效.',
   'migration_026')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
