-- 025_seed_guard_switches.sql (2026-04-27)
-- 在 system_settings 表插入两个新守卫开关的默认行 (value='0', 即 OFF).
--
-- 背景:
--   2026-04-27 上午 12 个 LIMIT 信号被 5m 阴/阳收盘确认守卫 + 价格回退守卫全部拦掉,
--   diag_blocked_signals_pnl.py 反推 8 误拦 / 3 救人 / 1 平 (ZBT SHORT +15.89%
--   是最大的误拦案例). 加 disable_5m_confirm 总开关让用户可以一键禁用该守卫.
--   chase_allow_slow 进一步允许 chase-entry 接受慢爬币.
--
-- 读取方:
--   strategy_whale.py / strategy_live.py / strategy_bigmid.py / strategy_f3.py
--   通过各自 _load_*_config 在进程启动时一次性读 (改完需重启策略进程才生效).
--
-- 写入方:
--   PC 端 templates/system_settings.html "守卫开关" 区两个 toggle
--   手机端 templates/mobile_settings.html "守卫开关" section 两个 toggle
--   都通过 PUT /api/system/settings/{key} 通用 KV API.
--
-- 幂等:
--   ON DUPLICATE KEY UPDATE 只刷 description / updated_by,
--   不动 setting_value, 避免覆盖用户已经在生产里调过的开关状态.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('disable_5m_confirm', '0',
   '跳过 LIMIT 单的 5m 阴/阳收盘确认守卫: 1=触发即按 limit_price 成交, 0=保留默认确认流程. 影响 whale/live/bigmid/f3, 改完重启策略进程生效.',
   'migration_025'),
  ('chase_allow_slow', '0',
   'CHASE 是否接受慢爬入场: 1=跳过 leader_gain >=3% 阈值, 0=保留急涨过滤. 仅影响 strategy_live 的 chase-entry, 改完重启策略进程生效.',
   'migration_025')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
