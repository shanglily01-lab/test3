-- 029_chase_dump_entry_switches.sql (2026-05-01)
-- 在 system_settings 表新增 chase-entry / dump-entry 两个入场总开关.
--
-- 背景:
--   2026-05-01 用户判断: "不是守卫不够, 是方向判断本身出了严重问题".
--   近 7 天 paper 数据印证: chase-entry / dump-entry 是"顺势追突破"逻辑, 在加密
--   假突破频发的市场结构性反向 — LONG 高位追多 37 笔 -789U, SHORT 低位杀跌 24 笔
--   -288U; 反向操作的 topshort 30 笔 +271U, bottomlong-climax 类似.
--   先关 chase / dump 止血, 让 topshort 主导 SHORT, f3-entry 主导 LONG.
--
-- 行为:
--   - chase_entry_enabled=0: chase_tick 在 IDLE 分支提前 return, 不触发新入场.
--     已有 LONG/PENDING/DONE 持仓继续按原 SL/TP/timeout/冷却管理, 不影响.
--   - dump_entry_enabled=0:  dump_tick 同理, 不触发新 SHORT.
--
-- 默认值 1 (向后兼容). 想关掉直接 UPDATE 0, strategy_live 60s 内自动 reload 生效.
--
-- 读取方:
--   strategy_live.py 的 _load_live_config (主循环每 60s reload). 不需要重启进程.
--
-- 幂等: ON DUPLICATE KEY UPDATE 只刷 description, 不动 setting_value.

INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('chase_entry_enabled', '1',
   'strategy_live chase-entry 入场总开关. 0=不触发新入场(已有仓位继续 monitor SL/TP), 1=正常. 默认 1, 60s 内动态生效.',
   'migration_029'),
  ('dump_entry_enabled', '1',
   'strategy_live dump-entry 入场总开关. 0=不触发新入场(已有仓位继续 monitor SL/TP), 1=正常. 默认 1, 60s 内动态生效.',
   'migration_029')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
