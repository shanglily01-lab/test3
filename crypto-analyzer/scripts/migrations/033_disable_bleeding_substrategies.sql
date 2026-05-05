-- 033_disable_bleeding_substrategies.sql (2026-05-04)
-- 关停近 7 天 paper 出血严重的几条入场路径 + 给 f3 子策略加总开关.
--
-- 背景 (诊断脚本: scripts/diag/diag_recent_trade_quality.py --days 7):
--   过去 7 天 paper 总 PnL = -889 U / 胜率 40.9%, 主要由以下子策略拖死:
--     - strategy_f3:f3-entry        46 笔 -917 U / 胜率 30.4% (元凶, 慢性出血)
--     - strategy_whale:swan          2 笔  -95 U / 胜率  0.0% (全部 5 分钟内被止损,
--                                                              叠加 SL 1.85% 异常未查清)
--     - strategy_bigmid:gemini       8 笔  -25 U / 胜率 37.5% (5-02 改 google-genai SDK 后变差)
--     - strategy_live:chase-entry   原应 5-02 关闭, 但 chase_entry_enabled=1 没生效, 5-02 后又开 4 笔
--     - strategy_live:dump-entry    7 天没新单, 但 dump_entry_enabled=1, 顺手关掉防止意外
--
-- 改动:
--   1. 新增 f3_strategy_enabled key, 默认 '0' (关). strategy_f3.py 已加读取 + tick 入场守卫.
--   2. 把 swan/chase/gemini/dump 全部 setting_value 改 '0'.
--
-- 影响:
--   - 只拦新入场, 已 PENDING 限价单和已开仓位继续走原有出场逻辑 (trail-tp/timeout/SL)
--   - 4 个 strategy 主循环每 60s reload, 改完无需重启进程
--   - 想恢复某条线: UPDATE setting_value='1' 即可

-- 1. f3 子策略加总开关 (strategy_f3.py 之前没有 enabled 开关)
INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('f3_strategy_enabled', '0',
   'F3 子策略总开关 (strategy_f3 入场扫描). 0=关 (33 部署后默认), 1=开. 60s 动态生效. '
   '关掉后 f3_tick 在 IDLE 入场扫描前 return, 已有 PENDING/LONG 持仓继续走原 SL/TP/timeout.',
   'migration_033')
ON DUPLICATE KEY UPDATE
  description = VALUES(description),
  updated_by  = VALUES(updated_by);

-- 2. 关掉 swan / chase / gemini / dump 入场
UPDATE `system_settings`
   SET setting_value = '0',
       updated_by    = 'migration_033'
 WHERE setting_key IN (
   'swan_strategy_enabled',
   'chase_entry_enabled',
   'gemini_strategy_enabled',
   'dump_entry_enabled'
 );
