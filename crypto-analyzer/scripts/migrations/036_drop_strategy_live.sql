-- 036_drop_strategy_live.sql
-- 2026-05-18: 完全删除 strategy_live 进程 + topshort 子策略.
-- 极简化进一步收敛, 只保留 strategy_bigmid (gemini) 一个策略进程.
--
-- 背景:
--   2026-05-15 的 035 已经把 chase/dump/whale/f3 等子策略代码和配置全部删了,
--   只剩 strategy_live (topshort) + strategy_bigmid (gemini) 两个进程.
--   现在判断 topshort 也不再需要, 把 strategy_live.py 整体删除,
--   配套清理 system_settings 里 4 个 live_* 参数 + 3 个 topshort_* 开关,
--   以及 strategy_state 表里 strategy='live' 的所有历史 IDLE/DONE 行.
--
-- 影响:
--   - strategy_live 进程之后不再启动, 4 个 live_* / 3 个 topshort_* 参数读不到
--     也没有任何代码会读 (相关 API/UI 字段同时移除).
--   - 已 commit 的历史 futures_orders / order_trigger_events / strategy_state
--     里 source='strategy_live:topshort' 的行保留不动 (历史可查).
--   - 不影响 strategy_bigmid, 它共用 ACCOUNT_ID=2 / 公共风控参数仍生效.

-- 1. 删 system_settings 里 4 个 live_* 参数 + 3 个 topshort_* 开关
DELETE FROM system_settings WHERE setting_key IN (
    -- strategy_live 主参数 (代码已删, 不再有人读)
    'live_sl_pct',
    'live_hard_tp_pct',
    'live_limit_offset_pct',
    'live_hold_hours',
    -- strategy_live: topshort 信号 30min 等待期
    'topshort_signal_wait_enabled',
    'topshort_signal_wait_min',
    'topshort_signal_adverse_pct'
);

-- 2. 清 strategy_state 表里 strategy='live' 的所有行
-- (IDLE/DONE 历史记录, 进程没了不需要再保留状态机槽位)
DELETE FROM strategy_state WHERE strategy = 'live';
