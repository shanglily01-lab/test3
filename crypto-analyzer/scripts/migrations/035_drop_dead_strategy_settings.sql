-- 035_drop_dead_strategy_settings.sql
-- 2026-05-15: 极简化重构
-- 删除 strategy_live (chase / dump / climax) / strategy_whale (全部 6 个子策略) /
-- strategy_f3 相关的 system_settings 行. 这些子策略的代码本次一起删除.
--
-- 保留: live_* / topshort_signal_wait_* / disable_sl_tp_hold / disable_5m_confirm /
--       gemini_* (bigmid 用) / max_positions / 等通用项.

DELETE FROM system_settings WHERE setting_key IN (
    -- strategy_live: chase / dump 入场总开关 + chase_allow_slow
    'chase_entry_enabled',
    'dump_entry_enabled',
    'chase_allow_slow',
    -- strategy_live: dump 信号等待期 (topshort 同套 key 保留)
    'dump_signal_wait_enabled',
    'dump_signal_wait_min',
    'dump_signal_adverse_pct',
    -- strategy_whale: 主参数 (代码删了不再有人读)
    'whale_sl_pct',
    'whale_hard_tp_pct',
    'whale_limit_offset_pct',
    'whale_hold_hours',
    -- strategy_whale: longhold 子策略
    'longhold_enabled',
    'longhold_sl_pct',
    'longhold_tp_pct',
    'longhold_limit_offset_pct',
    'longhold_hold_hours',
    'longhold_rebound_pct',
    -- strategy_whale: rev4d 子策略
    'rev4d_enabled',
    'rev4d_threshold_pct',
    'rev4d_sl_pct',
    'rev4d_tp_pct',
    'rev4d_hold_hours',
    'rev4d_cooldown_hours',
    -- strategy_whale: swan 子策略
    'swan_strategy_enabled',
    'swan_min_confidence',
    'swan_position_usdt',
    'swan_leverage',
    'swan_max_open',
    'swan_hold_minutes',
    'swan_cooldown_hours',
    'swan_last_run_id',
    -- strategy_f3: 总开关
    'f3_strategy_enabled'
);
