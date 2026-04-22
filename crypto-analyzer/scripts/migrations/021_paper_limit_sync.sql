-- 模拟盘限价单成交 -> 实盘同步
-- futures_orders 表新增同步状态列
ALTER TABLE futures_orders
  ADD COLUMN live_sync_status  VARCHAR(20)  NULL DEFAULT NULL COMMENT '实盘同步状态: NULL=未处理 SYNCED=成功 FAILED=失败 DISABLED=实盘关闭',
  ADD COLUMN live_synced_at    DATETIME     NULL DEFAULT NULL COMMENT '同步处理时间',
  ADD COLUMN live_position_id  VARCHAR(64)  NULL DEFAULT NULL COMMENT '实盘仓位ID';

CREATE INDEX IF NOT EXISTS idx_fo_live_sync ON futures_orders (status, order_type, live_sync_status, fill_time);

-- user_api_keys 表新增每笔实盘保证金
ALTER TABLE user_api_keys
  ADD COLUMN margin_per_trade DECIMAL(10,2) NOT NULL DEFAULT 40.00 COMMENT '每笔实盘保证金(USDT)，用于模拟盘->实盘同步';
