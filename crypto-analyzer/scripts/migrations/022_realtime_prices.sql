-- 022_realtime_prices.sql
-- 实时价格集中存储表
-- 2026-04-23 事件：三个策略各自轮询 /api/futures/price 打爆 Binance IP 限额
-- 解决：fast_collector_service 每 4s 拉 Binance 全市场 ticker/price (权重2)，
--       批量 UPSERT 到本表；/api/futures/price 端点优先读本表，降低 Binance 压力

CREATE TABLE IF NOT EXISTS realtime_prices (
  symbol      VARCHAR(30)  NOT NULL PRIMARY KEY COMMENT '交易对，如 BTC/USDT',
  price       DECIMAL(20,8) NOT NULL,
  updated_at  TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  source      VARCHAR(20)   DEFAULT 'binance_futures',
  KEY idx_updated_at (updated_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='实时价格集中存储，由 fast_collector_service 每 4s 批量更新';
