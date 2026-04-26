-- 024_order_trigger_events.sql (2026-04-26)
-- 新表 order_trigger_events: 记录 LIMIT 单从挂单到成交/取消之间的"中间事件"
--
-- 背景:
--   futures_orders 表只记录 LIMIT 单的最终状态 (PENDING/FILLED/CANCELLED).
--   但 LIMIT 在挂单期间可能多次"触及限价 -> 等 5m 反向确认 -> 被反向拒 -> 价格回撤 -> 再触及"
--   这些中间事件以前只打日志, 不入库, 没法事后追查"为什么这单挂了 53 分钟才 fill".
--
-- 事件类型:
--   TRIGGER_OBSERVING  价格首次到达限价线, 进入 5m 反向 K 确认观察期
--   5M_REJECT          下根 5m K 线方向不对 (SHORT 要阴线/LONG 要阳线), 拒绝成交, 重新等待
--   TRIGGER_RETREAT    还没等到下根 5m 收盘, 价格已经回撤到限价线另一侧, 清除观察记录
--
-- 用法:
--   - 同一 order_id 可对应多条事件 (一个 LIMIT 多次触发被反向拒, 全部入库)
--   - FILL / CANCEL 由 futures_orders.status / fill_time / canceled_at 表达, 不在此表
--   - 写入失败不阻塞主流程 (try/except 兜底, 见 strategy_live.py)

CREATE TABLE IF NOT EXISTS `order_trigger_events` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `order_id` varchar(50) NOT NULL COMMENT '对应 futures_orders.order_id',
  `event_type` varchar(32) NOT NULL COMMENT 'TRIGGER_OBSERVING / 5M_REJECT / TRIGGER_RETREAT',
  `event_time` datetime NOT NULL DEFAULT current_timestamp() COMMENT '事件时刻',
  `cur_price` decimal(18,8) DEFAULT NULL COMMENT '事件时市价',
  `limit_price` decimal(18,8) DEFAULT NULL COMMENT 'LIMIT 委托价',
  `bar_open_price` decimal(18,8) DEFAULT NULL COMMENT '5M_REJECT 当根 5m open',
  `bar_close_price` decimal(18,8) DEFAULT NULL COMMENT '5M_REJECT 当根 5m close',
  `detail` varchar(255) DEFAULT NULL COMMENT '附加信息',
  PRIMARY KEY (`id`),
  KEY `idx_order_id` (`order_id`),
  KEY `idx_event_time` (`event_time`),
  KEY `idx_event_type` (`event_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='LIMIT 单挂单期间中间事件流';
