-- 023_symbol_blacklist.sql
-- 品种级黑名单动态管理表
-- 2026-04-24: 让策略层"拉黑某币"不再需要改代码 + commit + 重启
-- 策略每 5 分钟从此表刷新一次，合并模块级 BASE 黑名单后生效

CREATE TABLE IF NOT EXISTS symbol_blacklist (
  symbol      VARCHAR(30)  NOT NULL PRIMARY KEY COMMENT '交易对，如 BTC/USDT',
  reason      VARCHAR(200) DEFAULT NULL         COMMENT '拉黑原因（可选）',
  created_by  VARCHAR(50)  DEFAULT 'manual'     COMMENT '来源: manual / auto / import',
  is_active   TINYINT(1)   NOT NULL DEFAULT 1   COMMENT '1=生效；0=暂停生效（保留历史）',
  created_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP    DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_active (is_active)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='品种级黑名单（策略 5 分钟刷新）';

-- 导入当前硬编码的 16 个币（一次性迁移；后续通过 API 增删）
INSERT IGNORE INTO symbol_blacklist (symbol, reason, created_by) VALUES
  ('DENT/USDT',  '币安即将下架',     'seed'),
  ('XAN/USDT',   '币安即将下架',     'seed'),
  ('SUPER/USDT', '币安即将下架',     'seed'),
  ('GUN/USDT',   '币安即将下架',     'seed'),
  ('UAI/USDT',   '币安即将下架',     'seed'),
  ('AAVE/USD',   '非 USDT 对',       'seed'),
  ('BTC/USD',    '非 USDT 对',       'seed'),
  ('XVG/USDT',   '币安即将下架',     'seed'),
  ('TRU/USDT',   '币安即将下架',     'seed'),
  ('DEGO/USDT',  '币安即将下架',     'seed'),
  ('ZRO/USDT',   '币安即将下架',     'seed'),
  ('RIVER/USDT', '币安即将下架',     'seed'),
  ('Q/USDT',     '反复止损',         'seed'),
  ('CHIP/USDT',  '反复止损',         'seed'),
  ('SPK/USDT',   '反复止损',         'seed'),
  ('UB/USDT',    '反复止损',         'seed');
