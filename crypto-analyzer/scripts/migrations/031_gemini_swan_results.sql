-- 031_gemini_swan_results.sql (2026-05-03)
-- Gemini 红黑天鹅榜数据落库 + 后台采集开关.
--
-- 背景:
--   2026-05-03 把 diag_gemini_swan_now.py / diag_gemini_swan_consistency.py 生产化:
--   后台每 2h 跑 3 轮 Gemini, 用 24h 涨跌 + 资金费率极值采样 universe (~40 symbols),
--   聚合 STRONG / MODERATE / WEAK 一致性等级, 落两张表供前端「红黑天鹅榜」(/swan_board)
--   渲染. 替换原币本位合约交易页面 (该页几乎是空壳).
--
-- 表设计:
--   gemini_swan_runs       - 每次跑 1 行 (元数据 + 整体 summary)
--   gemini_swan_verdicts   - 每个 symbol 1 行 (聚合后的判定)
--
-- 调度: app/scheduler.py + threading.Thread (避免 ~3 分钟 Gemini 调用阻塞主调度器)
-- 60s 动态生效: gemini_swan_enabled 改 0 后下一次 2h 触发 worker 早返回, 无需重启进程.

-- =============================================================================
-- 1. 主表: 每次跑的元数据
-- =============================================================================
CREATE TABLE IF NOT EXISTS `gemini_swan_runs` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `asof_utc` DATETIME NOT NULL COMMENT '采样时刻 (UTC)',
  `model` VARCHAR(64) NOT NULL COMMENT 'Gemini 模型名, 如 gemini-3-flash-preview',
  `rounds` INT NOT NULL DEFAULT 3 COMMENT '本次跑了几轮',
  `universe_size` INT NOT NULL DEFAULT 0 COMMENT 'universe 去重后总数 (跨轮并集)',
  `summary_zh` TEXT COMMENT 'Gemini 整体总评 (取最后一轮的 summary_zh)',
  `elapsed_s` DECIMAL(8,2) DEFAULT NULL COMMENT '总耗时 (秒)',
  `status` VARCHAR(20) NOT NULL DEFAULT 'success' COMMENT 'running/success/partial/failed',
  `error_msg` TEXT DEFAULT NULL COMMENT '失败时的错误信息',
  `triggered_by` VARCHAR(20) NOT NULL DEFAULT 'scheduler' COMMENT 'scheduler/manual',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `idx_asof` (`asof_utc`) USING BTREE,
  KEY `idx_status_asof` (`status`, `asof_utc`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Gemini 红黑天鹅榜每次采集的元数据';

-- =============================================================================
-- 2. 明细表: 每个 symbol 的聚合判定
-- =============================================================================
CREATE TABLE IF NOT EXISTS `gemini_swan_verdicts` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `run_id` INT NOT NULL COMMENT 'FK -> gemini_swan_runs.id',
  `symbol` VARCHAR(20) NOT NULL COMMENT '合约对, 如 KNC/USDT',
  `main_category` VARCHAR(20) NOT NULL COMMENT 'black_swan / red_swan / skip',
  `consistency_level` VARCHAR(10) NOT NULL COMMENT 'STRONG / MODERATE / WEAK / SKIP',
  `avg_confidence` DECIMAL(4,3) NOT NULL DEFAULT 0 COMMENT '同类别多轮平均 confidence',
  `rounds_total` INT NOT NULL COMMENT '本次跑的总轮数 (冗余, 方便查询)',
  `universe_appearances` INT NOT NULL COMMENT '在多少轮 universe 里出现',
  `black_count` INT NOT NULL DEFAULT 0 COMMENT '被标 black_swan 的轮次数',
  `red_count` INT NOT NULL DEFAULT 0 COMMENT '被标 red_swan 的轮次数',
  `skip_count` INT NOT NULL DEFAULT 0 COMMENT '被标 skip 的轮次数',
  `catalyst` TEXT DEFAULT NULL COMMENT '最后一轮的具体催化剂描述',
  `data_signal` VARCHAR(255) DEFAULT NULL COMMENT '支持判定的关键数据 (如 funding -0.7%)',
  `risk_note` VARCHAR(255) DEFAULT NULL COMMENT '反向风险提醒',
  `triggers` JSON DEFAULT NULL COMMENT '哪些维度命中 ["24h_gainer","funding_neg_extreme",...]',
  `universe_data` JSON DEFAULT NULL COMMENT '快照 {price, change_24h, funding_rate, vol_24h}',
  `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `uniq_run_symbol` (`run_id`, `symbol`) USING BTREE,
  KEY `idx_symbol_run` (`symbol`, `run_id`) USING BTREE,
  KEY `idx_cat_level` (`main_category`, `consistency_level`) USING BTREE,
  CONSTRAINT `fk_swan_verdicts_run`
    FOREIGN KEY (`run_id`) REFERENCES `gemini_swan_runs` (`id`)
    ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='Gemini 红黑天鹅榜每 symbol 的聚合判定';

-- =============================================================================
-- 3. 后台采集开关 (system_settings, 60s 动态生效)
-- =============================================================================
INSERT INTO `system_settings` (`setting_key`, `setting_value`, `description`, `updated_by`)
VALUES
  ('gemini_swan_enabled', '1',
   'Gemini 红黑天鹅榜后台采集总开关. 0=暂停 (worker 早返回不调 Gemini), 1=每 2 小时跑一次. 60s 动态生效.',
   'migration_031'),
  ('gemini_swan_rounds', '3',
   'Gemini 红黑天鹅榜每次跑几轮 (建议 1-5). 3 轮聚合能筛掉单轮拍脑袋的 WEAK 信号. 改完下一次 2h 触发生效.',
   'migration_031')
ON DUPLICATE KEY UPDATE
  `description` = VALUES(`description`),
  `updated_by`  = VALUES(`updated_by`);
