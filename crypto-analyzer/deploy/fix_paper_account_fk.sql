-- =============================================================================
-- 修复遗留的错误外键约束
--   问题: futures_orders / futures_trades / futures_positions 的 account_id
--         被错误地指向 paper_trading_accounts(id)（纸面交易表，已废弃）。
--         而实际 account_id=2 存在于 futures_trading_accounts 里，
--         导致任何 INSERT INTO futures_orders 都触发 FK 校验失败：
--           (1452, 'Cannot add or update a child row: a foreign key constraint
--                   fails (`dimesion`.`futures_orders`,
--                   CONSTRAINT `futures_orders_ibfk_1` FOREIGN KEY ...')
--         → 止盈止损平仓事务回滚，SL/TP 表面上"不工作"。
--   修复: DROP 所有指向 paper_trading_accounts 的 FK（整个表已不使用）。
--   幂等: 反复执行无副作用（没有匹配的 FK 时 LOOP 不执行）。
--
-- 用法（在 EC2 上）：
--   mysql -h"$DB_HOST" -P"$DB_PORT" -u"$DB_USER" -p"$DB_PASSWORD" \
--         "$DB_NAME" < deploy/fix_paper_account_fk.sql
-- =============================================================================

DELIMITER $$

DROP PROCEDURE IF EXISTS _fix_paper_fks $$

CREATE PROCEDURE _fix_paper_fks()
BEGIN
    DECLARE v_done INT DEFAULT 0;
    DECLARE v_table VARCHAR(128);
    DECLARE v_fk    VARCHAR(128);
    DECLARE v_sql   TEXT;

    DECLARE cur CURSOR FOR
        SELECT TABLE_NAME, CONSTRAINT_NAME
          FROM information_schema.KEY_COLUMN_USAGE
         WHERE TABLE_SCHEMA       = DATABASE()
           AND REFERENCED_TABLE_NAME = 'paper_trading_accounts'
           AND CONSTRAINT_NAME   <> 'PRIMARY';

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN cur;
    loop_fk: LOOP
        FETCH cur INTO v_table, v_fk;
        IF v_done = 1 THEN
            LEAVE loop_fk;
        END IF;

        SET v_sql = CONCAT('ALTER TABLE `', v_table, '` DROP FOREIGN KEY `', v_fk, '`');
        SELECT CONCAT('[fix] dropping ', v_table, '.', v_fk) AS info;

        SET @s = v_sql;
        PREPARE stmt FROM @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END LOOP;
    CLOSE cur;
END $$

DELIMITER ;

CALL _fix_paper_fks();
DROP PROCEDURE _fix_paper_fks;

-- 自检：确认再也没有 FK 指向 paper_trading_accounts
SELECT TABLE_NAME, CONSTRAINT_NAME
  FROM information_schema.KEY_COLUMN_USAGE
 WHERE TABLE_SCHEMA = DATABASE()
   AND REFERENCED_TABLE_NAME = 'paper_trading_accounts';
