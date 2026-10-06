-- TideFlow maintenance procedures.
--
-- Installs, into the CURRENT database, the procedures of spec §10:
--
--   CALL tideflow_compact('table' | 'db.table', from, to);   -- NULL = unbounded
--   CALL tideflow_apply_retention('table' | 'db.table');
--
-- They are thin wrappers over two UDFs exported by ha_tideflow.so. The
-- wrappers reference the table in the same statement as the UDF, so the
-- server opens it and the UDF can reach the open TideFlow instance.
--
-- Usage (once per database that holds TideFlow tables):
--   USE mydb;
--   SOURCE tideflow_install.sql;
--
-- Time bounds are interpreted like DATETIME values of the time axis
-- (UTC wall clock).

CREATE FUNCTION IF NOT EXISTS tideflow_compact_impl RETURNS INTEGER SONAME 'ha_tideflow.so';
CREATE FUNCTION IF NOT EXISTS tideflow_retention_impl RETURNS INTEGER SONAME 'ha_tideflow.so';

DELIMITER //

CREATE OR REPLACE PROCEDURE tideflow_compact(
    tbl     VARCHAR(129),
    ts_from DATETIME(6),
    ts_to   DATETIME(6))
  SQL SECURITY INVOKER
  COMMENT 'TideFlow: merge and re-encode the chunks overlapping [ts_from, ts_to]'
BEGIN
  DECLARE db VARCHAR(64) DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', 1), DATABASE());
  DECLARE t  VARCHAR(64) DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', -1), tbl);
  SET @tideflow_sql = CONCAT(
    'SELECT tideflow_compact_impl(', QUOTE(db), ', ', QUOTE(t), ', ',
    QUOTE(IFNULL(ts_from, '')), ', ', QUOTE(IFNULL(ts_to, '')), ') INTO @tideflow_result',
    ' FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `', REPLACE(db, '`', '``'), '`.`',
    REPLACE(t, '`', '``'), '` WHERE FALSE)');
  PREPARE tideflow_stmt FROM @tideflow_sql;
  EXECUTE tideflow_stmt;
  DEALLOCATE PREPARE tideflow_stmt;
END //

CREATE OR REPLACE PROCEDURE tideflow_apply_retention(tbl VARCHAR(129))
  SQL SECURITY INVOKER
  COMMENT 'TideFlow: drop the chunks older than RETENTION_PERIOD now'
BEGIN
  DECLARE db VARCHAR(64) DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', 1), DATABASE());
  DECLARE t  VARCHAR(64) DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', -1), tbl);
  SET @tideflow_sql = CONCAT(
    'SELECT tideflow_retention_impl(', QUOTE(db), ', ', QUOTE(t), ') INTO @tideflow_result',
    ' FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `', REPLACE(db, '`', '``'), '`.`',
    REPLACE(t, '`', '``'), '` WHERE FALSE)');
  PREPARE tideflow_stmt FROM @tideflow_sql;
  EXECUTE tideflow_stmt;
  DEALLOCATE PREPARE tideflow_stmt;
END //

DELIMITER ;
