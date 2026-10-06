-- MooseDB maintenance procedures.
--
-- Installs, into the CURRENT database, the procedures of spec §10:
--
--   CALL moosedb_compact('table' | 'db.table', from, to);   -- NULL = unbounded
--   CALL moosedb_apply_retention('table' | 'db.table');
--
-- They are thin wrappers over two UDFs exported by ha_moosedb.so. The
-- wrappers reference the table in the same statement as the UDF, so the
-- server opens it and the UDF can reach the open MooseDB instance.
--
-- Privileges: the UDFs themselves check the caller's privileges on the target
-- table (they can also be called directly): DELETE for retention, ALTER for
-- compaction. The procedures are SQL SECURITY INVOKER, and the statement they
-- build additionally needs SELECT on the table.
--
-- Usage (once per database that holds MooseDB tables):
--   USE mydb;
--   SOURCE moosedb_install.sql;
--
-- Time bounds are interpreted like DATETIME values of the time axis
-- (UTC wall clock).
--
-- The script does not depend on the installing client's character set: it
-- forces utf8mb4 (also stored as the routines' client character set), declares
-- every string parameter/variable as utf8mb4, and passes names to the UDFs as
-- hex literals (and the routines are created under STRICT_ALL_TABLES, so an
-- over-long name is an error, never silently truncated), so neither quoting
-- nor sql_mode (NO_BACKSLASH_ESCAPES, ANSI_QUOTES) can change their meaning.

SET @moosedb_install_sql_mode = @@SESSION.sql_mode;
SET @moosedb_install_names = CONCAT('SET NAMES ', @@SESSION.character_set_client,
                                     ' COLLATE ', @@SESSION.collation_connection);
SET NAMES utf8mb4;
SET SESSION sql_mode = 'STRICT_ALL_TABLES';

CREATE FUNCTION IF NOT EXISTS moosedb_compact_impl RETURNS INTEGER SONAME 'ha_moosedb.so';
CREATE FUNCTION IF NOT EXISTS moosedb_retention_impl RETURNS INTEGER SONAME 'ha_moosedb.so';

DELIMITER //

CREATE OR REPLACE PROCEDURE moosedb_compact(
    tbl     VARCHAR(129) CHARACTER SET utf8mb4,
    ts_from DATETIME(6),
    ts_to   DATETIME(6))
  SQL SECURITY INVOKER
  COMMENT 'MooseDB: merge and re-encode the chunks overlapping [ts_from, ts_to]'
BEGIN
  DECLARE tf_db  VARCHAR(64) CHARACTER SET utf8mb4 DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', 1), DATABASE());
  DECLARE tf_tbl VARCHAR(64) CHARACTER SET utf8mb4 DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', -1), tbl);
  DECLARE tf_prepared BOOLEAN DEFAULT FALSE;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    IF tf_prepared THEN
      DEALLOCATE PREPARE __moosedb_compact_stmt;
    END IF;
    RESIGNAL;
  END;
  SET @__moosedb_compact_sql = CONCAT(
    'SELECT moosedb_compact_impl(_utf8mb4 X''', HEX(tf_db), ''', _utf8mb4 X''', HEX(tf_tbl),
    ''', _utf8mb4 X''', HEX(IFNULL(ts_from, '')), ''', _utf8mb4 X''', HEX(IFNULL(ts_to, '')),
    ''') INTO @__moosedb_compact_rc FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `',
    REPLACE(tf_db, '`', '``'), '`.`', REPLACE(tf_tbl, '`', '``'), '` WHERE FALSE)');
  PREPARE __moosedb_compact_stmt FROM @__moosedb_compact_sql;
  SET tf_prepared = TRUE;
  EXECUTE __moosedb_compact_stmt;
  DEALLOCATE PREPARE __moosedb_compact_stmt;
  SET tf_prepared = FALSE;
END //

CREATE OR REPLACE PROCEDURE moosedb_apply_retention(
    tbl VARCHAR(129) CHARACTER SET utf8mb4)
  SQL SECURITY INVOKER
  COMMENT 'MooseDB: drop the chunks older than RETENTION_PERIOD now'
BEGIN
  DECLARE tf_db  VARCHAR(64) CHARACTER SET utf8mb4 DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', 1), DATABASE());
  DECLARE tf_tbl VARCHAR(64) CHARACTER SET utf8mb4 DEFAULT IF(LOCATE('.', tbl) > 0, SUBSTRING_INDEX(tbl, '.', -1), tbl);
  DECLARE tf_prepared BOOLEAN DEFAULT FALSE;
  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    IF tf_prepared THEN
      DEALLOCATE PREPARE __moosedb_retention_stmt;
    END IF;
    RESIGNAL;
  END;
  SET @__moosedb_retention_sql = CONCAT(
    'SELECT moosedb_retention_impl(_utf8mb4 X''', HEX(tf_db), ''', _utf8mb4 X''', HEX(tf_tbl),
    ''') INTO @__moosedb_retention_rc FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM `',
    REPLACE(tf_db, '`', '``'), '`.`', REPLACE(tf_tbl, '`', '``'), '` WHERE FALSE)');
  PREPARE __moosedb_retention_stmt FROM @__moosedb_retention_sql;
  SET tf_prepared = TRUE;
  EXECUTE __moosedb_retention_stmt;
  DEALLOCATE PREPARE __moosedb_retention_stmt;
  SET tf_prepared = FALSE;
END //

DELIMITER ;

-- Restore the installing session's settings.
SET SESSION sql_mode = @moosedb_install_sql_mode;
PREPARE __moosedb_restore_names FROM @moosedb_install_names;
EXECUTE __moosedb_restore_names;
DEALLOCATE PREPARE __moosedb_restore_names;
SET @moosedb_install_sql_mode = NULL;
SET @moosedb_install_names = NULL;
