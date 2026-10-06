/*
  MooseDB storage engine — time-series storage for MariaDB.

  Copyright (c) 2026 Kevenny

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  The handler is a thin adapter: it converts MariaDB records to TFRow values
  and back, and maps handler calls onto the C API of the Rust core
  (moosedb_ffi.h). All storage logic lives in Rust.
*/

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "my_global.h"
#include "handler.h"
#include "thr_lock.h"
#include "moosedb_ffi.h"
#include "moosedb_options.h"

/* How a MariaDB field is converted to/from a TFValue. */
enum class tf_conv : uint8_t
{
  DATETIME,   /* DATETIME            <-> TF_COL_TIMESTAMP (UTC wall clock, µs) */
  TIMESTAMP,  /* TIMESTAMP           <-> TF_COL_TIMESTAMP (Unix time, µs)      */
  INT,        /* integer types, YEAR, BIT <-> TF_COL_INT64                     */
  FLOAT,      /* FLOAT               <-> TF_COL_FLOAT32                        */
  DOUBLE,     /* DOUBLE              <-> TF_COL_FLOAT64                        */
  DECIMAL,    /* DECIMAL             <-> TF_COL_DECIMAL (canonical string)     */
  STRING,     /* everything else     <-> TF_COL_VARCHAR (val_str / store)      */
  TAG         /* COMMENT 'TAG'       <-> TF_COL_TAG (val_str / store)          */
};

struct tf_column
{
  uint field_index;        /* index in TABLE::field                */
  TFColumnType type;       /* storage type in the Rust core        */
  tf_conv conv;
};

/* Maps a MariaDB table definition onto the Rust core's schema. */
struct tf_layout
{
  std::vector<tf_column> columns;  /* stored (non-virtual) fields only  */
  uint ts_column= 0;               /* index into `columns`              */
  uint ts_key= MAX_KEY;            /* key on the timestamp column       */

  /* Position of a field among the TAG columns, or -1. */
  int tag_position(uint field_index) const;
};

struct tf_table_closer
{
  void operator()(MooseDBTable *t) const noexcept { moosedb_table_close(t); }
};
struct tf_scan_closer
{
  void operator()(MooseDBScan *s) const noexcept { moosedb_scan_close(s); }
};
struct tf_snapshot_closer
{
  void operator()(MooseDBSnapshot *s) const noexcept
  { moosedb_snapshot_close(s); }
};
/*
  A statement batch left open when its owner dies is never discarded: the
  server may already have reported those rows as inserted (and binlogged
  them). Normal paths commit explicitly; this is the safety net.
*/
struct tf_batch_closer
{
  void operator()(MooseDBBatch *b) const noexcept;
};
using tf_batch_ptr= std::unique_ptr<MooseDBBatch, tf_batch_closer>;
using tf_table_ptr= std::unique_ptr<MooseDBTable, tf_table_closer>;
using tf_scan_ptr= std::unique_ptr<MooseDBScan, tf_scan_closer>;
using tf_snapshot_ptr= std::unique_ptr<MooseDBSnapshot, tf_snapshot_closer>;

/*
  An equality/IN predicate on a TAG column, pushed down from the WHERE
  clause: the column must equal one of `values` under collation `cs`.
*/
struct tf_tag_predicate
{
  int tag_pos;
  CHARSET_INFO *cs;
  std::vector<std::string> values;   /* already in cs's character set */
};

/*
  Per-table state shared by every handler instance of one TABLE_SHARE.
  The Rust table is opened once here and is thread-safe.
*/
class MooseDB_share : public Handler_share
{
public:
  THR_LOCK lock;
  tf_table_ptr table;
  tf_layout layout;
  std::string path;      /* table directory, as given to open()      */
  std::string display;   /* db.table, for messages (no host paths)    */

  MooseDB_share();
  ~MooseDB_share() override;
};

class ha_moosedb final : public handler
{
public:
  ha_moosedb(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_moosedb() override;

  /* ── Identification ───────────────────────────────────────────────── */
  const char *index_type(uint) override { return "TSIDX"; }

  /* ── DDL ──────────────────────────────────────────────────────────── */
  int create(const char *name, TABLE *table_arg,
             HA_CREATE_INFO *create_info) override;
  int open(const char *name, int mode, uint test_if_locked) override;
  int close() override;
  int delete_table(const char *name) override;
  int rename_table(const char *from, const char *to) override;

  /* ── DML ──────────────────────────────────────────────────────────── */
  int write_row(const uchar *buf) override;
  int update_row(const uchar *, const uchar *) override
  { return HA_ERR_WRONG_COMMAND; }   /* append-only: UPDATE not supported */
  int delete_row(const uchar *) override
  { return HA_ERR_WRONG_COMMAND; }   /* use RETENTION_PERIOD or TRUNCATE  */
  int delete_all_rows() override;
  int truncate() override;
  void start_bulk_insert(ha_rows rows, uint flags) override;
  int end_bulk_insert() override;

  /* ── Full scans ───────────────────────────────────────────────────── */
  int rnd_init(bool scan) override;
  int rnd_next(uchar *buf) override;
  int rnd_end() override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  void position(const uchar *record) override;

  /* ── Index (timestamp range) scans ────────────────────────────────── */
  int index_init(uint idx, bool sorted) override;
  int index_read_map(uchar *buf, const uchar *key, key_part_map keypart_map,
                     enum ha_rkey_function find_flag) override;
  int index_next(uchar *buf) override;
  int index_prev(uchar *buf) override;
  int index_first(uchar *buf) override;
  int index_last(uchar *buf) override;
  int index_end() override;

  /* ── Condition pushdown (TAG equality / IN) ───────────────────────── */
  const COND *cond_push(const COND *cond) override;
  void cond_pop() override;
  int reset() override;

  /* ── Info ─────────────────────────────────────────────────────────── */
  int info(uint flag) override;
  ha_rows records_in_range(uint inx, const key_range *min_key,
                           const key_range *max_key,
                           page_range *res) override;
  bool get_error_message(int error, String *buf) override;

  /* ── Admin ────────────────────────────────────────────────────────── */
  int check(THD *thd, HA_CHECK_OPT *check_opt) override;
  int optimize(THD *thd, HA_CHECK_OPT *check_opt) override;

  /* ── Locking ──────────────────────────────────────────────────────── */
  int external_lock(THD *thd, int lock_type) override;
  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;

  /* ── Capabilities ─────────────────────────────────────────────────── */
  ulonglong table_flags() const override
  {
    return HA_NO_TRANSACTIONS | HA_REC_NOT_IN_SEQ |
           HA_STATS_RECORDS_IS_EXACT | HA_NO_AUTO_INCREMENT |
           HA_BINLOG_ROW_CAPABLE | HA_BINLOG_STMT_CAPABLE |
           HA_CAN_TABLE_CONDITION_PUSHDOWN;
  }
  ulong index_flags(uint, uint, bool) const override
  {
    return HA_READ_NEXT | HA_READ_PREV | HA_READ_ORDER | HA_READ_RANGE;
  }
  uint max_supported_keys() const override { return 2; }
  uint max_supported_key_parts() const override { return 1; }
  uint max_supported_key_length() const override { return 16; }

  /* ── MooseDB specific ────────────────────────────────────────────── */
  bool moosedb_is_timestamp_column(const Field *field) const;
  int moosedb_force_flush();
  int moosedb_compact_chunks(longlong from_us, longlong to_us);

private:
  MooseDB_share *get_share();
  int open_scan(longlong lo, longlong hi, bool sorted);
  int open_range_scan(longlong lo, longlong hi, bool backward, uchar *buf,
                      int not_found_error);
  int read_from_scan(uchar *buf, bool backward, int end_error);
  int fill_record(uchar *buf, const TFRow &row);
  int key_to_ts(uint idx, const uchar *key, longlong *out);
  int map_status(TFStatus status, const char *name= nullptr);
  int commit_batch();   /* publish + sync the open statement batch, if any */

  void collect_tag_predicates(const Item *cond,
                              std::vector<tf_tag_predicate> *out);
  bool pushed_series(const std::vector<uint64_t> **ids);

  THR_LOCK_DATA lock_data_;
  MooseDB_share *share_= nullptr;
  tf_scan_ptr scan_;
  bool scan_snapshot_saved_= false; /* snapshot of scan_ kept for rnd_pos */
  std::vector<tf_snapshot_ptr> snapshots_;  /* resolve positions in rnd_pos */
  tf_batch_ptr batch_;         /* open statement batch (lazy, per stmt)  */
  bool in_bulk_insert_= false;
  query_id_t bulk_query_id_= 0;  /* statement that called start_bulk_insert */
  std::string last_error_;     /* message for get_error_message()        */
  std::vector<String> str_bufs_;
  std::vector<TFValue> values_;

  /* Pushed-down TAG predicates: one entry per cond_push() call. */
  std::vector<std::vector<tf_tag_predicate>> pushed_;
  bool pushed_valid_= false;            /* pushed_ids_ is up to date      */
  bool pushed_filter_= false;           /* some predicate restricts rows  */
  std::vector<uint64_t> pushed_ids_;    /* series matching all predicates */
};
