/*
  TideFlow storage engine — time-series storage for MariaDB.

  Copyright (c) 2026 Kevenny / Rech Informática

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.
*/

#define MYSQL_SERVER 1

#include "ha_tideflow.h"

#include <algorithm>
#include <climits>
#include <cstring>
#include <ctime>

#include "my_global.h"
#include "sql_class.h"
#include "key.h"
#include "log.h"

static handlerton *tideflow_hton;

/* ── System variables ─────────────────────────────────────────────────── */

static const char *wal_sync_mode_names[]= {"fsync", "write", NullS};
static TYPELIB wal_sync_mode_typelib= CREATE_TYPELIB_FOR(wal_sync_mode_names);
enum { WAL_SYNC_FSYNC= 0, WAL_SYNC_WRITE= 1 };

static ulong srv_wal_sync_mode= WAL_SYNC_FSYNC;
static ulonglong srv_memtable_flush_threshold= 64ULL << 20;
static uint srv_compaction_trigger_chunks= 10;
static uint srv_compaction_threads= 2;
static uint srv_retention_check_interval= 3600;
static double srv_bloom_fpr= 0.01;
static ulonglong srv_chunk_cache_size= 128ULL << 20;
static uint srv_max_open_chunks= 100;

static MYSQL_SYSVAR_ENUM(wal_sync_mode, srv_wal_sync_mode, PLUGIN_VAR_RQCMDARG,
  "WAL durability at statement end: fsync (every committed row survives an "
  "OS crash) or write (rows survive a mysqld crash, not an OS crash)",
  NULL, NULL, WAL_SYNC_FSYNC, &wal_sync_mode_typelib);

static MYSQL_SYSVAR_ULONGLONG(memtable_flush_threshold,
  srv_memtable_flush_threshold, PLUGIN_VAR_RQCMDARG,
  "MemTable size in bytes that triggers a flush to a chunk, for tables "
  "without an explicit MEMTABLE_SIZE. Applies to tables opened afterwards",
  NULL, NULL, 64ULL << 20, 4096, 1ULL << 40, 0);

static MYSQL_SYSVAR_UINT(compaction_trigger_chunks,
  srv_compaction_trigger_chunks, PLUGIN_VAR_RQCMDARG,
  "Number of chunks in one time bucket that triggers compaction "
  "(reserved: compaction is not implemented yet)",
  NULL, NULL, 10, 2, 10000, 0);

static MYSQL_SYSVAR_UINT(compaction_threads, srv_compaction_threads,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Background compaction threads (reserved: compaction is not implemented yet)",
  NULL, NULL, 2, 1, 64, 0);

static MYSQL_SYSVAR_UINT(retention_check_interval,
  srv_retention_check_interval, PLUGIN_VAR_RQCMDARG,
  "Minimum seconds between two RETENTION_PERIOD sweeps of the same table. "
  "Sweeps run after write statements and on OPTIMIZE TABLE; 0 disables them",
  NULL, NULL, 3600, 0, UINT_MAX, 0);

static MYSQL_SYSVAR_DOUBLE(bloom_filter_false_positive_rate, srv_bloom_fpr,
  PLUGIN_VAR_RQCMDARG,
  "Target false-positive rate of per-chunk series Bloom filters "
  "(reserved: chunks currently use 0.01)",
  NULL, NULL, 0.01, 0.000001, 0.5, 0);

static MYSQL_SYSVAR_ULONGLONG(chunk_cache_size, srv_chunk_cache_size,
  PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
  "Bytes of decompressed chunk data to cache (reserved: no cache yet)",
  NULL, NULL, 128ULL << 20, 0, ULONGLONG_MAX, 0);

static MYSQL_SYSVAR_UINT(max_open_chunks, srv_max_open_chunks,
  PLUGIN_VAR_RQCMDARG,
  "Maximum chunk files kept open (reserved: chunks are opened per scan)",
  NULL, NULL, 100, 1, 1000000, 0);

static struct st_mysql_sys_var *tideflow_system_variables[]= {
  MYSQL_SYSVAR(wal_sync_mode),
  MYSQL_SYSVAR(memtable_flush_threshold),
  MYSQL_SYSVAR(compaction_trigger_chunks),
  MYSQL_SYSVAR(compaction_threads),
  MYSQL_SYSVAR(retention_check_interval),
  MYSQL_SYSVAR(bloom_filter_false_positive_rate),
  MYSQL_SYSVAR(chunk_cache_size),
  MYSQL_SYSVAR(max_open_chunks),
  NULL
};

/* ── Table options ────────────────────────────────────────────────────── */

ha_create_table_option tideflow_table_option_list[]=
{
  HA_TOPTION_STRING("CHUNK_INTERVAL", chunk_interval),
  HA_TOPTION_STRING("RETENTION_PERIOD", retention_period),
  HA_TOPTION_STRING("COMPRESSION", compression),
  HA_TOPTION_NUMBER("COMPRESSION_LEVEL", compression_level, 3, 1, 19, 1),
  HA_TOPTION_STRING("HOT_THRESHOLD", hot_threshold),
  HA_TOPTION_NUMBER("MEMTABLE_SIZE", memtable_size, 0, 0, 1ULL << 40, 1),
  HA_TOPTION_STRING("TIMESTAMP_COLUMN", timestamp_column),
  HA_TOPTION_END
};

/* ── Helpers ──────────────────────────────────────────────────────────── */

namespace {

constexpr longlong MICROS_PER_SEC= 1000000LL;
constexpr longlong MICROS_PER_DAY= 86400LL * MICROS_PER_SEC;

/* Days since 1970-01-01 (proleptic Gregorian). Mirrors the Rust core. */
longlong days_from_civil(longlong y, unsigned m, unsigned d)
{
  y-= m <= 2;
  const longlong era= (y >= 0 ? y : y - 399) / 400;
  const longlong yoe= y - era * 400;
  const longlong mp= m > 2 ? m - 3 : m + 9;
  const longlong doy= (153 * mp + 2) / 5 + d - 1;
  const longlong doe= yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

void civil_from_days(longlong z, longlong *y, unsigned *m, unsigned *d)
{
  z+= 719468;
  const longlong era= (z >= 0 ? z : z - 146096) / 146097;
  const longlong doe= z - era * 146097;
  const longlong yoe= (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
  const longlong doy= doe - (365 * yoe + yoe / 4 - yoe / 100);
  const longlong mp= (5 * doy + 2) / 153;
  *d= (unsigned) (doy - (153 * mp + 2) / 5 + 1);
  *m= (unsigned) (mp < 10 ? mp + 3 : mp - 9);
  *y= yoe + era * 400 + (*m <= 2);
}

longlong floor_div(longlong a, longlong b)
{
  return a / b - ((a % b != 0) && ((a < 0) != (b < 0)));
}

bool is_tag_comment(const LEX_CSTRING &c)
{
  const char *s= c.str, *e= c.str + c.length;
  while (s < e && my_isspace(system_charset_info, *s)) s++;
  while (e > s && my_isspace(system_charset_info, e[-1])) e--;
  return e - s == 3 && !my_strnncoll(system_charset_info,
                                    (const uchar *) s, 3,
                                    (const uchar *) "TAG", 3);
}

bool is_temporal(const Field *f)
{
  switch (f->real_type()) {
  case MYSQL_TYPE_DATETIME:
  case MYSQL_TYPE_DATETIME2:
  case MYSQL_TYPE_TIMESTAMP:
  case MYSQL_TYPE_TIMESTAMP2:
    return true;
  default:
    return false;
  }
}

/*
  Builds the column layout of a table and validates that its definition is
  something TideFlow can store. On error, a message is written to `err`.
*/
bool build_layout(TABLE_SHARE *s, tf_layout *out, std::string *err)
{
  tf_layout l;
  const ha_table_option_struct *opt= s->option_struct;

  /* Resolve the timestamp column. */
  Field *ts_field= nullptr;
  if (opt && opt->timestamp_column && opt->timestamp_column[0])
  {
    for (uint i= 0; i < s->fields && !ts_field; i++)
      if (!my_strcasecmp(system_charset_info, s->field[i]->field_name.str,
                         opt->timestamp_column))
        ts_field= s->field[i];
    if (!ts_field)
    {
      *err= std::string("TIMESTAMP_COLUMN '") + opt->timestamp_column +
            "' does not exist";
      return true;
    }
  }
  else
  {
    /* Default: the column of the first index on a temporal column. */
    for (uint k= 0; k < s->keys && !ts_field; k++)
    {
      Field *f= s->key_info[k].key_part[0].field;
      if (f && is_temporal(f))
        ts_field= f;
    }
  }
  if (!ts_field)
  {
    *err= "a TideFlow table needs a DATETIME/TIMESTAMP column with an index "
          "(INDEX ts_idx (ts)) or TIMESTAMP_COLUMN = '<column>'";
    return true;
  }
  if (!is_temporal(ts_field) || !ts_field->stored_in_db())
  {
    *err= std::string("timestamp column '") + ts_field->field_name.str +
          "' must be a stored DATETIME or TIMESTAMP column";
    return true;
  }
  if (ts_field->real_maybe_null())
  {
    *err= std::string("timestamp column '") + ts_field->field_name.str +
          "' must be NOT NULL";
    return true;
  }

  /* Indexes: only plain, single-part indexes on the timestamp column. */
  for (uint k= 0; k < s->keys; k++)
  {
    const KEY &key= s->key_info[k];
    if (key.user_defined_key_parts != 1 ||
        key.key_part[0].field != ts_field)
    {
      *err= std::string("index '") + key.name.str +
            "' is not supported: TideFlow only indexes the timestamp column "
            "(TAG columns are indexed implicitly)";
      return true;
    }
    if (key.flags & HA_NOSAME)
    {
      *err= std::string("index '") + key.name.str +
            "' cannot be UNIQUE/PRIMARY: time-series rows are not unique by time";
      return true;
    }
    l.ts_key= k;
  }
  if (l.ts_key == MAX_KEY)
  {
    *err= std::string("missing index on the timestamp column: add INDEX (") +
          ts_field->field_name.str + ")";
    return true;
  }

  for (uint i= 0; i < s->fields; i++)
  {
    Field *f= s->field[i];
    if (!f->stored_in_db())
      continue;
    if (f->flags & AUTO_INCREMENT_FLAG)
    {
      *err= std::string("AUTO_INCREMENT column '") + f->field_name.str +
            "' is not supported";
      return true;
    }
    tf_column c{i, TF_COL_VARCHAR, tf_conv::STRING};
    if (f == ts_field)
      l.ts_column= (uint) l.columns.size();
    if (is_tag_comment(f->comment))
    {
      if (f == ts_field)
      {
        *err= "the timestamp column cannot be a TAG";
        return true;
      }
      c.type= TF_COL_TAG;
      c.conv= tf_conv::TAG;
      l.columns.push_back(c);
      continue;
    }
    switch (f->real_type()) {
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
      c.type= TF_COL_TIMESTAMP; c.conv= tf_conv::DATETIME; break;
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      c.type= TF_COL_TIMESTAMP; c.conv= tf_conv::TIMESTAMP; break;
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_BIT:
      c.type= TF_COL_INT64; c.conv= tf_conv::INT; break;
    case MYSQL_TYPE_FLOAT:
      c.type= TF_COL_FLOAT32; c.conv= tf_conv::FLOAT; break;
    case MYSQL_TYPE_DOUBLE:
      c.type= TF_COL_FLOAT64; c.conv= tf_conv::DOUBLE; break;
    case MYSQL_TYPE_NEWDECIMAL:
      c.type= TF_COL_DECIMAL; c.conv= tf_conv::DECIMAL; break;
    case MYSQL_TYPE_GEOMETRY:
      *err= std::string("column '") + f->field_name.str +
            "': spatial types are not supported";
      return true;
    default:
      /* VARCHAR, CHAR, TEXT/BLOB, DATE, TIME, ENUM, SET, JSON, UUID, ... */
      break;
    }
    l.columns.push_back(c);
  }
  *out= std::move(l);
  return false;
}

/* Owns the strings a TFTableConfig points into. */
struct tf_config_holder
{
  std::vector<const char *> names;
  std::vector<uint8_t> types;
  TFTableConfig cfg{};

  tf_config_holder(TABLE_SHARE *s, const tf_layout &l)
  {
    const ha_table_option_struct *opt= s->option_struct;
    for (const tf_column &c : l.columns)
    {
      names.push_back(s->field[c.field_index]->field_name.str);
      types.push_back((uint8_t) c.type);
    }
    cfg.data_dir= nullptr;
    cfg.chunk_interval= opt ? opt->chunk_interval : nullptr;
    cfg.retention_period= opt ? opt->retention_period : nullptr;
    cfg.compression= opt ? opt->compression : nullptr;
    cfg.compression_level= opt ? (uint8_t) opt->compression_level : 0;
    cfg.hot_threshold= opt ? opt->hot_threshold : nullptr;
    cfg.memtable_size_bytes= opt && opt->memtable_size
                               ? opt->memtable_size
                               : srv_memtable_flush_threshold;
    cfg.ts_column_index= l.ts_column;
    cfg.column_count= (uint32_t) l.columns.size();
    cfg.column_names= names.data();
    cfg.column_types= types.data();
  }
};

/* Message of the last Rust error on this thread ("" if none). */
std::string take_last_error()
{
  char *msg= tideflow_last_error();
  std::string s= msg ? msg : "";
  tideflow_free_str(msg);
  return s;
}

/* Engine-private error codes, reported through get_error_message(). */
constexpr int HA_ERR_TIDEFLOW_BASE= HA_ERR_LAST + 1000;

} // namespace

/* ── Share ────────────────────────────────────────────────────────────── */

TideFlow_share::TideFlow_share()
{
  thr_lock_init(&lock);
}

TideFlow_share::~TideFlow_share()
{
  /*
    The last handler of this TABLE_SHARE is gone (FLUSH TABLES, DROP,
    shutdown, table cache eviction): seal the MemTable so data does not
    linger only in the WAL.
  */
  if (table && tideflow_flush(table.get()) != TF_OK)
    sql_print_warning("TideFlow: flush on close failed: %s",
                      take_last_error().c_str());
  table.reset();
  thr_lock_delete(&lock);
}

/* ── Handler ──────────────────────────────────────────────────────────── */

ha_tideflow::ha_tideflow(handlerton *hton, TABLE_SHARE *table_arg)
  : handler(hton, table_arg)
{
  ref_length= TF_POSITION_LEN;
}

ha_tideflow::~ha_tideflow()= default;

int ha_tideflow::map_status(TFStatus status)
{
  if (status == TF_OK)
    return 0;
  last_error_= take_last_error();
  switch (status) {
  case TF_ERR_FULL:      return HA_ERR_RECORD_FILE_FULL;
  case TF_ERR_READONLY:  return HA_ERR_TABLE_READONLY;
  case TF_ERR_CORRUPT:   return HA_ERR_CRASHED_ON_USAGE;
  case TF_ERR_OOM:       return HA_ERR_OUT_OF_MEM;
  case TF_ERR_NOT_FOUND: return HA_ERR_NO_SUCH_TABLE;
  default:               return HA_ERR_TIDEFLOW_BASE + (int) status;
  }
}

bool ha_tideflow::get_error_message(int error, String *buf)
{
  if (error >= HA_ERR_TIDEFLOW_BASE && !last_error_.empty())
    buf->copy(last_error_.c_str(), last_error_.length(), system_charset_info);
  return false;
}

bool ha_tideflow::tideflow_is_timestamp_column(const Field *field) const
{
  if (!share_ || !field)
    return false;
  const tf_layout &l= share_->layout;
  return l.ts_column < l.columns.size() &&
         l.columns[l.ts_column].field_index == field->field_index;
}

TideFlow_share *ha_tideflow::get_share()
{
  lock_shared_ha_data();
  auto *s= static_cast<TideFlow_share *>(get_ha_share_ptr());
  if (!s)
  {
    s= new (std::nothrow) TideFlow_share;
    if (s)
      set_ha_share_ptr(static_cast<Handler_share *>(s));
  }
  unlock_shared_ha_data();
  return s;
}

int ha_tideflow::create(const char *name, TABLE *table_arg, HA_CREATE_INFO *)
{
  DBUG_ENTER("ha_tideflow::create");
  if (!table_arg || !name)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  tf_layout layout;
  std::string err;
  if (build_layout(table_arg->s, &layout, &err))
  {
    my_printf_error(ER_ILLEGAL_HA_CREATE_OPTION, "TideFlow: %s", MYF(0),
                    err.c_str());
    DBUG_RETURN(HA_WRONG_CREATE_OPTION);
  }
  tf_config_holder cfg(table_arg->s, layout);
  TFStatus st= tideflow_table_create(name, &cfg.cfg);
  if (st != TF_OK)
  {
    std::string msg= take_last_error();
    my_printf_error(ER_ILLEGAL_HA_CREATE_OPTION, "TideFlow: %s", MYF(0),
                    msg.c_str());
    DBUG_RETURN(HA_WRONG_CREATE_OPTION);
  }
  DBUG_RETURN(0);
}

int ha_tideflow::open(const char *name, int, uint)
{
  DBUG_ENTER("ha_tideflow::open");
  if (!(share_= get_share()))
    DBUG_RETURN(HA_ERR_OUT_OF_MEM);

  lock_shared_ha_data();
  int error= 0;
  if (!share_->table)
  {
    std::string err;
    if (build_layout(table->s, &share_->layout, &err))
    {
      last_error_= err;
      error= HA_ERR_TIDEFLOW_BASE + TF_ERR_INVALID_ARG;
    }
    else
    {
      tf_config_holder cfg(table->s, share_->layout);
      TideFlowTable *t= nullptr;
      error= map_status(tideflow_table_open(name, &cfg.cfg, &t));
      share_->table.reset(t);
      if (error)
        sql_print_error("TideFlow: cannot open %s: %s", name,
                        last_error_.c_str());
    }
  }
  unlock_shared_ha_data();
  if (error)
    DBUG_RETURN(error);

  thr_lock_data_init(&share_->lock, &lock_data_, NULL);
  str_bufs_.resize(share_->layout.columns.size());
  values_.resize(share_->layout.columns.size());
  DBUG_RETURN(0);
}

int ha_tideflow::close()
{
  DBUG_ENTER("ha_tideflow::close");
  scan_.reset();
  DBUG_RETURN(0);
}

int ha_tideflow::delete_table(const char *name)
{
  DBUG_ENTER("ha_tideflow::delete_table");
  TFStatus st= tideflow_table_drop(name);
  if (st == TF_ERR_NOT_FOUND)
  {
    take_last_error();
    DBUG_RETURN(ENOENT);
  }
  DBUG_RETURN(map_status(st));
}

int ha_tideflow::rename_table(const char *from, const char *to)
{
  DBUG_ENTER("ha_tideflow::rename_table");
  TFStatus st= tideflow_table_rename(from, to);
  if (st == TF_ERR_NOT_FOUND)
  {
    take_last_error();
    DBUG_RETURN(ENOENT);
  }
  DBUG_RETURN(map_status(st));
}

/* ── Row conversion ───────────────────────────────────────────────────── */

int ha_tideflow::write_row(const uchar *buf)
{
  DBUG_ENTER("ha_tideflow::write_row");
  if (!share_ || !share_->table)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  const tf_layout &l= share_->layout;
  const my_ptrdiff_t diff= buf - table->record[0];
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->read_set);
  int error= 0;

  for (size_t i= 0; i < l.columns.size() && !error; i++)
  {
    const tf_column &c= l.columns[i];
    Field *f= table->field[c.field_index];
    TFValue &v= values_[i];
    v.kind= (uint8_t) c.type;
    v.data.int_val= 0;
    f->move_field_offset(diff);
    v.is_null= f->is_null();
    if (!v.is_null)
    {
      switch (c.conv) {
      case tf_conv::DATETIME:
      {
        MYSQL_TIME lt;
        if (f->get_date(&lt, date_mode_t(0)) || lt.month == 0 || lt.day == 0)
        {
          last_error_= std::string("column '") + f->field_name.str +
                       "': zero or partial dates cannot be stored";
          error= HA_ERR_TIDEFLOW_BASE + TF_ERR_INVALID_ARG;
          break;
        }
        const longlong days= days_from_civil(lt.year, lt.month, lt.day);
        const longlong secs= ((days * 24 + lt.hour) * 60 + lt.minute) * 60 +
                             lt.second;
        v.data.ts_us= secs * MICROS_PER_SEC + (longlong) lt.second_part;
        break;
      }
      case tf_conv::TIMESTAMP:
      {
        ulong usec= 0;
        const my_time_t secs= f->get_timestamp(&usec);
        v.data.ts_us= (longlong) secs * MICROS_PER_SEC + (longlong) usec;
        break;
      }
      case tf_conv::INT:
        v.data.int_val= f->val_int();
        break;
      case tf_conv::FLOAT:
        v.data.float32_val= (float) f->val_real();
        break;
      case tf_conv::DOUBLE:
        v.data.float_val= f->val_real();
        break;
      case tf_conv::DECIMAL:
      case tf_conv::STRING:
      case tf_conv::TAG:
      {
        String *s= f->val_str(&str_bufs_[i], &str_bufs_[i]);
        v.data.str_val.ptr= s ? s->ptr() : "";
        v.data.str_val.len= s ? (uint32_t) s->length() : 0;
        break;
      }
      }
    }
    f->move_field_offset(-diff);
  }
  dbug_tmp_restore_column_map(&table->read_set, old_map);
  if (error)
    DBUG_RETURN(error);

  TFRow row{(uint32_t) values_.size(), values_.data()};
  error= map_status(tideflow_write_row(share_->table.get(), &row));
  if (!error)
    wrote_rows_= true;
  DBUG_RETURN(error);
}

int ha_tideflow::fill_record(uchar *buf, const TFRow &row)
{
  const tf_layout &l= share_->layout;
  if (row.col_count != l.columns.size() || !row.values)
  {
    last_error_= "row shape does not match the table definition";
    return HA_ERR_TIDEFLOW_BASE + TF_ERR_CORRUPT;
  }
  memset(buf, 0, table->s->null_bytes);
  const my_ptrdiff_t diff= buf - table->record[0];
  MY_BITMAP *old_map= dbug_tmp_use_all_columns(table, &table->write_set);

  for (size_t i= 0; i < l.columns.size(); i++)
  {
    const tf_column &c= l.columns[i];
    const TFValue &v= row.values[i];
    Field *f= table->field[c.field_index];
    f->move_field_offset(diff);
    if (v.is_null)
    {
      f->set_null();
      f->reset();
    }
    else
    {
      f->set_notnull();
      switch (c.conv) {
      case tf_conv::DATETIME:
      {
        const longlong us= v.data.ts_us;
        const longlong days= floor_div(us, MICROS_PER_DAY);
        const longlong tod= us - days * MICROS_PER_DAY;
        longlong y; unsigned m, d;
        civil_from_days(days, &y, &m, &d);
        MYSQL_TIME lt;
        memset(&lt, 0, sizeof(lt));
        lt.year= (uint) y; lt.month= m; lt.day= d;
        lt.hour= (uint) (tod / (3600 * MICROS_PER_SEC));
        lt.minute= (uint) (tod / (60 * MICROS_PER_SEC) % 60);
        lt.second= (uint) (tod / MICROS_PER_SEC % 60);
        lt.second_part= (ulong) (tod % MICROS_PER_SEC);
        lt.time_type= MYSQL_TIMESTAMP_DATETIME;
        f->store_time_dec(&lt, f->decimals());
        break;
      }
      case tf_conv::TIMESTAMP:
      {
        const longlong us= v.data.ts_us;
        const longlong secs= floor_div(us, MICROS_PER_SEC);
        f->store_timestamp((my_time_t) secs, (ulong) (us - secs * MICROS_PER_SEC));
        break;
      }
      case tf_conv::INT:
        f->store(v.data.int_val, f->is_unsigned());
        break;
      case tf_conv::FLOAT:
        f->store((double) v.data.float32_val);
        break;
      case tf_conv::DOUBLE:
        f->store(v.data.float_val);
        break;
      case tf_conv::DECIMAL:
      case tf_conv::STRING:
      case tf_conv::TAG:
        f->store(v.data.str_val.ptr ? v.data.str_val.ptr : "",
                 v.data.str_val.len, f->charset());
        break;
      }
    }
    f->move_field_offset(-diff);
  }
  dbug_tmp_restore_column_map(&table->write_set, old_map);
  return 0;
}

/* ── Statement end / durability ───────────────────────────────────────── */

int ha_tideflow::end_bulk_insert()
{
  if (!wrote_rows_ || !share_ || !share_->table)
    return 0;
  wrote_rows_= false;
  return map_status(tideflow_sync_wal(share_->table.get(),
                                      srv_wal_sync_mode == WAL_SYNC_FSYNC));
}

int ha_tideflow::external_lock(THD *, int lock_type)
{
  DBUG_ENTER("ha_tideflow::external_lock");
  if (lock_type != F_UNLCK || !share_ || !share_->table)
    DBUG_RETURN(0);

  /*
    Statement end. TideFlow is non-transactional, so this is the point where
    the client's OK is about to be sent: make appended rows durable.
  */
  int error= end_bulk_insert();

  if (srv_retention_check_interval > 0)
  {
    const time_t now= time(nullptr);
    time_t last= share_->last_retention_check.load();
    if (now - last >= (time_t) srv_retention_check_interval &&
        share_->last_retention_check.compare_exchange_strong(last, now))
    {
      if (tideflow_apply_retention(share_->table.get()) != TF_OK)
        sql_print_warning("TideFlow: retention sweep of %s failed: %s",
                          table->s->path.str, take_last_error().c_str());
    }
  }
  DBUG_RETURN(error);
}

THR_LOCK_DATA **ha_tideflow::store_lock(THD *, THR_LOCK_DATA **to,
                                        enum thr_lock_type lock_type)
{
  /*
    Plain table locks: writers are exclusive. Scans take snapshots in the
    core, but MemTable row positions (rnd_pos) are only stable while no
    concurrent flush can happen, which exclusive writes guarantee.
  */
  if (lock_type != TL_IGNORE && lock_data_.type == TL_UNLOCK)
    lock_data_.type= lock_type;
  *to++= &lock_data_;
  return to;
}

/* ── Maintenance ──────────────────────────────────────────────────────── */

int ha_tideflow::delete_all_rows()
{
  DBUG_ENTER("ha_tideflow::delete_all_rows");
  if (!share_ || !share_->table)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  scan_.reset();
  DBUG_RETURN(map_status(tideflow_truncate(share_->table.get())));
}

int ha_tideflow::truncate()
{
  return delete_all_rows();
}

int ha_tideflow::tideflow_force_flush()
{
  if (!share_ || !share_->table)
    return HA_ERR_INTERNAL_ERROR;
  return map_status(tideflow_flush(share_->table.get()));
}

int ha_tideflow::check(THD *thd, HA_CHECK_OPT *)
{
  DBUG_ENTER("ha_tideflow::check");
  uint32_t bad= 0;
  if (int error= map_status(tideflow_check(share_->table.get(), &bad)))
    DBUG_RETURN(error);
  if (bad)
  {
    std::string msg= take_last_error();
    push_warning_printf(thd, Sql_condition::WARN_LEVEL_WARN, ER_NOT_KEYFILE,
                        "TideFlow: %u corrupt chunk(s): %s", bad, msg.c_str());
    sql_print_error("TideFlow: CHECK TABLE %s found %u corrupt chunk(s): %s",
                    table->s->path.str, bad, msg.c_str());
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }
  DBUG_RETURN(HA_ADMIN_OK);
}

int ha_tideflow::optimize(THD *, HA_CHECK_OPT *)
{
  DBUG_ENTER("ha_tideflow::optimize");
  /* Seal the MemTable and expire old chunks. Chunk merging (compaction)
     will be added here once implemented in the core. */
  if (tideflow_force_flush() ||
      map_status(tideflow_apply_retention(share_->table.get())))
    DBUG_RETURN(HA_ADMIN_FAILED);
  DBUG_RETURN(HA_ADMIN_OK);
}

/* ── Full scans ───────────────────────────────────────────────────────── */

int ha_tideflow::rnd_init(bool scan)
{
  DBUG_ENTER("ha_tideflow::rnd_init");
  scan_.reset();
  TideFlowScan *s= nullptr;
  TFStatus st;
  if (scan)
    st= tideflow_scan_open(share_->table.get(), &s);
  else
    /* rnd_pos() only: an empty range opens a handle without copying data. */
    st= tideflow_range_scan_open(share_->table.get(), 1, 0, nullptr, nullptr,
                                 0, &s);
  scan_.reset(s);
  DBUG_RETURN(map_status(st));
}

int ha_tideflow::read_from_scan(uchar *buf, bool backward, int end_error)
{
  if (!scan_)
    return HA_ERR_INTERNAL_ERROR;
  TFRow row{0, nullptr};
  bool end= false;
  TFStatus st= backward ? tideflow_scan_prev(scan_.get(), &row, &end)
                        : tideflow_scan_next(scan_.get(), &row, &end);
  if (int error= map_status(st))
    return error;
  if (end)
    return end_error;
  return fill_record(buf, row);
}

int ha_tideflow::rnd_next(uchar *buf)
{
  DBUG_ENTER("ha_tideflow::rnd_next");
  DBUG_RETURN(read_from_scan(buf, false, HA_ERR_END_OF_FILE));
}

int ha_tideflow::rnd_end()
{
  DBUG_ENTER("ha_tideflow::rnd_end");
  scan_.reset();
  DBUG_RETURN(0);
}

void ha_tideflow::position(const uchar *)
{
  DBUG_ENTER("ha_tideflow::position");
  if (!scan_ || tideflow_scan_position(scan_.get(), ref) != TF_OK)
  {
    take_last_error();
    memset(ref, 0xff, ref_length);
  }
  DBUG_VOID_RETURN;
}

int ha_tideflow::rnd_pos(uchar *buf, uchar *pos)
{
  DBUG_ENTER("ha_tideflow::rnd_pos");
  if (!scan_)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  TFRow row{0, nullptr};
  if (int error= map_status(tideflow_scan_fetch(scan_.get(), pos, &row)))
    DBUG_RETURN(error == HA_ERR_NO_SUCH_TABLE ? HA_ERR_RECORD_DELETED : error);
  DBUG_RETURN(fill_record(buf, row));
}

/* ── Index scans ──────────────────────────────────────────────────────── */

int ha_tideflow::index_init(uint idx, bool)
{
  DBUG_ENTER("ha_tideflow::index_init");
  active_index= idx;
  scan_.reset();
  DBUG_RETURN(0);
}

int ha_tideflow::index_end()
{
  DBUG_ENTER("ha_tideflow::index_end");
  scan_.reset();
  active_index= MAX_KEY;
  DBUG_RETURN(0);
}

longlong ha_tideflow::key_to_ts(uint idx, const uchar *key)
{
  KEY *ki= &table->key_info[idx];
  Field *f= ki->key_part[0].field;
  /* Decode through the field itself, using record[1] as scratch space. */
  key_restore(table->record[1], key, ki, ki->key_part[0].store_length);
  const my_ptrdiff_t diff= table->record[1] - table->record[0];
  f->move_field_offset(diff);
  longlong ts;
  if (f->real_type() == MYSQL_TYPE_TIMESTAMP ||
      f->real_type() == MYSQL_TYPE_TIMESTAMP2)
  {
    ulong usec= 0;
    ts= (longlong) f->get_timestamp(&usec) * MICROS_PER_SEC + (longlong) usec;
  }
  else
  {
    MYSQL_TIME lt;
    if (f->get_date(&lt, date_mode_t(0)) || lt.month == 0 || lt.day == 0)
      ts= LONGLONG_MIN;   /* zero date sorts before everything */
    else
      ts= (((days_from_civil(lt.year, lt.month, lt.day) * 24 + lt.hour) * 60 +
            lt.minute) * 60 + lt.second) * MICROS_PER_SEC +
          (longlong) lt.second_part;
  }
  f->move_field_offset(-diff);
  return ts;
}

int ha_tideflow::open_range_scan(longlong lo, longlong hi, bool backward,
                                 uchar *buf, int not_found_error)
{
  scan_.reset();
  TideFlowScan *s= nullptr;
  int error= map_status(tideflow_range_scan_open(share_->table.get(), lo, hi,
                                                 nullptr, nullptr, 0, &s));
  scan_.reset(s);
  if (error)
    return error;
  if (backward && (error= map_status(tideflow_scan_seek_end(scan_.get()))))
    return error;
  return read_from_scan(buf, backward, not_found_error);
}

int ha_tideflow::index_read_map(uchar *buf, const uchar *key,
                                key_part_map keypart_map,
                                enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_tideflow::index_read_map");
  if (!key || !keypart_map)
    DBUG_RETURN(find_flag == HA_READ_PREFIX_LAST ||
                find_flag == HA_READ_PREFIX_LAST_OR_PREV
                ? index_last(buf) : index_first(buf));

  const longlong ts= key_to_ts(active_index, key);
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  bool backward= false;
  switch (find_flag) {
  case HA_READ_KEY_EXACT:
  case HA_READ_PREFIX:
    lo= hi= ts; break;
  case HA_READ_KEY_OR_NEXT:
    lo= ts; break;
  case HA_READ_AFTER_KEY:
    if (ts == LONGLONG_MAX)
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    lo= ts + 1; break;
  case HA_READ_KEY_OR_PREV:
  case HA_READ_PREFIX_LAST_OR_PREV:
    hi= ts; backward= true; break;
  case HA_READ_BEFORE_KEY:
    if (ts == LONGLONG_MIN)
      DBUG_RETURN(HA_ERR_KEY_NOT_FOUND);
    hi= ts - 1; backward= true; break;
  case HA_READ_PREFIX_LAST:
    lo= hi= ts; backward= true; break;
  default:
    DBUG_RETURN(HA_ERR_WRONG_COMMAND);
  }
  /*
    For forward range reads the server has already set end_range (see
    handler::read_range_first) and stops at it; bounding the scan with it
    avoids materializing rows past the end of the range. Bounding by the end
    key inclusively is always a superset, the server re-checks the bound.
  */
  if (!backward && end_range && end_range->key)
    hi= std::min(hi, key_to_ts(active_index, end_range->key));
  DBUG_RETURN(open_range_scan(lo, hi, backward, buf, HA_ERR_KEY_NOT_FOUND));
}

int ha_tideflow::index_next(uchar *buf)
{
  DBUG_ENTER("ha_tideflow::index_next");
  DBUG_RETURN(read_from_scan(buf, false, HA_ERR_END_OF_FILE));
}

int ha_tideflow::index_prev(uchar *buf)
{
  DBUG_ENTER("ha_tideflow::index_prev");
  DBUG_RETURN(read_from_scan(buf, true, HA_ERR_END_OF_FILE));
}

int ha_tideflow::index_first(uchar *buf)
{
  DBUG_ENTER("ha_tideflow::index_first");
  DBUG_RETURN(open_range_scan(LONGLONG_MIN, LONGLONG_MAX, false, buf,
                              HA_ERR_END_OF_FILE));
}

int ha_tideflow::index_last(uchar *buf)
{
  DBUG_ENTER("ha_tideflow::index_last");
  DBUG_RETURN(open_range_scan(LONGLONG_MIN, LONGLONG_MAX, true, buf,
                              HA_ERR_END_OF_FILE));
}

/* ── Statistics ───────────────────────────────────────────────────────── */

int ha_tideflow::info(uint flag)
{
  DBUG_ENTER("ha_tideflow::info");
  if (!share_ || !share_->table)
    DBUG_RETURN(0);
  if (flag & (HA_STATUS_VARIABLE | HA_STATUS_CONST))
  {
    uint64_t rows= 0, data= 0, compressed= 0;
    uint32_t chunks= 0;
    if (int error= map_status(tideflow_table_stats(share_->table.get(), &rows,
                                                   &data, &compressed,
                                                   &chunks)))
      DBUG_RETURN(error);
    stats.records= (ha_rows) rows;
    stats.deleted= 0;
    stats.data_file_length= compressed;
    stats.index_file_length= 0;
    stats.mean_rec_length= rows ? (ulong) (data / rows) : 0;
  }
  DBUG_RETURN(0);
}

ha_rows ha_tideflow::records_in_range(uint inx, const key_range *min_key,
                                      const key_range *max_key, page_range *)
{
  DBUG_ENTER("ha_tideflow::records_in_range");
  longlong lo= LONGLONG_MIN, hi= LONGLONG_MAX;
  if (min_key)
  {
    lo= key_to_ts(inx, min_key->key);
    if (min_key->flag == HA_READ_AFTER_KEY && lo < LONGLONG_MAX)
      lo++;
  }
  if (max_key)
  {
    hi= key_to_ts(inx, max_key->key);
    if (max_key->flag == HA_READ_BEFORE_KEY && hi > LONGLONG_MIN)
      hi--;
  }
  uint64_t rows= 0;
  if (tideflow_estimate_rows(share_->table.get(), lo, hi, &rows) != TF_OK)
  {
    take_last_error();
    DBUG_RETURN(HA_POS_ERROR);
  }
  /* Never 0: the optimizer would treat the range as provably empty. */
  DBUG_RETURN((ha_rows) std::max<uint64_t>(rows, 1));
}

/* ── Plugin registration ──────────────────────────────────────────────── */

static handler *tideflow_create_handler(handlerton *hton, TABLE_SHARE *table,
                                        MEM_ROOT *mem_root)
{
  return new (mem_root) ha_tideflow(hton, table);
}

/* Table data lives in a directory, not in files with extensions. */
static const char *tideflow_exts[]= {NullS};

static int tideflow_init_func(void *p)
{
  DBUG_ENTER("tideflow_init_func");
  tideflow_hton= static_cast<handlerton *>(p);
  tideflow_hton->create= tideflow_create_handler;
  tideflow_hton->flags= HTON_NO_FLAGS;
  tideflow_hton->table_options= tideflow_table_option_list;
  tideflow_hton->tablefile_extensions= tideflow_exts;
  DBUG_RETURN(0);
}

static struct st_mysql_storage_engine tideflow_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

maria_declare_plugin(tideflow)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &tideflow_storage_engine,
  "TideFlow",
  "Kevenny / Rech Informatica",
  "Time-series storage engine: append-only, chunked by time, WAL-backed",
  PLUGIN_LICENSE_GPL,
  tideflow_init_func,                    /* Plugin Init   */
  NULL,                                  /* Plugin Deinit */
  0x0001,                                /* version 0.1   */
  NULL,                                  /* status vars   */
  tideflow_system_variables,             /* system vars   */
  "0.1.0",                               /* string version */
  MariaDB_PLUGIN_MATURITY_EXPERIMENTAL   /* maturity      */
}
maria_declare_plugin_end;
