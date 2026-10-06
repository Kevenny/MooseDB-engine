/*
  TideFlow storage engine — TABLE OPTIONS.

  Copyright (c) 2026 Kevenny / Rech Informática

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.
*/

#pragma once

#include "handler.h"

/*
  The server types TABLE_SHARE::option_struct as `ha_table_option_struct *`,
  so the engine's struct must carry exactly this name.

  String options are validated by the Rust core at CREATE TABLE time; a NULL
  value selects the documented default.
*/
struct ha_table_option_struct
{
  const char *chunk_interval;   /* '1 HOUR' .. '1 MONTH'          (default '1 DAY')   */
  const char *retention_period; /* 'N DAYS' ... | 'FOREVER'        (default FOREVER)   */
  const char *compression;      /* 'ZSTD' | 'LZ4' | 'NONE'         (default 'ZSTD')    */
  ulonglong compression_level;  /* 1-19                            (default 3)         */
  const char *hot_threshold;    /* 'N DAYS' ...                    (default '7 DAYS')  */
  ulonglong memtable_size;      /* bytes; 0 = tideflow_memtable_flush_threshold       */
  const char *timestamp_column; /* NULL = the column of the timestamp index           */
};

extern ha_create_table_option tideflow_table_option_list[];
