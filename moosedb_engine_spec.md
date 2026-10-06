# MooseDB Engine — Especificação Técnica

> Storage engine time-series nativa para MariaDB 11.4+.
> Documento de arquitetura e implementação destinado ao **Claude Code**.
> Leia este arquivo inteiro antes de escrever qualquer linha de código.

---

## 1. Visão geral e motivação

O MariaDB não possui storage engine otimizada para dados de série temporal.
Workloads time-series têm características radicalmente diferentes do OLTP:

- **Escrita monotônica** — timestamps sempre crescentes, nunca atualizados
- **Leitura por range** — `WHERE ts BETWEEN t1 AND t2` é o padrão dominante
- **Cardinalidade controlada** — número de séries (métricas, sensores, hosts) é finito
- **Compressibilidade altíssima** — deltas de timestamp e valores numéricos consecutivos
  comprimem 10–30x com algoritmos especializados
- **TTL obrigatório** — dados antigos expiram; nenhum banco OLTP faz isso nativamente
- **Sem UPDATE/DELETE por linha** — dados imutáveis após inserção

Nenhuma engine disponível no MariaDB Community atende esse perfil:
InnoDB tem overhead de MVCC desnecessário, MyRocks é LSM genérico sem
chunk-awareness temporal, e S3 é somente leitura.

**MooseDB** é a resposta: uma engine plugável, escrita em **C++ moderno (C++20)**
com núcleo de storage em **Rust** exposto via FFI C, integrada à MariaDB Handler API,
compatível com MariaDB 11.4 LTS em diante.

---

## 2. Decisões de linguagem e segurança

### Por que C++ + Rust (não C++ puro)

A Handler API do MariaDB exige C++ para a camada de integração (`handler` subclass),
mas o núcleo de storage — onde vivem os bugs de memória, race conditions e
vulnerabilidades — será implementado em **Rust**.

```
┌─────────────────────────────────────────┐
│  MariaDB Server (C++)                   │
│                                         │
│  ┌──────────────────────────────────┐   │
│  │  ha_moosedb.cc / .h  (C++)      │   │  ← Handler API, DDL, SQL parsing
│  │  Herda de: handler               │   │
│  └────────────┬─────────────────────┘   │
│               │ FFI C ABI               │
│  ┌────────────▼─────────────────────┐   │
│  │  libmoosedb.so  (Rust)          │   │  ← Storage, compressão, índices, WAL
│  │  Expõe API C pura (no_std safe)  │   │
│  └──────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

**Benefícios concretos:**
- Zero buffer overflows, use-after-free ou data races no núcleo de storage
- Gorilla/Delta encoding e ZSTD implementados em Rust safe
- C++ restrito à camada de integração MariaDB — menor superfície de risco
- FFI C ABI estável — a fronteira C++↔Rust usa apenas tipos C primitivos

### Padrões de segurança obrigatórios no C++

```cpp
// Proibido — usar apenas smart pointers
raw_ptr = new SomeObject();                 // ❌
auto obj = std::make_unique<SomeObject>();  // ✅

// Proibido — usar std::span ou bounds-checked acesso
buffer[i] = val;                            // ❌ se i não verificado
if (i < buf.size()) buf[i] = val;           // ✅

// Todos os ponteiros recebidos da MariaDB API são verificados antes de uso
assert(table != nullptr);
if (!table) return HA_ERR_INTERNAL_ERROR;
```

### Padrões de segurança no Rust

- `#![forbid(unsafe_code)]` em todos os crates exceto `moosedb-ffi`
- `moosedb-ffi` contém APENAS os `extern "C"` de entrada — sem lógica
- Toda lógica de storage em crates `unsafe`-free

---

## 3. Arquitetura interna

### 3.1 Estrutura de dados: TSM Tree (Time Structured Merge Tree)

Inspirada no InfluxDB TSM e no TDengine TSDB, adaptada ao modelo relacional
do MariaDB.

```
Escrita de dados
       │
       ▼
┌──────────────┐    Full     ┌──────────────────┐
│   WAL        │────────────►│  MemTable        │
│  (append     │             │  (BTreeMap        │
│   only)      │             │   série→dados)    │
└──────────────┘             └────────┬─────────┘
                                      │ Flush (threshold)
                                      ▼
                             ┌──────────────────┐
                             │  Chunks (disco)  │
                             │  .tfl files      │
                             │  por intervalo   │
                             │  de tempo        │
                             └────────┬─────────┘
                                      │ Compaction
                                      ▼
                             ┌──────────────────┐
                             │  Chunks frios    │
                             │  (compressão     │
                             │   máxima ZSTD)   │
                             └──────────────────┘
```

### 3.2 Chunks — unidade fundamental de storage

Cada tabela MooseDB é dividida em **chunks** por intervalo de tempo configurável.
Um chunk é um arquivo `.tfl` imutável após fechamento.

```
chunk_20261001T000000_20261002T000000.tfl
│
├── Header (64 bytes)
│   ├── magic:        "TFLW" (4 bytes)
│   ├── version:      uint16
│   ├── flags:        uint16  (compressed, encrypted, sealed)
│   ├── ts_min:       int64   (Unix microssegundos)
│   ├── ts_max:       int64
│   ├── row_count:    uint64
│   ├── series_count: uint32
│   └── checksum:     uint32  (CRC32 do header)
│
├── Series Index
│   ├── series_id → offset/tamanho dos blocos de dados
│   └── Bloom filter (para existência de série no chunk)
│
├── Data Blocks (por série, colunar)
│   ├── Timestamps block  (Delta-of-delta + Simple8b)
│   ├── Column_1 block    (Gorilla para float, Delta para int)
│   ├── Column_2 block    ...
│   └── NULL bitmap
│
└── Footer
    ├── index_offset: uint64
    ├── bloom_offset: uint64
    └── checksum:     uint32 (CRC32 do arquivo completo)
```

### 3.3 Série — conceito central

Uma **série** é a combinação única de (tabela, conjunto de tags/labels).
Exemplo: métricas de CPU de um host específico formam uma série.

```sql
-- Esta query acessa 1 série
SELECT * FROM metricas WHERE host='srv01' AND metric='cpu_pct'
  AND ts BETWEEN '2026-10-01' AND '2026-10-02';

-- Esta query acessa N séries (fan-out por host)
SELECT host, AVG(value) FROM metricas
  WHERE metric='cpu_pct'
  AND ts BETWEEN '2026-10-01' AND '2026-10-02'
  GROUP BY host;
```

O Series Index mapeia a combinação de valores das colunas TAG para um
`series_id` (uint64) que é a chave interna de todos os índices.

### 3.4 Compressão por tipo de dado

| Tipo da coluna   | Algoritmo primário          | Fallback |
|------------------|-----------------------------|----------|
| TIMESTAMP/DATETIME | Delta-of-delta + Simple8b | LZ4      |
| FLOAT/DOUBLE     | Gorilla XOR encoding        | ZSTD     |
| INT/BIGINT       | Delta encoding + Simple8b   | ZSTD     |
| TINYINT (boolean)| Run-length encoding         | Bitmap   |
| VARCHAR/TEXT     | ZSTD dicionário             | LZ4      |
| DECIMAL          | Scale + Delta + Simple8b    | ZSTD     |
| NULL bitmap      | Run-length encoding         | —        |

**Chunk quente** (< `hot_threshold`): LZ4 — compressão rápida, CPU baixo
**Chunk frio**  (> `hot_threshold`): ZSTD nível 3–9 — máxima compressão

### 3.5 WAL (Write-Ahead Log)

```
wal_000001.log
├── Entry 1: [CRC32][length][timestamp][series_id][column_values...]
├── Entry 2: ...
└── ...

Ciclo de vida:
  1. INSERT → grava no WAL antes de qualquer outra operação
  2. WAL confirmado → responde OK ao cliente
  3. MemTable atualizada em background
  4. Flush da MemTable → chunk fechado e selado
  5. WAL até esse ponto descartado (truncate)
  6. Crash recovery: replay do WAL do último checkpoint
```

---

## 4. Estrutura do projeto

```
moosedb/
├── storage/moosedb/                   # Diretório da engine no source tree MariaDB
│   ├── CMakeLists.txt                  # Build integrado ao MariaDB cmake
│   ├── ha_moosedb.h                   # Declaração da classe handler
│   ├── ha_moosedb.cc                  # Implementação C++ da Handler API
│   ├── moosedb_options.h              # TABLE_OPTIONS e system variables
│   ├── moosedb_ffi.h                  # Declarações C da API Rust (gerado por cbindgen)
│   └── rust/                           # Crate workspace Rust
│       ├── Cargo.toml                  # Workspace
│       ├── moosedb-core/              # Lógica de storage (unsafe-free)
│       │   ├── Cargo.toml
│       │   └── src/
│       │       ├── lib.rs
│       │       ├── chunk.rs            # Estrutura e I/O de chunks
│       │       ├── chunk_writer.rs     # Escrita de chunks
│       │       ├── chunk_reader.rs     # Leitura e decompressão
│       │       ├── memtable.rs         # Buffer em memória
│       │       ├── wal.rs              # Write-ahead log
│       │       ├── series_index.rs     # Índice de séries (hash+bloom)
│       │       ├── compaction.rs       # Merge de chunks
│       │       ├── retention.rs        # TTL e expiração automática
│       │       ├── compression/
│       │       │   ├── mod.rs
│       │       │   ├── gorilla.rs      # XOR float encoding
│       │       │   ├── delta.rs        # Delta/delta-of-delta
│       │       │   ├── simple8b.rs     # Simple8b integer packing
│       │       │   └── rle.rs          # Run-length encoding
│       │       └── index/
│       │           ├── bloom.rs        # Bloom filter por chunk
│       │           └── series.rs       # Mapeamento tag→series_id
│       ├── moosedb-ffi/               # Camada FFI (contém unsafe)
│       │   ├── Cargo.toml
│       │   └── src/
│       │       └── lib.rs              # extern "C" de entrada, zero lógica
│       └── moosedb-tests/             # Testes de integração Rust
│           ├── Cargo.toml
│           └── src/
│               └── lib.rs
├── mysql-test/suite/moosedb/          # Testes MTR
│   ├── t/
│   │   ├── basic.test
│   │   ├── retention.test
│   │   ├── compression.test
│   │   ├── chunk_query.test
│   │   └── crash_recovery.test
│   └── r/                              # Resultados esperados
└── docs/
    ├── architecture.md
    └── user-guide.md
```

---

## 5. Handler API — métodos obrigatórios

```cpp
// storage/moosedb/ha_moosedb.h

#pragma once
#include "handler.h"
#include "moosedb_ffi.h"

// Opções de tabela específicas da MooseDB
struct ha_moosedb_table_option_struct {
    const char *chunk_interval;    // ex: "1 DAY", "1 HOUR", "1 WEEK"
    const char *retention_period;  // ex: "90 DAYS", "1 YEAR", "FOREVER"
    const char *compression;       // "ZSTD", "LZ4", "NONE"
    uint        compression_level; // 1–19 para ZSTD, ignorado para LZ4
    const char *hot_threshold;     // ex: "7 DAYS" — fronteira quente/frio
    longlong    memtable_size;     // bytes — default 64MB
    const char *timestamp_column;  // nome da coluna timestamp (obrigatório)
};

class ha_moosedb : public handler {
public:
    ha_moosedb(handlerton *hton, TABLE_SHARE *table_arg);
    ~ha_moosedb() override;

    // ── Identificação ─────────────────────────────────────────────────────
    const char *table_type() const override { return "MooseDB"; }
    const char *index_type(uint inx) override { return "TSIDX"; }

    // ── DDL ───────────────────────────────────────────────────────────────
    int  create(const char *name, TABLE *table_arg,
                HA_CREATE_INFO *create_info) override;
    int  open(const char *name, int mode, uint test_if_locked) override;
    int  close() override;
    int  delete_table(const char *name) override;
    int  rename_table(const char *from, const char *to) override;

    // ── DML ───────────────────────────────────────────────────────────────
    int  write_row(const uchar *buf) override;
    int  update_row(const uchar *old_data,
                    const uchar *new_data) override;  // retorna HA_ERR_WRONG_COMMAND
    int  delete_row(const uchar *buf) override;        // retorna HA_ERR_WRONG_COMMAND

    // ── Scans ─────────────────────────────────────────────────────────────
    int  rnd_init(bool scan) override;
    int  rnd_next(uchar *buf) override;
    int  rnd_end() override;
    int  rnd_pos(uchar *buf, uchar *pos) override;
    void position(const uchar *record) override;

    // ── Index (range scan por timestamp) ──────────────────────────────────
    int  index_init(uint idx, bool sorted) override;
    int  index_read_map(uchar *buf, const uchar *key,
                        key_part_map keypart_map,
                        enum ha_rkey_function find_flag) override;
    int  index_next(uchar *buf) override;
    int  index_prev(uchar *buf) override;
    int  index_end() override;

    // ── Info ──────────────────────────────────────────────────────────────
    int  info(uint flag) override;
    ha_rows records_in_range(uint inx, const key_range *min_key,
                             const key_range *max_key) override;

    // ── Capacidades ───────────────────────────────────────────────────────
    ulonglong table_flags() const override {
        return (HA_NO_TRANSACTIONS      |   // sem MVCC overhead
                HA_REC_NOT_IN_SEQ      |
                HA_CAN_INDEX_BLOBS     |
                HA_STATS_RECORDS_IS_EXACT);
    }
    ulong index_flags(uint inx, uint part, bool all_parts) const override {
        return (HA_READ_NEXT | HA_READ_PREV | HA_READ_RANGE);
    }
    uint max_supported_keys() const override      { return 2; }
    uint max_supported_key_parts() const override { return 4; }

    // ── Específicos MooseDB ───────────────────────────────────────────────
    bool moosedb_is_timestamp_column(Field *field) const;
    int  moosedb_force_flush();
    int  moosedb_compact_chunks(const char *from_ts, const char *to_ts);

private:
    MooseDBTable  *tf_table_;   // handle opaco do Rust
    MooseDBScan   *tf_scan_;    // estado do scan atual
    MEM_ROOT        mem_root_;
};
```

---

## 6. API FFI C (interface Rust → C++)

```c
// storage/moosedb/moosedb_ffi.h
// Gerado por cbindgen — não editar manualmente

#pragma once
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// ── Handles opacos ────────────────────────────────────────────────────────────
typedef struct MooseDBTable  MooseDBTable;
typedef struct MooseDBScan   MooseDBScan;
typedef struct MooseDBWriter MooseDBWriter;

// ── Resultado de operações ────────────────────────────────────────────────────
typedef enum {
    TF_OK              = 0,
    TF_ERR_IO          = 1,
    TF_ERR_CORRUPT     = 2,
    TF_ERR_FULL        = 3,
    TF_ERR_NOT_FOUND   = 4,
    TF_ERR_INVALID_ARG = 5,
    TF_ERR_OOM         = 6,
    TF_ERR_READONLY    = 7,
} TFStatus;

// ── Config de tabela ──────────────────────────────────────────────────────────
typedef struct {
    const char    *data_dir;
    const char    *chunk_interval;    // "1 DAY", "1 HOUR", "1 WEEK", "1 MONTH"
    const char    *retention_period;  // "90 DAYS", "1 YEAR", "FOREVER"
    const char    *compression;       // "ZSTD", "LZ4", "NONE"
    uint8_t        compression_level;
    const char    *hot_threshold;
    uint64_t       memtable_size_bytes;
    uint32_t       ts_column_index;
    uint32_t       column_count;
    const char   **column_names;
    const uint8_t *column_types;      // TFColumnType enum
} TFTableConfig;

// ── Tipos de coluna ───────────────────────────────────────────────────────────
typedef enum {
    TF_COL_TIMESTAMP = 0,
    TF_COL_INT64     = 1,
    TF_COL_FLOAT64   = 2,
    TF_COL_FLOAT32   = 3,
    TF_COL_BOOL      = 4,
    TF_COL_VARCHAR   = 5,
    TF_COL_TAG       = 6,   // coluna de label/tag — usada para séries
    TF_COL_DECIMAL   = 7,
} TFColumnType;

// ── Valor de uma célula ───────────────────────────────────────────────────────
typedef struct {
    TFColumnType type;
    bool         is_null;
    union {
        int64_t  ts_us;        // microssegundos Unix para TIMESTAMP
        int64_t  int_val;
        double   float_val;
        float    float32_val;
        bool     bool_val;
        struct {
            const char *ptr;
            uint32_t    len;
        } str_val;
    };
} TFValue;

// ── Linha ─────────────────────────────────────────────────────────────────────
typedef struct {
    uint32_t  col_count;
    TFValue  *values;    // array de col_count valores
} TFRow;

// ── Lifecycle da tabela ───────────────────────────────────────────────────────
TFStatus moosedb_table_create(const char *name,
                                const TFTableConfig *config);

TFStatus moosedb_table_open(const char *name,
                              const TFTableConfig *config,
                              MooseDBTable **out_table);

TFStatus moosedb_table_close(MooseDBTable *table);

TFStatus moosedb_table_drop(const char *name);

TFStatus moosedb_table_rename(const char *from, const char *to);

// ── Escrita ───────────────────────────────────────────────────────────────────
TFStatus moosedb_write_row(MooseDBTable *table, const TFRow *row);

TFStatus moosedb_flush(MooseDBTable *table);

// ── Leitura (scan completo) ───────────────────────────────────────────────────
TFStatus moosedb_scan_open(MooseDBTable *table,
                             MooseDBScan **out_scan);

TFStatus moosedb_scan_next(MooseDBScan *scan,
                             TFRow *out_row,
                             bool  *out_eof);

TFStatus moosedb_scan_close(MooseDBScan *scan);

// ── Leitura (range por timestamp) ─────────────────────────────────────────────
TFStatus moosedb_range_scan_open(MooseDBTable *table,
                                   int64_t  ts_start_us,
                                   int64_t  ts_end_us,
                                   uint32_t *tag_col_indices,
                                   TFValue  *tag_values,
                                   uint32_t  tag_count,
                                   MooseDBScan **out_scan);

// ── Manutenção ────────────────────────────────────────────────────────────────
TFStatus moosedb_compact(MooseDBTable *table,
                           int64_t ts_start_us,
                           int64_t ts_end_us);

TFStatus moosedb_apply_retention(MooseDBTable *table);

// ── Info ──────────────────────────────────────────────────────────────────────
TFStatus moosedb_table_stats(MooseDBTable *table,
                               uint64_t *out_row_count,
                               uint64_t *out_data_bytes,
                               uint64_t *out_compressed_bytes,
                               uint32_t *out_chunk_count);

// ── Memória ───────────────────────────────────────────────────────────────────
void moosedb_free_str(char *ptr);

#ifdef __cplusplus
}
#endif
```

---

## 7. Opções de tabela (TABLE OPTIONS)

```sql
-- Sintaxe completa ao criar tabela MooseDB
CREATE TABLE nome_tabela (
    ts          DATETIME(6)  NOT NULL,
    host        VARCHAR(64)  NOT NULL  COMMENT 'TAG',
    metric      VARCHAR(64)  NOT NULL  COMMENT 'TAG',
    value       DOUBLE,
    value_int   BIGINT,
    status      TINYINT(1),
    INDEX ts_idx (ts)          -- obrigatório: índice na coluna timestamp
) ENGINE=MooseDB
  CHUNK_INTERVAL    = '1 DAY'   -- agrupamento temporal dos arquivos
  RETENTION_PERIOD  = '90 DAYS' -- expiração automática (FOREVER = sem expiração)
  COMPRESSION       = 'ZSTD'    -- algoritmo de compressão dos chunks frios
  COMPRESSION_LEVEL = 3         -- nível ZSTD 1-19 (default: 3)
  HOT_THRESHOLD     = '7 DAYS'  -- chunks mais novos usam LZ4 (escrita rápida)
  MEMTABLE_SIZE     = 67108864  -- 64MB — tamanho do buffer em memória
  TIMESTAMP_COLUMN  = 'ts';     -- coluna timestamp primária (obrigatório)
```

### Valores válidos para CHUNK_INTERVAL

`'1 HOUR'`, `'6 HOUR'`, `'12 HOUR'`, `'1 DAY'`, `'1 WEEK'`, `'1 MONTH'`

### Valores válidos para RETENTION_PERIOD

`'N HOURS'`, `'N DAYS'`, `'N WEEKS'`, `'N MONTHS'`, `'N YEARS'`, `'FOREVER'`

### Colunas TAG

Colunas marcadas com `COMMENT 'TAG'` formam a identidade da série.
A engine cria um índice invertido hash(tag_values) → series_id.
Queries que filtram por TAG são otimizadas — apenas as séries correspondentes
são lidas, sem scan completo.

---

## 8. Sistema de variáveis globais

```sql
SHOW VARIABLES LIKE 'moosedb%';

+------------------------------------------+----------+
| Variable_name                            | Value    |
+------------------------------------------+----------+
| moosedb_wal_sync_mode                   | fsync    |
| moosedb_memtable_flush_threshold        | 67108864 |
| moosedb_compaction_trigger_chunks       | 10       |
| moosedb_compaction_threads              | 2        |
| moosedb_retention_check_interval        | 3600     |
| moosedb_bloom_filter_false_positive_rate| 0.01     |
| moosedb_chunk_cache_size                | 134217728|
| moosedb_max_open_chunks                 | 100      |
+------------------------------------------+----------+
```

---

## 9. INFORMATION_SCHEMA — tabelas de diagnóstico

Registrar dois plugins IS junto com a engine:

### MOOSEDB_TABLES

```sql
SELECT * FROM information_schema.MOOSEDB_TABLES;

+--------+-----------+-----------+---------------+------------------+
| SCHEMA | TABLE     | ROW_COUNT | DATA_BYTES    | COMPRESSED_BYTES |
| CHUNKS | HOT_CHUNKS| COLD_CHUNK| RETENTION     | CHUNK_INTERVAL   |
+--------+-----------+-----------+---------------+------------------+
```

### MOOSEDB_CHUNKS

```sql
SELECT * FROM information_schema.MOOSEDB_CHUNKS;

+--------+-----------+---------------------+---------------------+
| SCHEMA | TABLE     | TS_MIN              | TS_MAX              |
| ROWS   | DATA_MB   | COMPRESSED_MB       | RATIO               |
| STATUS | COMPRESSION | SEALED_AT         | CHUNK_FILE          |
+--------+-----------+---------------------+---------------------+
```

Status possíveis: `HOT`, `WARM`, `COLD`, `COMPACTING`, `EXPIRED`

---

## 10. Comandos SQL de manutenção

```sql
-- Forçar flush da MemTable para disco
FLUSH TABLES nome_tabela;

-- Compactar chunks em um range de tempo
OPTIMIZE TABLE nome_tabela;

-- Via stored procedure (range específico):
CALL moosedb_compact('nome_tabela', '2026-01-01', '2026-06-30');

-- Aplicar retenção imediatamente
CALL moosedb_apply_retention('nome_tabela');

-- Verificar integridade dos chunks (CRC32)
CHECK TABLE nome_tabela;

-- Ver estatísticas detalhadas
SELECT * FROM information_schema.MOOSEDB_CHUNKS
WHERE TABLE_NAME = 'nome_tabela'
ORDER BY TS_MIN DESC;
```

---

## 11. Comportamento de UPDATE e DELETE

A MooseDB é **append-only** por design — dados time-series são imutáveis.

```sql
UPDATE metricas SET value = 1.0 WHERE host='srv01';
-- ERROR 1031 (HY000): Table storage engine for 'metricas'
-- doesn't have this option (UPDATE not supported by MooseDB)

DELETE FROM metricas WHERE host='srv01';
-- ERROR 1031 (HY000): Table storage engine for 'metricas'
-- doesn't have this option (DELETE not supported — use RETENTION_PERIOD)
```

**Exceção:** `DELETE FROM tabela` sem WHERE é permitido (equivale a truncate).
`TRUNCATE TABLE` também é suportado.

---

## 12. Crash recovery

```
Startup da engine:
  1. Para cada tabela MooseDB:
     a. Abrir WAL mais recente
     b. Identificar último checkpoint (último flush confirmado)
     c. Replay de todas as entradas do WAL após o checkpoint
     d. Reconstruir MemTable a partir do replay
     e. Verificar CRC32 do chunk mais recente
     f. Se CRC32 falhar: mover para .tfl.corrupt, logar erro, continuar
     g. Truncar WAL até o novo checkpoint

Garantia: nenhuma linha confirmada (write_row retornou OK) é perdida.
Garantia: nenhuma linha não-confirmada aparece após recovery.
```

---

## 13. Suporte a replicação

- **SBR:** INSERT statements replicados normalmente
- **RBR:** row events gerados pelo `write_row` são replicados
- Chunks físicos NÃO são replicados — apenas os INSERTs
- Réplicas reconstroem seus próprios chunks independentemente
- **Galera (wsrep):** não suportado na v1.0 — escopo de v2.0

---

## 14. Segurança — encryption at rest

Integração com `file_key_management` ou `aws_key_management` do MariaDB:

```sql
CREATE TABLE metricas_seguras (
    ts    DATETIME(6) NOT NULL,
    value DOUBLE,
    INDEX ts_idx (ts)
) ENGINE=MooseDB
  ENCRYPTION       = 'YES'
  CHUNK_INTERVAL   = '1 DAY'
  RETENTION_PERIOD = '30 DAYS';
```

Cada chunk `.tfl` é encriptado com AES-256-CTR usando chave derivada do
key management plugin. O WAL também é encriptado.

Implementado na camada Rust usando as crates `aes` + `ctr` (pure Rust, auditadas).

---

## 15. Dependências

### C++

Nenhuma dependência externa além dos headers do MariaDB Server.

### Rust (gerenciadas via Cargo)

```toml
# moosedb-core/Cargo.toml
[dependencies]
lz4_flex    = { version = "0.11", features = ["safe-encode", "safe-decode"] }
zstd        = { version = "0.13", default-features = false }
byteorder   = "1.5"
ahash       = "0.8"
probabilistic-collections = "0.6"
crc32fast   = "1.4"
aes         = { version = "0.8", optional = true }
ctr         = { version = "0.9", optional = true }

[features]
default          = ["compression-zstd", "compression-lz4"]
encryption       = ["dep:aes", "dep:ctr"]
compression-zstd = ["dep:zstd"]
compression-lz4  = ["dep:lz4_flex"]
```

---

## 16. Build system — integração com MariaDB cmake

```cmake
# storage/moosedb/CMakeLists.txt

find_program(CARGO_EXECUTABLE   cargo   REQUIRED)
find_program(CBINDGEN_EXECUTABLE cbindgen)

# Build da biblioteca Rust
add_custom_command(
    OUTPUT  ${CMAKE_CURRENT_SOURCE_DIR}/rust/target/release/libmoosedb.a
    COMMAND ${CARGO_EXECUTABLE} build --release
            --manifest-path ${CMAKE_CURRENT_SOURCE_DIR}/rust/Cargo.toml
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/rust/moosedb-core/src/*.rs
            ${CMAKE_CURRENT_SOURCE_DIR}/rust/moosedb-ffi/src/*.rs
    COMMENT "Building MooseDB Rust core"
)

# Gerar moosedb_ffi.h via cbindgen
add_custom_command(
    OUTPUT  ${CMAKE_CURRENT_SOURCE_DIR}/moosedb_ffi.h
    COMMAND ${CBINDGEN_EXECUTABLE}
            --config ${CMAKE_CURRENT_SOURCE_DIR}/rust/cbindgen.toml
            --output ${CMAKE_CURRENT_SOURCE_DIR}/moosedb_ffi.h
            ${CMAKE_CURRENT_SOURCE_DIR}/rust/moosedb-ffi
    DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/rust/moosedb-ffi/src/lib.rs
)

MYSQL_ADD_PLUGIN(moosedb
    ha_moosedb.cc
    STORAGE_ENGINE
    DEFAULT
    LINK_LIBRARIES
        ${CMAKE_CURRENT_SOURCE_DIR}/rust/target/release/libmoosedb.a
        pthread dl m
)
```

---

## 17. Testes MTR obrigatórios

```sql
-- mysql-test/suite/moosedb/t/basic.test

CREATE TABLE t1 (
    ts     DATETIME(6) NOT NULL,
    host   VARCHAR(64) NOT NULL COMMENT 'TAG',
    value  DOUBLE,
    INDEX ts_idx (ts)
) ENGINE=MooseDB CHUNK_INTERVAL='1 HOUR' RETENTION_PERIOD='7 DAYS';

INSERT INTO t1 VALUES
    ('2026-10-01 10:00:00.000000', 'srv01', 42.5),
    ('2026-10-01 10:00:01.000000', 'srv01', 43.1),
    ('2026-10-01 10:00:02.000000', 'srv02', 11.2);

SELECT COUNT(*) FROM t1;                 -- deve retornar 3
SELECT * FROM t1 WHERE host = 'srv01';   -- deve retornar 2 linhas
SELECT * FROM t1
  WHERE ts BETWEEN '2026-10-01 10:00:00'
               AND '2026-10-01 10:00:01'; -- deve retornar 2 linhas

--error ER_ILLEGAL_HA
UPDATE t1 SET value = 0 WHERE host='srv01';

--error ER_ILLEGAL_HA
DELETE FROM t1 WHERE host='srv01';

TRUNCATE TABLE t1;
SELECT COUNT(*) FROM t1;  -- deve retornar 0

DROP TABLE t1;
```

---

## 18. Ordem de implementação sugerida

1. **Rust: `moosedb-ffi`** — stubs vazios de todos os `extern "C"` retornando `TF_OK`
2. **C++: `ha_moosedb.cc`** — handler mínimo que compila e carrega com `INSTALL SONAME`
3. **Rust: WAL** — append-only, CRC32, replay
4. **Rust: MemTable** — BTreeMap em memória, flush threshold
5. **Rust: Chunk writer** — header + dados raw sem compressão
6. **C++: `write_row`** — `moosedb_write_row` funcional end-to-end
7. **Rust: Chunk reader + scan** — leitura sequencial
8. **C++: `rnd_init` / `rnd_next` / `rnd_end`** — full table scan funcional
9. **Testes MTR básicos passando**
10. **Rust: compressão** — LZ4 quente, ZSTD frio
11. **Rust: Series Index + Bloom filter** — séries por TAG
12. **C++: `index_read_map`** — range scan por timestamp
13. **Rust: Compaction** — merge de chunks, compressão fria
14. **Rust: Retention** — TTL e expiração automática
15. **C++: IS plugins** — `MOOSEDB_TABLES` e `MOOSEDB_CHUNKS`
16. **Rust: Encryption** — AES-256-CTR com key management
17. **Crash recovery** — teste com kill -9 durante escrita
18. **Testes MTR completos** — retention, compression, crash, replication

---

## 19. Referências técnicas obrigatórias

**MariaDB Handler API:**
- `sql/handler.h` no source tree MariaDB 11.4 — classe `handler` base
- `storage/example/ha_example.cc` — handler mínimo de referência
- `storage/tokudb/ha_tokudb.cc` — engine complexa com LSM
- `storage/duckdb/` branch `11.4` de `MariaDB/server` — referência mais recente

**Algoritmos time-series:**
- Pelkonen et al. (2015) — "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
  https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
- InfluxDB TSM Tree: https://docs.influxdata.com/influxdb/v1/concepts/storage_engine/
- TDengine TSDB: https://tdengine.com/storage-engine-lsm-tree-optimization-guide/
- GreptimeDB storage: https://greptime.com/blogs/2022-12-21-storage-engine-design

**Rust FFI com C++:**
- `cbindgen` — geração de headers C: https://github.com/mozilla/cbindgen
- Crate `alopex-skulk` — TSM em Rust: https://docs.rs/alopex-skulk
- The Rustonomicon — FFI seguro: https://doc.rust-lang.org/nomicon/ffi.html

**Segurança:**
- CWE-416 (use-after-free) — mitigado por Rust ownership
- CWE-122 (heap buffer overflow) — mitigado por Rust bounds checking
- AES-256-CTR para encryption at rest — RFC 3686

---

## 20. Critérios de aceitação

- [ ] `INSTALL SONAME 'ha_moosedb.so'` funciona no MariaDB 11.4.x
- [ ] `SHOW ENGINES` lista MooseDB com suporte YES
- [ ] `CREATE TABLE ... ENGINE=MooseDB` com todas as TABLE OPTIONS
- [ ] INSERT de 1 milhão de linhas em menos de 30 segundos (NVMe)
- [ ] SELECT com range de timestamp usa index scan (não full scan)
- [ ] Compressão ZSTD reduz dados numéricos para < 15% do tamanho original
- [ ] TTL expira chunks automaticamente no intervalo configurado
- [ ] `SHOW VARIABLES LIKE 'moosedb%'` retorna todas as variáveis
- [ ] `SELECT * FROM information_schema.MOOSEDB_CHUNKS` retorna dados corretos
- [ ] UPDATE e DELETE por linha retornam HA_ERR_WRONG_COMMAND
- [ ] TRUNCATE TABLE funciona
- [ ] Crash recovery: kill -9 durante escrita → nenhuma linha confirmada perdida
- [ ] `CHECK TABLE` verifica CRC32 de todos os chunks
- [ ] Replicação row-based funciona para INSERT
- [ ] Encryption at rest: chunk `.tfl` é ilegível sem a chave
- [ ] Todos os testes MTR em `suite/moosedb` passam
- [ ] `cargo test` no workspace Rust sem falhas
- [ ] `#![forbid(unsafe_code)]` em todos os crates exceto `moosedb-ffi`
- [ ] `cargo clippy -- -D warnings` sem warnings

---

## 21. Identificação do projeto

```
Engine name:      MooseDB
Plugin name:      moosedb
Shared library:   ha_moosedb.so
File extension:   .tfl (chunks), .tfl.wal (WAL), .tfl.idx (índice de séries)
Data directory:   <datadir>/<schema>/nome_tabela/
Author:           Kevenny
License:          GPLv2
MariaDB mínimo:   11.4.0
Rust mínimo:      1.75 (edition 2021)
C++ padrão:       C++20
```

---

*Documento gerado para uso exclusivo com Claude Code.*
*Autor do projeto: Kevenny.*
*Ambiente alvo: MariaDB 11.4 LTS em Oracle Linux 9 / OCI.*
