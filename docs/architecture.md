# TideFlow — Arquitetura implementada

Este documento descreve o que **está implementado** e registra cada decisão que
diverge de `tideflow_engine_spec.md`, com a justificativa. A spec continua sendo
a referência de produto; este arquivo é a referência de implementação.

## Camadas

```
mysqld ──► ha_tideflow.so
             ├── ha_tideflow.cc   (C++20)  Handler API, conversão de registros, sysvars
             └── libtideflow.a    (Rust)   linkada estaticamente
                   ├── tideflow-ffi   C ABI: só marshalling + catch_unwind
                   └── tideflow-core  #![forbid(unsafe_code)] — toda a lógica de storage
```

* `tideflow-ffi` é o único crate com `unsafe`. Toda entrada valida ponteiros,
  roda sob `catch_unwind` (panic vira `TF_ERR_INTERNAL`, nunca derruba o
  `mysqld`) e registra a mensagem em `tideflow_last_error()`.
* `tideflow_ffi.h` é gerado por cbindgen e **versionado**; o build do plugin
  roda `cbindgen --verify` e falha se o header estiver desatualizado.

## Layout em disco

Cada tabela é um diretório `<datadir>/<schema>/<tabela>/`:

| Arquivo | Conteúdo |
|---|---|
| `MANIFEST` | lista de chunks vivos + checkpoint do WAL (CRC32, troca atômica) |
| `wal_NNNNNN.tfl.wal` | segmentos do WAL: `[crc32][len][linha]` |
| `chunk_<início>_<fim>_<id>.tfl` | chunk selado (formato em `chunk.rs`) |
| `*.tfl.corrupt` | chunk em quarentena (CRC inválido na recuperação) |

### Protocolo de durabilidade

Toda mudança estrutural (flush, truncate, retenção) segue:

1. escrever os arquivos novos (`.tmp` → fsync → rename);
2. **trocar o MANIFEST atomicamente** — ponto de commit;
3. apagar o que o novo MANIFEST não referencia.

Na abertura (recuperação): arquivos não listados no MANIFEST são apagados,
chunks corrompidos vão para quarentena, o chunk mais novo tem o CRC completo
verificado, segmentos de WAL cobertos pelo checkpoint são removidos e os
demais são reaplicados na MemTable (cauda rasgada do último segmento é
truncada). Testado em `tideflow-tests` (crash simulado) e em
`crash_recovery.test` (kill -9 real do `mysqld`).

Se uma falha deixa memória e disco possivelmente divergentes (ex.: erro ao
gravar o MANIFEST), a tabela entra em modo **read-only** até ser reaberta; a
reabertura resolve a ambiguidade pela recuperação.

## Mapeamento de tipos

| MariaDB | Core | Observação |
|---|---|---|
| DATETIME | TIMESTAMP (µs) | relógio de parede tratado como UTC — fronteiras de chunk independem de `time_zone` |
| TIMESTAMP | TIMESTAMP (µs) | tempo Unix real |
| TINYINT..BIGINT, YEAR, BIT | INT64 | BIGINT UNSIGNED preservado bit a bit |
| FLOAT / DOUBLE | FLOAT32 / FLOAT64 | |
| DECIMAL | DECIMAL | string canônica (exata) |
| `COMMENT 'TAG'` | TAG | identidade da série, armazenada uma vez por série por chunk |
| demais (VARCHAR, TEXT, BLOB, DATE, TIME, ENUM, SET, JSON…) | VARCHAR | via `val_str`/`store` |
| GEOMETRY | — | rejeitado no CREATE |

## Divergências da spec (e por quê)

| # | Spec | Implementado | Motivo |
|---|---|---|---|
| 1 | `struct ha_tideflow_table_option_struct` | `struct ha_table_option_struct` | O servidor tipa `TABLE_SHARE::option_struct` com esse nome exato. |
| 2 | `records_in_range(inx, min, max)` | `+ page_range *` | Assinatura do 11.4. |
| 3 | `compression_level` como `uint` | `ulonglong` | `HA_TOPTION_NUMBER` exige `ulonglong`. |
| 4 | `MYSQL_ADD_PLUGIN(... DEFAULT)` | `MODULE_ONLY` | `DEFAULT` compila a engine *dentro* do servidor; a spec exige `INSTALL SONAME 'ha_tideflow.so'`. |
| 5 | Glob em `DEPENDS` do `add_custom_command` | `FILE(GLOB_RECURSE … CONFIGURE_DEPENDS)` + target `tideflow_rust` | CMake não expande globs em `DEPENDS`; o build Rust nunca seria refeito. |
| 6 | TINYINT(1) → BOOL | TINYINT → INT64 | `TINYINT(1)` aceita -128..127; mapear para bool perderia dados. A compressão (RLE/delta) recupera o custo. |
| 7 | `max_supported_key_parts = 4` | `1`, só índice no timestamp, não-UNIQUE | Índice composto exigiria que a engine garantisse igualdade em todas as partes (o otimizador remove condições usadas em `ref`). TAGs são filtradas por séries, não por índice SQL. UNIQUE é rejeitado: a engine não garante unicidade. |
| 8 | `HA_CAN_INDEX_BLOBS` | removido; `+HA_BINLOG_ROW_CAPABLE \| HA_BINLOG_STMT_CAPABLE \| HA_NO_AUTO_INCREMENT \| HA_READ_ORDER` | Sem as flags de binlog, INSERT falha com binlog ativo (spec §13 exige replicação). `HA_READ_ORDER` é obrigatória para o servidor usar `index_prev`/`ORDER BY ts DESC` com o índice. |
| 9 | TFValue com `bool bool_val`, `TFColumnType type`, união anônima | `uint8_t bool_val`, `uint8_t kind`, união nomeada `data` | Ler um `bool`/enum Rust com valor inválido vindo do C é UB. cbindgen não gera união anônima. |
| 10 | — | `TF_ERR_INTERNAL`, `TF_ERR_UNSUPPORTED` | Panic capturado / operação declarada mas não implementada. |
| 11 | — | `tideflow_sync_wal`, `tideflow_truncate`, `tideflow_scan_prev/seek_end/position/fetch`, `tideflow_check`, `tideflow_estimate_rows`, `tideflow_last_error` | Necessárias para `external_lock`, TRUNCATE, `index_prev/last`, `position/rnd_pos`, CHECK TABLE, `records_in_range` e mensagens de erro úteis. |
| 12 | WAL confirmado por linha (`fsync` a cada `write_row`) | `fsync` no **fim do statement** (`external_lock(F_UNLCK)` / `end_bulk_insert`) | Linha "confirmada" = statement retornou OK. fsync por linha inviabiliza o critério de 1M linhas em 30 s. `tideflow_wal_sync_mode=write` troca fsync por write (sobrevive a crash do mysqld, não do SO). |
| 13 | Entrada do WAL `[CRC][len][ts][series_id][valores]` | `[CRC][len][linha completa]` | `series_id` é reatribuído deterministicamente no replay; gravar as tags evita um índice de séries persistente separado. |
| 14 | Header de 64 bytes com campos listados | Mesmos campos + `column_count`, `chunk_id`, `wal_seq`, `schema_fingerprint`; CRC no offset 60 | Os campos listados somam 40 bytes; o resto foi usado para diagnóstico e validação de schema. |
| 15 | Checkpoint implícito | `MANIFEST` explícito | Um flush gera vários chunks (um por intervalo) — sem um ponto de commit único, um crash no meio duplicaria ou perderia linhas. |
| 16 | Status `HOT/WARM/...` por chunk | — (fase 15) | IS plugins ainda não implementados. |
| 17 | `RETENTION` por thread de fundo | Varredura preguiçosa no fim de statements (respeitando `tideflow_retention_check_interval`) e em `OPTIMIZE TABLE` | Evita thread própria nesta fase; mesmo efeito observável para tabelas em uso. |
| 18 | `.tfl.idx` (índice de séries) | não existe | Índice reconstruído dos chunks + WAL na abertura (determinístico). |

## Limitações conhecidas (v0.1)

* **Sem compressão ainda** (blocos `PLAIN`); o formato já reserva bytes de
  codificação/compressão por bloco (fase 10).
* Scans por índice **materializam** as linhas do intervalo para ordená-las
  (merge em streaming é uma otimização futura). `MIN(ts)`/`MAX(ts)` sem `WHERE`
  materializam a tabela inteira.
* Locks de tabela padrão: escritas são exclusivas. Leituras concorrentes com
  escrita exigem tornar posições da MemTable estáveis através de flushes.
* Sob `LOCK TABLES`, o fsync do WAL acontece no `UNLOCK TABLES`.
* `DELETE FROM t` sem `WHERE` com `binlog_format=ROW` é executado linha a linha
  pelo servidor e falha (`ER_ILLEGAL_HA`); `TRUNCATE` funciona sempre.
* Filtros por TAG ainda não são empurrados do SQL para o core (a infraestrutura
  — `ScanFilter`, Bloom por chunk — existe e é testada); o servidor filtra.
* Compactação, IS plugins, encryption e procedures `CALL tideflow_*` não
  existem ainda.
