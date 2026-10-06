# TideFlow — Arquitetura implementada (v0.2)

Este documento descreve o que **está implementado** e registra cada decisão que
diverge de `tideflow_engine_spec.md`, com a justificativa. A spec continua sendo
a referência de produto; este arquivo é a referência de implementação.

## Camadas

```
mysqld ──► ha_tideflow.so
             ├── ha_tideflow.cc   (C++20)  Handler API, pushdown de TAG, IS plugins, UDFs, sysvars
             └── libtideflow.a    (Rust)   linkada estaticamente, símbolos ocultos
                   ├── tideflow-ffi   C ABI: só marshalling + catch_unwind
                   └── tideflow-core  #![forbid(unsafe_code)] — toda a lógica de storage
```

| Módulo (`tideflow-core`) | Responsabilidade |
|---|---|
| `table` | estado da tabela, protocolo de durabilidade, recuperação |
| `wal` | segmentos `[CRC][len][linha]`, cifrados opcionalmente |
| `memtable` | buffer em segmentos imutáveis (snapshot O(1)) |
| `chunk`, `chunk_writer`, `chunk_reader`, `block` | formato `.tfl` v2, blocos por coluna |
| `compression/` | delta-of-delta, delta, Simple8b, Gorilla, RLE, LZ4/ZSTD |
| `scan` | snapshots, scan em streaming, k-way merge ordenado |
| `compaction` | merge de chunks por intervalo, recodificação fria |
| `maintenance` | registro de tabelas abertas + pool de threads de fundo |
| `crypto` | AES-256-CTR, provedor de chaves |
| `cache` | LRU de blocos decodificados e de descritores de arquivo |
| `inspect` | diagnóstico para o INFORMATION_SCHEMA |

* `tideflow-ffi` é o único crate com `unsafe`. Toda entrada valida ponteiros,
  roda sob `catch_unwind` (um panic vira `TF_ERR_INTERNAL`, nunca derruba o
  `mysqld`) e registra a mensagem em `tideflow_last_error()`.
* `tideflow_ffi.h` é gerado por cbindgen e **versionado**; o build roda
  `cbindgen --verify` e falha se o header estiver desatualizado.
* Os símbolos de `libtideflow.a` (std do Rust, zstd, lz4) são ocultados com
  `--exclude-libs,libtideflow.a`, para nunca interporem com outra biblioteca
  carregada no `mysqld`. Só essa lib: os ponteiros de serviço de
  `libmysqlservices.a` precisam continuar visíveis ao `dlsym` do servidor.

## Layout em disco

Cada tabela é um diretório `<datadir>/<schema>/<tabela>/`:

| Arquivo | Conteúdo |
|---|---|
| `MANIFEST` | lista de chunks vivos + checkpoint do WAL (CRC32, troca atômica) |
| `OPTIONS` | TABLE OPTIONS normalizadas (texto), lidas pelo INFORMATION_SCHEMA |
| `wal_NNNNNN.tfl.wal` | segmentos do WAL |
| `chunk_<início>_<fim>_<id>.tfl` | chunk selado (formato v2, `chunk.rs`) |
| `*.tfl.corrupt` | chunk em quarentena (CRC inválido na recuperação) |

### Formato do chunk (v2)

Header de 64 bytes (texto claro) → header de criptografia de 32 bytes (se
cifrado) → blocos de coluna → índice de séries → Bloom filter → footer de 24
bytes. Cada **bloco de coluna** tem codificação por tipo, codec (LZ4/ZSTD,
aplicado só se reduzir o tamanho) e CRC próprio; o arquivo inteiro tem um CRC
sobre os bytes armazenados (o `CHECK TABLE` funciona sem a chave).

| Tipo | Codificação |
|---|---|
| TIMESTAMP | delta-of-delta + Simple8b |
| INT64 | delta + Simple8b |
| FLOAT64 / FLOAT32 | Gorilla XOR |
| BOOL e mapa de NULLs | RLE |
| VARCHAR / DECIMAL | bytes com prefixo de tamanho (+ codec) |

Chunks **quentes** (intervalo mais novo que `HOT_THRESHOLD`) usam LZ4; os
**frios** usam o codec da tabela (`COMPRESSION`, ZSTD por padrão, nível
`COMPRESSION_LEVEL`). A compactação recodifica chunks que esfriaram.

### Protocolo de durabilidade

Toda mudança estrutural (flush, compactação, retenção, truncate) segue:

1. escrever os arquivos novos (`.tmp` → fsync → rename);
2. **trocar o MANIFEST atomicamente** — ponto de commit;
3. marcar como obsoletos os chunks que saíram do conjunto vivo.

Um chunk obsoleto só é **apagado quando o último snapshot que o referencia é
liberado** (`ChunkFile::drop`). Na recuperação, arquivos fora do MANIFEST são
apagados, chunks corrompidos vão para quarentena, o chunk mais novo tem o CRC
completo verificado e o WAL não coberto pelo checkpoint é reaplicado.

Se uma falha deixa memória e disco possivelmente divergentes, a tabela entra
em modo **read-only** até ser reaberta; a reabertura resolve pela recuperação.

## Concorrência

* `store_lock` faz como o InnoDB: INSERTs rodam concorrentes entre si e com
  SELECTs (`TL_WRITE_ALLOW_WRITE`); TRUNCATE/OPTIMIZE/DELETE/ALTER continuam
  exclusivos.
* O core serializa os appends num mutex; leitores só seguram o mutex para
  pegar um **snapshot**: lista de chunks + segmentos congelados da MemTable
  (sem cópia). O snapshot é compartilhado enquanto a tabela não muda.
* `position()` guarda o snapshot do scan; `rnd_pos()` resolve posições por
  ele. Por isso o filesort continua correto mesmo com flush/compactação
  concorrentes (testado em `concurrency.test` e `tideflow-tests`).
* Existe **uma instância por diretório** no processo (`maintenance::open_shared`).
  DROP/RENAME aposentam a instância e esperam o job de manutenção em curso.

## Leitura

* **Full scan**: chunk por chunk, série por série (blocos lidos sob demanda,
  via cache LRU), e por fim a MemTable.
* **Scan por índice** (`index_read_map`, `index_first/last`, `ORDER BY ts`):
  *k-way merge* ordenado por `(ts, origem, ordinal)`, ascendente ou
  descendente. Cada série só é decodificada quando chega ao topo do heap, então
  `ORDER BY ts DESC LIMIT n` lê apenas os dados mais novos. Se a direção muda
  no meio (`index_next` após `index_prev`), o intervalo é materializado.
* **Pushdown de TAG** (`cond_push`): igualdades, `IN` e igualdades múltiplas
  sobre colunas TAG com constantes string viram um conjunto de `series_id`,
  calculado comparando os valores de cada série **com a collation da
  comparação** (`strnncollsp`). Só essas séries são lidas; os Bloom filters
  podam chunks. O servidor continua avaliando o WHERE inteiro.

## Manutenção em background

`tideflow_compaction_threads` workers + um agendador (tick de 1 s) percorrem
as tabelas **abertas**:

* retenção a cada `tideflow_retention_check_interval` s (a primeira um
  intervalo após a abertura);
* compactação quando um intervalo acumula `tideflow_compaction_trigger_chunks`
  chunks, ou quando um chunk esfria e ainda não está no codec frio.

A compactação monta o chunk novo fora do lock (uma série por vez) e confirma
só se as entradas ainda estiverem vivas.

## Criptografia

`ENCRYPTION='YES' ENCRYPTION_KEY_ID=n` cifra chunks e WAL com AES-256-CTR. As
chaves vêm do plugin de key management do servidor (`file_key_management`,
`aws_key_management`, …) através de um callback; cada arquivo guarda id e
versão da chave e um IV aleatório, então a rotação de chaves funciona
(arquivos antigos seguem legíveis; novos usam a versão mais recente). A chave
precisa ter 256 bits. Ficam em claro: os headers (contagens, intervalo de
tempo, ids), o MANIFEST e o OPTIONS.

## Mapeamento de tipos

| MariaDB | Core | Observação |
|---|---|---|
| DATETIME | TIMESTAMP (µs) | relógio de parede tratado como UTC — fronteiras de chunk independem de `time_zone` |
| TIMESTAMP | TIMESTAMP (µs) | tempo Unix real |
| TINYINT..BIGINT, YEAR, BIT | INT64 | BIGINT UNSIGNED preservado bit a bit |
| FLOAT / DOUBLE | FLOAT32 / FLOAT64 | |
| DECIMAL | DECIMAL | string canônica (exata) |
| `COMMENT 'TAG'` | TAG | identidade da série |
| demais (VARCHAR, TEXT, BLOB, DATE, TIME, ENUM, SET, JSON…) | VARCHAR | via `val_str`/`store` |
| GEOMETRY | — | rejeitado no CREATE |

## Divergências da spec (e por quê)

| # | Spec | Implementado | Motivo |
|---|---|---|---|
| 1 | `struct ha_tideflow_table_option_struct` | `struct ha_table_option_struct` | O servidor tipa `TABLE_SHARE::option_struct` com esse nome exato. |
| 2 | `records_in_range(inx, min, max)` | `+ page_range *` | Assinatura do 11.4. |
| 3 | `compression_level` como `uint` | `ulonglong` | `HA_TOPTION_NUMBER` exige `ulonglong`. |
| 4 | `MYSQL_ADD_PLUGIN(... DEFAULT)` | `MODULE_ONLY` | `DEFAULT` compila a engine *dentro* do servidor; a spec exige `INSTALL SONAME`. |
| 5 | Glob em `DEPENDS` do `add_custom_command` | `FILE(GLOB_RECURSE … CONFIGURE_DEPENDS)` | CMake não expande globs em `DEPENDS`. |
| 6 | TINYINT(1) → BOOL | TINYINT → INT64 | `TINYINT(1)` aceita -128..127; a codificação delta comprime igualmente. |
| 7 | `max_supported_key_parts = 4` | `1`, só índice no timestamp, não-UNIQUE | O otimizador remove condições usadas em `ref`; um índice composto exigiria igualdade exata em todas as partes. TAGs são filtradas por pushdown. |
| 8 | `HA_CAN_INDEX_BLOBS` | removido; `+HA_BINLOG_*_CAPABLE`, `HA_NO_AUTO_INCREMENT`, `HA_READ_ORDER`, `HA_CAN_TABLE_CONDITION_PUSHDOWN` | Binlog (§13), `ORDER BY ts DESC` pelo índice, pushdown de TAG. |
| 9 | TFValue com `bool`/enum/união anônima | `uint8_t bool_val`, `uint8_t kind`, união `data` | Ler `bool`/enum inválido vindo do C é UB no Rust; cbindgen não gera união anônima. |
| 10 | — | `TF_ERR_INTERNAL`, `TF_ERR_UNSUPPORTED` | Panic capturado / recurso indisponível. |
| 11 | API FFI mínima | + sync do WAL, truncate, scans filtrados/bidirecionais, snapshots, séries, check, estimativa, inspeção, manutenção, chaves, settings | Necessárias para a Handler API completa e os recursos acima. |
| 12 | fsync por `write_row` | fsync no **fim do statement** (`external_lock(F_UNLCK)`, `end_bulk_insert`; sob `LOCK TABLES`, a cada INSERT de uma linha) | Linha confirmada = statement retornou OK; fsync por linha inviabiliza 1M linhas em 30 s. |
| 13 | Entrada do WAL `[CRC][len][ts][series_id][valores]` | header de segmento + `[CRC][len][linha]` | Header guarda os parâmetros de criptografia; `series_id` é reconstruído no replay. |
| 14 | Header de 64 bytes com os campos listados | mesmos campos + `column_count`, `chunk_id`, `wal_seq`, `schema_fingerprint`; codec nas flags | Os campos listados somam 40 bytes; o resto valida schema e ajuda diagnóstico. |
| 15 | Checkpoint implícito | `MANIFEST` explícito | Um flush gera vários chunks; sem commit único, um crash duplicaria ou perderia linhas. |
| 16 | `.tfl.idx` (índice de séries) | não existe | Índice reconstruído dos chunks + WAL na abertura. |
| 17 | `ENCRYPTION='YES'` | + `ENCRYPTION_KEY_ID` (padrão 1) | Escolher a chave do key management. |
| 18 | `CALL tideflow_compact(...)` global | procedures instaladas por banco (`sql/tideflow_install.sql`) sobre UDFs do `.so` | Plugins não criam procedures; `CALL` sem prefixo só procura no banco corrente. |
| 19 | Colunas IS `SCHEMA`, `TABLE`, `COLD_CHUNK` | `TABLE_SCHEMA`, `TABLE_NAME`, `COLD_CHUNKS` + colunas extras | Convenção do INFORMATION_SCHEMA; `ROWS` é palavra reservada (use `` `ROWS` ``). |
| 20 | Deps `byteorder`, `ahash`, `probabilistic-collections` | `std`, Bloom próprio, `getrandom` | Menos dependências; nada que a std não resolva. |

## Limitações conhecidas

* A manutenção de background só alcança tabelas **abertas** (no table cache).
  `OPTIMIZE TABLE` e as procedures funcionam sempre.
* `DELETE FROM t` sem `WHERE` com `binlog_format=ROW` é executado linha a linha
  pelo servidor e falha (`ER_ILLEGAL_HA`); `TRUNCATE` funciona sempre.
* Pushdown de TAG só para constantes string em `=`, `IN` e igualdades
  múltiplas ligadas por `AND` (OR e TAGs numéricas são avaliados só pelo
  servidor).
* A compactação mantém um intervalo de tempo inteiro em um chunk; com
  `CHUNK_INTERVAL='1 MONTH'` e muitas séries, o chunk resultante pode ser grande.
* O plugin se declara `EXPERIMENTAL`: requer `plugin_maturity=experimental`.
