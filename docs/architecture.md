# MooseDB — Arquitetura implementada (v0.2)

Este documento descreve o que **está implementado** e registra cada decisão que
diverge de `moosedb_engine_spec.md`, com a justificativa. A spec continua sendo
a referência de produto; este arquivo é a referência de implementação.

## Camadas

```
mysqld ──► ha_moosedb.so
             ├── ha_moosedb.cc   (C++20)  Handler API, pushdown de TAG, IS plugins, UDFs, sysvars
             └── libmoosedb.a    (Rust)   linkada estaticamente, símbolos ocultos
                   ├── moosedb-ffi   C ABI: só marshalling + catch_unwind
                   └── moosedb-core  #![forbid(unsafe_code)] — toda a lógica de storage
```

| Módulo (`moosedb-core`) | Responsabilidade |
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

* `moosedb-ffi` é o único crate com `unsafe`. Toda entrada valida ponteiros,
  roda sob `catch_unwind` (um panic vira `TF_ERR_INTERNAL`, nunca derruba o
  `mysqld`) e registra a mensagem em `moosedb_last_error()`.
* `moosedb_ffi.h` é gerado por cbindgen e **versionado**; o build roda
  `cbindgen --verify` e falha se o header estiver desatualizado.
* Os símbolos de `libmoosedb.a` (std do Rust, zstd, lz4) são ocultados com
  `--exclude-libs,libmoosedb.a`, para nunca interporem com outra biblioteca
  carregada no `mysqld`. Só essa lib: os ponteiros de serviço de
  `libmysqlservices.a` precisam continuar visíveis ao `dlsym` do servidor.

## Layout em disco

Cada tabela é um diretório `<datadir>/<schema>/<tabela>/`:

| Arquivo | Conteúdo |
|---|---|
| `MANIFEST` | lista de chunks vivos + checkpoint do WAL (`wal_seq`) + início do replay (`replay_seq`) (v2; CRC32, troca atômica) |
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
liberado** (`ChunkFile::drop`). Na recuperação:

* arquivos fora do MANIFEST (órfãos de um crash) e `.tmp` são apagados;
* chunks com header/rodapé em claro corrompidos vão para quarentena
  (`.tfl.corrupt`) e saem do MANIFEST; o chunk mais novo tem o CRC completo
  verificado;
* um chunk **listado no MANIFEST e ausente** faz a abertura falhar (nunca é
  descartado em silêncio), exceto quando existe o `.tfl.corrupt`
  correspondente — quarentena interrompida por crash, que é concluída;
* segmentos de WAL com header incompleto (0–7 bytes, zeros, parâmetros de
  cifra truncados) nunca receberam entradas e são removidos; o restante do WAL
  não coberto pelo checkpoint é reaplicado.

Se uma falha deixa memória e disco possivelmente divergentes — inclusive
**qualquer erro de fsync** (WAL, chunk, MANIFEST, diretório) — a tabela entra
em modo **read-only** até ser reaberta; um fsync nunca é "tentado de novo"
(após falha, o kernel pode ter descartado as páginas). A reabertura resolve
pela recuperação. Erros de validação (linha acima de 1 GiB no WAL) só falham
o statement.

### Atomicidade por statement (lotes)

Cada statement escreve num **lote** (`batch.rs`): as linhas ficam num buffer
privado e entram na MemTable **de uma vez**, sob o mutex, no commit — um
snapshot nunca vê parte de um statement. O handler faz commit no fim de todo
statement (`external_lock(F_UNLCK)`, `end_bulk_insert`, e `reset()`/`close()`
como rede de segurança; INSERT de uma linha sob `LOCK TABLES` usa a escrita
de linha única — atômica — seguida de sync do WAL), **inclusive
quando o statement falha**: a engine é não transacional para o servidor e o
binlog já registra as linhas inseridas; descartá-las divergiria a réplica.

Casos específicos:

* **Triggers, funções e `CALL`** (modos *prelocked*): o servidor não chama
  `start_bulk_insert`, mas a escrita continua em lote; o commit ocorre no
  `external_lock(F_UNLCK)` do statement externo ou, dentro de `CALL`/`LOCK
  TABLES`, no `reset()` ao fim da sub-instrução que usou a tabela.
* **Ler as próprias escritas**: linhas do statement corrente são invisíveis
  também para a própria sessão até o commit — um trigger ou rotina não vê as
  linhas que o statement externo acabou de inserir.
* **Réplica (RBR)**: o servidor aplica um statement em vários eventos de
  linhas; o commit é adiado até `STMT_END_F` (fim do statement, antes de a
  posição do relay log avançar) — um commit e um fsync por statement.
* **`LOCK TABLES`**: INSERT de uma linha fora de lote é gravado e sincronizado
  na hora; multi-linha commita em `end_bulk_insert`.
* Erro de commit em `reset()` é reportado ao cliente (`print_error`); em
  `close()`/destrutor (caminhos anormais) só vai para o log.

WAL v2 (segmentos v1 continuam legíveis, cada entrada = linha commitada):

| kind | corpo | significado |
|---|---|---|
| ROW | `batch_id` + linha | linha de lote ainda não commitado |
| COMMIT | `batch_id` | todas as ROW do lote passam a valer |
| ROW_COMMIT | linha | lote de uma linha |

Replay e checkpoint (granularidade de segmento):

* o flush grava `wal_seq` (até onde a MemTable foi selada) e
  `replay_seq = min(wal_seq + 1, segmento da 1ª linha de qualquer lote aberto)`;
  só segmentos `< replay_seq` são apagados — um lote aberto nunca perde linhas
  por causa de um flush de outro escritor;
* o replay lê a partir de `replay_seq`, agrupa por lote e aplica um lote só ao
  achar seu COMMIT **depois** de `wal_seq` (os anteriores já estão em chunks —
  sem duplicação); lote sem COMMIT é descartado (crash no meio do statement
  não deixa linhas);
* ids de lote nunca se repetem (`max visto + 1` na abertura); TRUNCATE
  invalida lotes abertos por época.

**Spill**: quando o buffer de um lote passa de `MEMTABLE_SIZE`, ele grava
chunks *em estágio* (protocolo `.tmp` → fsync → rename, fora do MANIFEST) e
para de logar no WAL. O commit grava o restante em estágio e faz **um** swap
de MANIFEST com todos — commit durável e atômico; sem COMMIT no WAL, as
linhas logadas antes do spill são descartadas no replay. Abort/crash: os
chunks em estágio são órfãos e são removidos. Memória por lote limitada a
`MEMTABLE_SIZE` (N statements concorrentes na mesma tabela: até N ×
`MEMTABLE_SIZE`). Um lote aberto que logou no WAL retém os segmentos desde a
sua primeira linha enquanto durar o statement; acima de 64 segmentos retidos
a engine registra um warning no log.

### Limites de dados não confiáveis

Arquivos em disco são tratados como entrada não confiável: nenhum campo lido
controla uma alocação sem teto (um OOM aborta o `mysqld` e não é capturado
por `catch_unwind`). Bloco descomprimido ≤ 256 MiB e plausível em relação ao
tamanho armazenado; série ≤ 2²⁷ linhas por chunk, soma das séries = contagem
do header; RLE e decodificação crescem sob demanda. Violação → `Corrupt`.
Os mesmos limites são aplicados na escrita (erro explícito, nunca perda).

## Concorrência

* `store_lock` faz como o InnoDB: INSERTs rodam concorrentes entre si e com
  SELECTs (`TL_WRITE_ALLOW_WRITE`); TRUNCATE/OPTIMIZE/DELETE/ALTER continuam
  exclusivos.
* Escritores acumulam linhas em lotes privados (sem mutex) e só tomam o mutex
  no commit; leitores só seguram o mutex para
  pegar um **snapshot**: lista de chunks + segmentos congelados da MemTable
  (sem cópia). O snapshot é compartilhado enquanto a tabela não muda.
* `position()` guarda o snapshot do scan; `rnd_pos()` resolve posições por
  ele. Por isso o filesort continua correto mesmo com flush/compactação
  concorrentes (testado em `concurrency.test` e `moosedb-tests`).
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

`moosedb_compaction_threads` workers + um agendador (tick de 1 s) percorrem
as tabelas **abertas**:

* retenção a cada `moosedb_retention_check_interval` s (a primeira um
  intervalo após a abertura);
* compactação quando um intervalo acumula `moosedb_compaction_trigger_chunks`
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

**Modelo de ameaça: só confidencialidade.** AES-CTR não tem MAC e os CRC32
não são autenticação — quem escreve no datadir consegue alterar bits do texto
claro sem a chave (testado). Integridade autenticada (AEAD) exigiria formato
v3. Salvaguardas atuais:

* abertura **fail-closed**: falha de decriptação do índice (chave errada ou
  índice corrompido sob cifra) recusa a abertura sem quarentena e sem
  reescrever o MANIFEST; para descartar um chunk de fato corrompido, o
  operador o renomeia para `.tfl.corrupt`;
* tabela cifrada não aceita chunk em claro, e vice-versa;
* sem chave disponível, criação, abertura e flush falham — não há fallback
  para texto claro.

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
| 1 | `struct ha_moosedb_table_option_struct` | `struct ha_table_option_struct` | O servidor tipa `TABLE_SHARE::option_struct` com esse nome exato. |
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
| 12 | fsync por `write_row` | commit do **lote** no fim do statement (`external_lock(F_UNLCK)`, `end_bulk_insert`; sob `LOCK TABLES`, cada INSERT de uma linha) com fsync conforme `wal_sync_mode` | Linha confirmada = statement retornou OK; fsync por linha inviabiliza 1M linhas em 30 s. Ver §Atomicidade por statement. |
| 13 | Entrada do WAL `[CRC][len][ts][series_id][valores]` | header de segmento + `[CRC][len][linha]` | Header guarda os parâmetros de criptografia; `series_id` é reconstruído no replay. |
| 14 | Header de 64 bytes com os campos listados | mesmos campos + `column_count`, `chunk_id`, `wal_seq`, `schema_fingerprint`; codec nas flags | Os campos listados somam 40 bytes; o resto valida schema e ajuda diagnóstico. |
| 15 | Checkpoint implícito | `MANIFEST` explícito | Um flush gera vários chunks; sem commit único, um crash duplicaria ou perderia linhas. |
| 16 | `.tfl.idx` (índice de séries) | não existe | Índice reconstruído dos chunks + WAL na abertura. |
| 17 | `ENCRYPTION='YES'` | + `ENCRYPTION_KEY_ID` (padrão 1) | Escolher a chave do key management. |
| 18 | `CALL moosedb_compact(...)` global | procedures instaladas por banco (`sql/moosedb_install.sql`) sobre UDFs do `.so` | Plugins não criam procedures; `CALL` sem prefixo só procura no banco corrente. |
| 19 | Colunas IS `SCHEMA`, `TABLE`, `COLD_CHUNK` | `TABLE_SCHEMA`, `TABLE_NAME`, `COLD_CHUNKS` + colunas extras | Convenção do INFORMATION_SCHEMA; `ROWS` é palavra reservada (use `` `ROWS` ``). |
| 20 | Deps `byteorder`, `ahash`, `probabilistic-collections` | `std`, Bloom próprio, `getrandom` | Menos dependências; nada que a std não resolva. |
| 21 | — (particionamento não mencionado) | `HTON_NO_PARTITION`: `PARTITION BY` é recusado | `ha_partition` reorganiza partições com `delete_row`, que a engine append-only não suporta: `ADD`/`COALESCE PARTITION` duplicavam e perdiam linhas. O tempo já é particionado por chunks. |
| 22 | Procedures sem modelo de privilégio | UDFs exigem `DELETE` (retenção) e `ALTER` (compactação) na tabela; tabela inexistente e sem privilégio dão o mesmo erro | MariaDB não aplica ACL a UDFs; sem a checagem, qualquer usuário podia expirar dados alheios. |
| 23 | `RETENTION_PERIOD` / `HOT_THRESHOLD` sem limite | ≤ 10 000 anos; cálculo de cutoff saturante | Períodos absurdos estouravam a aritmética e geravam cutoff no futuro (retenção apagaria tudo). |
| 24 | — (isolamento não especificado) | atomicidade por statement: linhas de um statement aparecem todas de uma vez; crash no meio não deixa nada; sem MVCC nem transações multi-statement | Leituras nunca veem statements parciais; MVCC completo não se justifica com dados imutáveis. Erro no meio do statement mantém as linhas já inseridas (coerente com o binlog de engine não transacional). |
| 25 | Entrada do WAL única | WAL v2 com `ROW`/`COMMIT`/`ROW_COMMIT`; MANIFEST v2 com `replay_seq` | Necessário para a atomicidade por statement com escritores concorrentes. v1 continua legível. |

## Limitações conhecidas

* A manutenção de background só alcança tabelas **abertas** (no table cache).
  `OPTIMIZE TABLE` e as procedures funcionam sempre.
* `DELETE FROM t` sem `WHERE` com `binlog_format=ROW` é executado linha a linha
  pelo servidor e falha (`ER_ILLEGAL_HA`); `TRUNCATE` funciona sempre.
* Pushdown de TAG só para constantes string em `=`, `IN` e igualdades
  múltiplas ligadas por `AND` (OR e TAGs numéricas são avaliados só pelo
  servidor).
* A compactação mantém um intervalo de tempo em um chunk; com
  `CHUNK_INTERVAL='1 MONTH'` e muitas séries, o chunk resultante pode ser grande.
  Uma série que não cabe nos limites de bloco é dividida em vários chunks do
  mesmo intervalo; um grupo que falha ou precisa de divisão não é reenfileirado
  pelo background até o conjunto de chunks mudar (só `OPTIMIZE` força) — isso
  inclui a recodificação quente→fria e falhas transitórias (ENOSPC/EIO), e
  um `OPTIMIZE` com parte dos grupos falhando retorna OK (falhas só no log).
* `MEMTABLE_SIZE` ≤ 96 MiB (garante blocos de flush < 256 MiB); tabelas antigas
  com valor maior abrem com o valor limitado.
* O plugin se declara `EXPERIMENTAL`: requer `plugin_maturity=experimental`.
* Criptografia sem integridade autenticada (ver §Criptografia).
* Tabelas não podem ser particionadas.
* `HANDLER ... OPEN` não é suportado (`ER_ILLEGAL_HA`); `IN` com mais de
  `in_predicate_conversion_threshold` (1000) valores vira subquery no
  servidor e não é empurrado.
