# TideFlow — Guia do usuário

TideFlow é uma storage engine time-series para MariaDB 11.4+: dados
append-only, agrupados em arquivos por intervalo de tempo, comprimidos por
tipo de coluna e expirados automaticamente.

## Instalação

```ini
# my.cnf — v0.x se declara EXPERIMENTAL
[mariadb]
plugin_maturity = experimental
plugin_load_add = ha_tideflow
```

ou, com `plugin_maturity = experimental` já no my.cnf (a variável é somente
leitura), em tempo de execução:

```sql
INSTALL SONAME 'ha_tideflow';
SHOW ENGINES;                                   -- TideFlow | YES
```

Para `CALL tideflow_compact(...)` e `CALL tideflow_apply_retention(...)`,
instale as procedures em cada banco que tiver tabelas TideFlow:

```sql
USE metricas;
SOURCE /caminho/para/storage/tideflow/sql/tideflow_install.sql;
```

## Criando uma tabela

```sql
CREATE TABLE metricas (
    ts        DATETIME(6) NOT NULL,
    host      VARCHAR(64) NOT NULL COMMENT 'TAG',
    metric    VARCHAR(64) NOT NULL COMMENT 'TAG',
    value     DOUBLE,
    value_int BIGINT,
    INDEX ts_idx (ts)
) ENGINE=TideFlow
  CHUNK_INTERVAL    = '1 DAY'
  RETENTION_PERIOD  = '90 DAYS'
  COMPRESSION       = 'ZSTD'
  COMPRESSION_LEVEL = 3
  HOT_THRESHOLD     = '7 DAYS';
```

Regras:

* uma coluna `DATETIME` ou `TIMESTAMP` **NOT NULL** é o eixo do tempo, com um
  índice simples (`INDEX (ts)`); outros índices, `UNIQUE`/`PRIMARY KEY` e
  `AUTO_INCREMENT` não são aceitos;
* colunas com `COMMENT 'TAG'` formam a identidade da **série** (host, sensor,
  métrica…). Filtros `=`/`IN` sobre TAGs leem só as séries correspondentes;
* `DATETIME` é armazenado como relógio de parede (UTC); `TIMESTAMP`, como
  instante Unix.

| Opção | Valores | Padrão |
|---|---|---|
| `CHUNK_INTERVAL` | `1 HOUR`, `6 HOUR`, `12 HOUR`, `1 DAY`, `1 WEEK`, `1 MONTH` | `1 DAY` |
| `RETENTION_PERIOD` | `N HOURS/DAYS/WEEKS/MONTHS/YEARS`, `FOREVER` | `FOREVER` |
| `COMPRESSION` | `ZSTD`, `LZ4`, `NONE` (codec dos chunks frios) | `ZSTD` |
| `COMPRESSION_LEVEL` | 1–19 | 3 |
| `HOT_THRESHOLD` | período; mais novos = LZ4 | `7 DAYS` |
| `MEMTABLE_SIZE` | bytes (mín. 4096) | `tideflow_memtable_flush_threshold` |
| `TIMESTAMP_COLUMN` | nome da coluna | coluna do índice temporal |
| `ENCRYPTION` | `YES`/`NO` | `NO` |
| `ENCRYPTION_KEY_ID` | id no key management | 1 |

## Escrevendo e lendo

```sql
INSERT INTO metricas VALUES (NOW(6), 'srv01', 'cpu', 42.5, NULL);

SELECT host, AVG(value) FROM metricas
 WHERE metric = 'cpu' AND ts >= NOW() - INTERVAL 1 HOUR
 GROUP BY host;

SELECT * FROM metricas WHERE host = 'srv01' ORDER BY ts DESC LIMIT 10;
```

* `UPDATE` e `DELETE ... WHERE` retornam `ER_ILLEGAL_HA`: os dados são
  imutáveis. Use `RETENTION_PERIOD` para expirar e `TRUNCATE TABLE` (ou
  `DELETE` sem `WHERE`) para esvaziar.
* Uma linha está durável quando o statement retorna OK
  (`tideflow_wal_sync_mode=fsync`). Com `write`, sobrevive a um crash do
  `mysqld`, mas não a uma queda do sistema operacional.

## Manutenção

```sql
FLUSH TABLES metricas;     -- sela a MemTable em chunks
OPTIMIZE TABLE metricas;   -- flush + retenção + compactação de tudo
CALL tideflow_compact('metricas', '2026-01-01', '2026-06-30');
CALL tideflow_apply_retention('metricas');
CHECK TABLE metricas;      -- verifica o CRC32 de todos os chunks
```

Em background, threads de manutenção aplicam a retenção e compactam os
intervalos de tempo que acumulam chunks, em tabelas abertas.

## Diagnóstico

Requer o privilégio `PROCESS`.

```sql
SELECT * FROM information_schema.TIDEFLOW_TABLES;

SELECT TS_MIN, TS_MAX, `ROWS`, SERIES, DATA_MB, COMPRESSED_MB, RATIO,
       STATUS, COMPRESSION, CHUNK_FILE
  FROM information_schema.TIDEFLOW_CHUNKS
 WHERE TABLE_NAME = 'metricas' ORDER BY TS_MIN DESC;
```

`STATUS`: `HOT` (recente, LZ4), `WARM` (esfriou, aguardando recodificação),
`COLD` (codec final), `COMPACTING`, `EXPIRED` (aguardando a próxima varredura).

## Criptografia

```ini
[mariadb]
plugin_load_add = file_key_management
file_key_management_filename = /etc/mysql/keys.txt   # chaves de 256 bits
```

```sql
CREATE TABLE segura (ts DATETIME(6) NOT NULL, v DOUBLE, INDEX(ts))
  ENGINE=TideFlow ENCRYPTION='YES' ENCRYPTION_KEY_ID=1;
```

Chunks e WAL são cifrados com AES-256-CTR. Rotacionar a chave no key
management é transparente: dados novos usam a versão mais recente e os
antigos continuam legíveis.

## Variáveis globais

| Variável | Padrão | Efeito |
|---|---|---|
| `tideflow_wal_sync_mode` | `fsync` | `fsync` ou `write` no fim do statement |
| `tideflow_memtable_flush_threshold` | 64 MiB | MemTable padrão por tabela |
| `tideflow_compaction_trigger_chunks` | 10 | chunks por intervalo que disparam compactação |
| `tideflow_compaction_threads` | 2 | threads de manutenção (somente leitura) |
| `tideflow_retention_check_interval` | 3600 | segundos entre varreduras de retenção (0 = desliga) |
| `tideflow_bloom_filter_false_positive_rate` | 0.01 | Bloom filter dos chunks novos |
| `tideflow_chunk_cache_size` | 128 MiB | cache de blocos decodificados |
| `tideflow_max_open_chunks` | 100 | descritores de chunk mantidos abertos |

## Replicação

Replicação por linha (RBR) e por statement funcionam para INSERT e TRUNCATE;
cada réplica constrói os próprios chunks. Galera não é suportado.
