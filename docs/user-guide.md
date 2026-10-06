# MooseDB — Guia do usuário

MooseDB é uma storage engine time-series para MariaDB 11.4+: dados
append-only, agrupados em arquivos por intervalo de tempo, comprimidos por
tipo de coluna e expirados automaticamente.

## Instalação

```ini
# my.cnf — v0.x se declara EXPERIMENTAL
[mariadb]
plugin_maturity = experimental
plugin_load_add = ha_moosedb
```

ou, com `plugin_maturity = experimental` já no my.cnf (a variável é somente
leitura), em tempo de execução:

```sql
INSTALL SONAME 'ha_moosedb';
SHOW ENGINES;                                   -- MooseDB | YES
```

Para `CALL moosedb_compact(...)` e `CALL moosedb_apply_retention(...)`,
instale as procedures em cada banco que tiver tabelas MooseDB:

```sql
USE metricas;
SOURCE /caminho/para/storage/moosedb/sql/moosedb_install.sql;
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
) ENGINE=MooseDB
  CHUNK_INTERVAL    = '1 DAY'
  RETENTION_PERIOD  = '90 DAYS'
  COMPRESSION       = 'ZSTD'
  COMPRESSION_LEVEL = 3
  HOT_THRESHOLD     = '7 DAYS';
```

Regras:

* uma coluna `DATETIME` ou `TIMESTAMP` **NOT NULL** é o eixo do tempo, com um
  índice simples (`INDEX (ts)`); outros índices, `UNIQUE`/`PRIMARY KEY` e
  `AUTO_INCREMENT` não são aceitos, nem `PARTITION BY` (o tempo já é
  particionado em chunks);
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
| `MEMTABLE_SIZE` | bytes (4096 – 96 MiB) | `moosedb_memtable_flush_threshold` |
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
* Cada statement é **atômico para leitura e para crash**: outras sessões veem
  todas as linhas de um INSERT de uma vez, e um crash no meio dele não deixa
  nada. Não há transações (`BEGIN`/`ROLLBACK` não desfazem) e, se o statement
  falhar no meio, as linhas inseridas antes do erro permanecem (como no MyISAM).
* Uma linha está durável quando o statement retorna OK
  (`moosedb_wal_sync_mode=fsync`). Com `write`, sobrevive a um crash do
  `mysqld`, mas não a uma queda do sistema operacional.

## Manutenção

```sql
FLUSH TABLES metricas;     -- sela a MemTable em chunks
OPTIMIZE TABLE metricas;   -- flush + retenção + compactação de tudo
CALL moosedb_compact('metricas', '2026-01-01', '2026-06-30');
CALL moosedb_apply_retention('metricas');
CHECK TABLE metricas;      -- verifica o CRC32 de todos os chunks
```

Privilégios: `moosedb_apply_retention` exige `DELETE` na tabela e
`moosedb_compact`, `ALTER`. As procedures funcionam com qualquer charset de
cliente (instale o script com o cliente que preferir).

Em background, threads de manutenção aplicam a retenção e compactam os
intervalos de tempo que acumulam chunks, em tabelas abertas.

## Diagnóstico

Requer o privilégio `PROCESS`.

```sql
SELECT * FROM information_schema.MOOSEDB_TABLES;

SELECT TS_MIN, TS_MAX, `ROWS`, SERIES, DATA_MB, COMPRESSED_MB, RATIO,
       STATUS, COMPRESSION, CHUNK_FILE
  FROM information_schema.MOOSEDB_CHUNKS
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
  ENGINE=MooseDB ENCRYPTION='YES' ENCRYPTION_KEY_ID=1;
```

Chunks e WAL são cifrados com AES-256-CTR. Rotacionar a chave no key
management é transparente: dados novos usam a versão mais recente e os
antigos continuam legíveis.

A criptografia protege a **confidencialidade**, não a integridade: quem pode
escrever no datadir consegue adulterar dados cifrados sem ser detectado. Se a
chave configurada não decifra a tabela, ela simplesmente não abre — nada é
descartado; corrija a configuração de chaves e reabra.

## Variáveis globais

| Variável | Padrão | Efeito |
|---|---|---|
| `moosedb_wal_sync_mode` | `fsync` | `fsync` ou `write` no fim do statement |
| `moosedb_memtable_flush_threshold` | 64 MiB | MemTable padrão por tabela (máx. 96 MiB) |
| `moosedb_compaction_trigger_chunks` | 10 | chunks por intervalo que disparam compactação |
| `moosedb_compaction_threads` | 2 | threads de manutenção (somente leitura) |
| `moosedb_retention_check_interval` | 3600 | segundos entre varreduras de retenção (0 = desliga) |
| `moosedb_bloom_filter_false_positive_rate` | 0.01 | Bloom filter dos chunks novos |
| `moosedb_chunk_cache_size` | 128 MiB | cache de blocos decodificados |
| `moosedb_max_open_chunks` | 100 | descritores de chunk mantidos abertos |

## Replicação

Replicação por linha (RBR) e por statement funcionam para INSERT e TRUNCATE;
cada réplica constrói os próprios chunks. Galera não é suportado.
