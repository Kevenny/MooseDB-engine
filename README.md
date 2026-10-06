# TideFlow Engine

Storage engine time-series para MariaDB 11.4+ — handler em C++20, núcleo de
storage em Rust (`#![forbid(unsafe_code)]`) exposto via C ABI.

* Especificação de produto: [tideflow_engine_spec.md](tideflow_engine_spec.md)
* Arquitetura implementada e divergências da spec: [docs/architecture.md](docs/architecture.md)

## Estado atual (v0.1)

Fases 1–9 e 11–12 da spec (§18): WAL com CRC32 e replay, MemTable, chunks
selados por intervalo de tempo, MANIFEST atômico, recuperação de crash,
Bloom filter por chunk, scans completos e por intervalo de timestamp
(`index_read_map`, `ORDER BY ts DESC`), `TRUNCATE`, `CHECK TABLE`, retenção.

Ainda não: compressão (LZ4/ZSTD), compactação, IS plugins, encryption.

## Build e testes (Docker)

Todo o toolchain roda em containers; o plugin é compilado contra o source
exato do servidor da imagem `mariadb:11.4` (11.4.13).

```bash
# Imagens
docker build -f docker/dev.Dockerfile  -t tideflow-dev  docker/
docker build -f docker/test.Dockerfile -t tideflow-test docker/

# Rust: testes, clippy, header C
docker run --rm -v "$PWD:/work" -v tideflow-cargo:/usr/local/cargo/registry \
  -v tideflow-build:/build -w /work/storage/tideflow/rust tideflow-dev \
  bash -c 'cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings'

# Plugin ha_tideflow.so → build/
docker run --rm -v "$PWD:/work" -v tideflow-cargo:/usr/local/cargo/registry \
  -v tideflow-build:/build tideflow-dev bash /work/docker/build-plugin.sh

# Suíte MTR contra o servidor oficial
docker run --rm -v "$PWD:/work" tideflow-test bash /work/docker/run-mtr.sh
```

## Instalação

```sql
-- A engine se declara EXPERIMENTAL enquanto estiver em v0.x:
--   plugin_maturity = experimental   (my.cnf)
INSTALL SONAME 'ha_tideflow';
```
