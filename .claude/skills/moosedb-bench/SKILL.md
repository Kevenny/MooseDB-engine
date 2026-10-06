---
name: moosedb-bench
description: Mede throughput de encode/decode, taxa de compressão por tipo de dado e latência de scan do MooseDB. Use quando o usuário perguntar "está lento?", pedir para comparar codecs, ou quantificar o impacto de uma mudança em compression/, scan.rs, chunk_reader.rs ou cache.rs.
---

# Benchmark de performance

## Quando usar

- "Está lento?" / "o codec X ficou mais rápido depois da mudança Y?"
- Antes/depois de uma mudança em `compression/`, `scan.rs`, `chunk_reader.rs`,
  `cache.rs`, `wal.rs` (overhead de fsync).
- Comparar taxa de compressão entre LZ4 (chunk quente) e ZSTD (chunk frio)
  para um dataset específico.

## Como executar

**Não há `cargo bench`/`criterion` configurado no workspace hoje.**
Delegue ao agent `perf-benchmark` — ele decide se mede com um harness
descartável (`#[test]` cronometrado em `--release`) ou, se o usuário pedir
algo permanente, propõe adicionar `criterion` antes de adicionar a
dependência.

## Contrato de saída

```markdown
## Benchmark — <alvo>

| Métrica | Valor | Observação |
|---|---:|---|
| Throughput Gorilla encode | 185 MB/s | buffer de 10k pontos, release |
| Taxa de compressão FLOAT64 (ZSTD-3) | 4.2x | dataset sintético senoidal |

### Achados (ranqueados por impacto)
1. **[alto, esforço baixo, risco baixo]** <achado quantificado>
```

- Sempre quantificar (MB/s, registros/s, ns/op, razão). Nunca
  "rápido"/"lento" sem número.
- Toda recomendação tem esforço e risco (baixo/médio/alto).
- Se não for possível medir, reporte "não medido" — nunca estime.

## Quem executa

Agente principal despacha; **`perf-benchmark`** mede e resume.
