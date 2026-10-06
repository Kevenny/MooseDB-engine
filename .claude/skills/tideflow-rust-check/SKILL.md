---
name: tideflow-rust-check
description: Roda cargo test --workspace e cargo clippy --all-targets -D warnings para o núcleo Rust do TideFlow dentro do container tideflow-dev. Use antes de qualquer commit que toque storage/tideflow/rust/**.
---

# Checagem Rust — test + clippy

## Quando usar

- Antes de comitar qualquer mudança em `storage/tideflow/rust/**`.
- Depois que o agent `rust-core-engineer` termina uma implementação.
- Quando o usuário pedir "roda os testes Rust" / "roda o clippy".

## Como executar

```bash
docker run --rm -v "$PWD:/work" -v tideflow-cargo:/usr/local/cargo/registry \
  -v tideflow-build:/build -w /work/storage/tideflow/rust tideflow-dev \
  bash -c 'cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings'
```

Clippy roda com `-D warnings` — qualquer warning é tratado como erro,
coerente com o padrão do projeto.

Se a mudança tocou `tideflow-ffi/src/lib.rs` (assinatura de função
`extern "C"`), verifique também o header gerado:

```bash
docker run --rm -v "$PWD:/work" -w /work/storage/tideflow tideflow-dev \
  cbindgen --verify --config rust/cbindgen.toml --output tideflow_ffi.h rust/tideflow-ffi
```

## Contrato de saída

**Delegue execução e leitura ao agent `tideflow-build-runner`.** Ele devolve:

```json
{
  "cargo_test": {"total": 0, "passed": 0, "failed": 0, "failed_tests": []},
  "clippy": {"warnings": 0, "detalhes": []},
  "cbindgen_header": "ok|desatualizado|não verificado"
}
```

Se `failed_tests` ou `clippy.detalhes` não estiver vazio, **não considere a
tarefa concluída** — corrija antes de seguir para build/MTR.

## Quem executa

Agente principal (ou `rust-core-engineer` ao terminar uma mudança) despacha;
**`tideflow-build-runner`** executa e lê.
