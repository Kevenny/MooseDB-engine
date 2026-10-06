# TideFlow Engine — Contexto Operacional (Claude Code)

> Harness de agents/skills para trabalhar no TideFlow Engine: storage engine
> time-series para MariaDB 11.4+, handler em **C++20**, núcleo de storage em
> **Rust** (`#![forbid(unsafe_code)]`) exposto via **C ABI**.

## Fontes de verdade

| Arquivo | Conteúdo |
|---|---|
| `tideflow_engine_spec.md` | Especificação de produto. Não editar levianamente. |
| `docs/architecture.md` | O que está **implementado de fato** + tabela de divergências da spec (com justificativa). Em caso de conflito com a spec, este arquivo vence. |
| `docs/user-guide.md` | Guia do usuário final (SQL, opções de tabela, procedures). |

Leia `docs/architecture.md` antes de tocar em qualquer módulo — ele tem o mapa
de camadas, o layout em disco, o protocolo de durabilidade e a tabela de
divergências. Não repita conteúdo dele aqui; ele é a referência.

## Arquitetura (resumo)

```
mysqld ──► ha_tideflow.so (C++20, Handler API)
             └── libtideflow.a (Rust, linkada estaticamente)
                   ├── tideflow-ffi   único crate com `unsafe` — só marshalling + catch_unwind
                   └── tideflow-core  #![forbid(unsafe_code)] — toda a lógica de storage
```

## Princípio inegociável — "delegate large reads/runs"

**O agente principal nunca lê diretamente** saída bruta de:

- `docker build` / `cmake --build` (configuração do servidor MariaDB é verbosa)
- `cargo test --workspace` / `cargo clippy --all-targets`
- a suíte MTR (`run-mtr.sh`, 14 testes, diffs `.reject` podem ser longos)
- benchmarks (encode/decode, compressão, scan)
- testes de crash-recovery (`kill -9` + replay de WAL)

Para qualquer um desses, **delegue ao agent `tideflow-build-runner`** (builds e
testes funcionais) ou **`perf-benchmark`** (medições de performance). Eles
rodam no contexto isolado deles e devolvem só um resumo estruturado
(pass/fail, erros, métricas). Isso protege a janela de contexto do agente
principal — essencial porque logs de build do MariaDB (`cmake_release`) e da
suíte MTR facilmente passam de milhares de linhas.

**Exceção**: ler um único arquivo fonte pequeno (`.rs`, `.cc`, `.h`) para
editar é direto, sem delegação.

## Mapeamento sintoma/pedido → quem executa

| Pedido do usuário | Skill | Agent |
|---|---|---|
| "builda o plugin" / "compila" | `tideflow-build` | `tideflow-build-runner` |
| "roda os testes Rust" / "roda o clippy" | `tideflow-rust-check` | `tideflow-build-runner` |
| "roda a suíte MTR" | `tideflow-mtr` | `tideflow-build-runner` |
| "testa crash recovery" / "kill -9" | `tideflow-crash-recovery` | `tideflow-build-runner` |
| Mudar `tideflow-core` / `tideflow-ffi` | — | `rust-core-engineer` |
| Mudar `ha_tideflow.cc/.h`, `CMakeLists.txt` | — | `cpp-handler-engineer` |
| "está lento" / comparar codecs / taxa de compressão / latência de scan | `tideflow-bench` | `perf-benchmark` |
| Antes de comitar mudança em core/handler | `tideflow-safety-review` | `storage-code-reviewer` |
| Código divergiu da spec | `tideflow-doc-sync` | agente principal |

## Regras de segurança obrigatórias (não negociáveis)

### Rust

- `#![forbid(unsafe_code)]` em **todos** os crates exceto `tideflow-ffi`.
- `tideflow-ffi` contém **apenas** `extern "C"` de entrada: valida ponteiros,
  roda sob `catch_unwind`, registra erro em `tideflow_last_error()`. **Zero**
  lógica de negócio ali — isso vive em `tideflow-core`.
- `panic = "unwind"` no profile release é deliberado: um panic no core deve
  virar `TF_ERR_INTERNAL` na fronteira FFI, nunca derrubar o `mysqld`. Nunca
  trocar para `panic = "abort"`.
- `tideflow_ffi.h` é gerado por `cbindgen` e **versionado no git**. Nunca
  editar manualmente — regenerar e `cbindgen --verify`.

### C++

- Proibido ponteiro bruto (`new`/`delete`) — só smart pointers.
- Todo acesso a buffer/array é bounds-checked antes do acesso.
- Todo ponteiro recebido da API do MariaDB é verificado contra `nullptr`
  antes de uso.

Qualquer diff que viole essas regras deve ser bloqueado — ver skill
`tideflow-safety-review` / agent `storage-code-reviewer`.

### Protocolo de durabilidade (não violar silenciosamente)

`.tmp` → fsync → rename → **troca atômica do MANIFEST** (ponto de commit) →
chunk obsoleto só é apagado quando o último snapshot que o referencia é
liberado. Qualquer mudança em `wal.rs`, `manifest.rs`, `chunk_writer.rs`,
`compaction.rs` ou `table.rs` deve preservar esse protocolo — ver
`docs/architecture.md` §Protocolo de durabilidade. Se não tiver certeza,
rode `tideflow-safety-review` antes de concluir.

## Como apresentar resultados de performance

- **Sempre quantificar**: MB/s, registros/s, taxa de compressão, ns/op,
  % do tempo. Nunca "rápido"/"lento" sem número.
- Toda recomendação tem **esforço** (baixo/médio/alto) e **risco**
  (baixo/médio/alto).
- Ranquear findings por impacto medido, não por curiosidade técnica.
- Se não tem medição, diga **"não medido"** — não invente números.

## Cautela em build/CI

- Nunca compilar a engine como `DEFAULT` (vira parte do servidor, viola a
  decisão registrada em `docs/architecture.md` — a spec exige
  `INSTALL SONAME`). Mantenha `STORAGE_ENGINE MODULE_ONLY`.
- Nunca remover `catch_unwind` da fronteira FFI.
- Nunca commitar mudança em `storage/tideflow/rust/**` ou
  `storage/tideflow/*.cc/.h` sem rodar ao menos `tideflow-rust-check`.
- O plugin se declara `EXPERIMENTAL` (`plugin_maturity=experimental`) — isso
  é intencional em v0.x, não "corrigir" sem o usuário pedir.

## Quando despachar agents em paralelo

Se a tarefa precisa de **dois ou mais** agents (ex.: mudar `tideflow-core` E
validar com MTR, ou revisar segurança E medir performance), despache em
paralelo numa única mensagem com múltiplas tool calls. Série desperdiça
tempo.
