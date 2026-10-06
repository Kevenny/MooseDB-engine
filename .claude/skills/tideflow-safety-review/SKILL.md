---
name: tideflow-safety-review
description: Revisão de segurança pré-commit para o TideFlow Engine — verifica que o diff não introduz unsafe fora de tideflow-ffi, não quebra o protocolo de durabilidade nem o contrato de concorrência, e que ponteiros/buffers seguem as regras obrigatórias do projeto. Use antes de comitar qualquer mudança em storage/tideflow/rust/** ou storage/tideflow/*.cc/.h.
---

# Revisão de segurança pré-commit

## Quando usar

- Antes de `git commit` em qualquer mudança sob `storage/tideflow/`.
- Depois que `rust-core-engineer` ou `cpp-handler-engineer` terminam uma
  implementação, antes de reportar a tarefa como concluída.

## Como executar

Delegue ao agent `storage-code-reviewer`, passando o escopo (diff staged,
últimos N commits, ou lista de arquivos). Ele roda `git diff` (read-only) e
revisa contra a checklist de invariantes do projeto — não contra estilo
geral de código.

## Contrato de saída

```markdown
## Revisão de segurança — <escopo>

### Bloqueadores
1. `arquivo:linha` — <violação concreta>

### Nits
1. `arquivo:linha` — <sugestão>
```

**Qualquer bloqueador impede considerar a tarefa concluída.** Corrija e rode
a revisão de novo antes de prosseguir para `tideflow-rust-check` /
`tideflow-build` / commit.

## Quem executa

Agente principal (ou o agent que implementou a mudança) despacha;
**`storage-code-reviewer`** revisa.
