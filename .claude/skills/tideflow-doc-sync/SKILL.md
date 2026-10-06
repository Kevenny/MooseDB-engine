---
name: tideflow-doc-sync
description: Compara o código implementado contra tideflow_engine_spec.md e atualiza a tabela de divergências em docs/architecture.md quando uma mudança intencional se afasta da spec. Use depois de qualquer decisão de design que diverge da spec original, ou quando o usuário perguntar "isso está documentado?"/"a spec está desatualizada?".
---

# Sincronização spec ↔ implementação

## Quando usar

- Depois que `rust-core-engineer` ou `cpp-handler-engineer` tomam uma
  decisão que diverge de `tideflow_engine_spec.md` (assinatura diferente,
  comportamento diferente, estrutura de dados diferente).
- Quando o usuário perguntar se a documentação está atualizada.

## Como executar

1. Leia a seção relevante de `tideflow_engine_spec.md` e a tabela de
   divergências em `docs/architecture.md` (§Divergências da spec).
2. Se a mudança é uma divergência **nova**, adicione uma linha na tabela:
   `| # | <o que a spec diz> | <o que foi implementado> | <motivo> |`.
   Siga o estilo das 20 entradas existentes — motivo é sempre técnico e
   concreto (ex.: "o servidor tipa X com esse nome exato"), nunca "decisão
   de design" vago.
3. Se a mudança **resolve** uma divergência (código passou a seguir a
   spec), remova a linha correspondente da tabela e considere se vale
   atualizar a spec também — só com confirmação do usuário, a spec é a
   referência de produto e não deve ser editada levianamente.
4. Se a mudança introduz uma **limitação conhecida** nova (não é bem uma
   divergência, é algo que ainda não foi feito), adicione em
   §Limitações conhecidas em vez da tabela de divergências.

## Contrato de saída

Edite `docs/architecture.md` diretamente (é um documento pequeno, leitura
direta é aceitável — não precisa delegar). Reporte ao usuário só a(s)
linha(s) adicionada(s)/removida(s), não o arquivo inteiro.

## Quem executa

Agente principal — arquivo pequeno (~190 linhas), não há razão para
delegar a leitura.
