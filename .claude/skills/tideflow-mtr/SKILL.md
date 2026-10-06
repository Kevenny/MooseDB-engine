---
name: tideflow-mtr
description: Roda a suíte MTR (14 testes funcionais) do TideFlow contra o servidor MariaDB oficial via Docker. Use para validar DDL, tipos, chunk queries, manutenção e recuperação de crash end-to-end.
---

# Suíte MTR — testes funcionais end-to-end

## Quando usar

- Depois de `tideflow-build` ter gerado `build/ha_tideflow.so` com sucesso.
- Antes de considerar qualquer mudança em `ha_tideflow.cc/.h` ou em
  `tideflow-core` (comportamento observável) pronta.
- Quando o usuário pedir "roda a suíte MTR" / "testa DDL" / "testa tipos".

Testes disponíveis em `mysql-test/suite/tideflow/t/`: `basic`, `chunk_query`,
`types`, `crash_recovery`, `ddl`, `maintenance`.

## Pré-requisito

Requer `build/ha_tideflow.so` já compilado — rode `tideflow-build` primeiro
se ainda não existir ou se o código mudou desde o último build.

## Como executar

Suíte completa:
```bash
docker run --rm -v "$PWD:/work" tideflow-test bash /work/docker/run-mtr.sh
```

Um teste específico (mais rápido ao iterar):
```bash
docker run --rm -v "$PWD:/work" tideflow-test bash /work/docker/run-mtr.sh --do-test=ddl
```

## Contrato de saída

**Delegue execução e leitura ao agent `tideflow-build-runner`.** Ele devolve:

```json
{
  "total": 14, "passed": 0, "failed": 0,
  "falhas": [{"teste": "ddl", "diff_resumido": "..."}]
}
```

Para cada falha, o `diff_resumido` deve ser só o trecho divergente entre
`.result` e `.reject` — nunca o arquivo `.result` inteiro.

## Quem executa

Agente principal (ou `cpp-handler-engineer`/`rust-core-engineer` ao terminar
uma mudança) despacha; **`tideflow-build-runner`** executa e lê.
