---
name: tideflow-build
description: Builda as imagens Docker (dev/test) e o plugin ha_tideflow.so do TideFlow Engine. Use quando o usuário pedir para "buildar", "compilar o plugin" ou validar que o código compila contra o MariaDB 11.4 oficial.
---

# Build do plugin TideFlow

## Quando usar

- Depois de qualquer mudança em `storage/tideflow/**` (Rust ou C++) antes de
  rodar MTR.
- Quando o usuário pedir "builda", "compila o plugin", "gera o .so".
- Primeira vez no repositório (precisa das imagens Docker).

## Como executar

1. Se as imagens não existem ainda:
   ```bash
   docker build -f docker/dev.Dockerfile  -t tideflow-dev  docker/
   docker build -f docker/test.Dockerfile -t tideflow-test docker/
   ```
2. Build do plugin:
   ```bash
   docker run --rm -v "$PWD:/work" -v tideflow-cargo:/usr/local/cargo/registry \
     -v tideflow-build:/build tideflow-dev bash /work/docker/build-plugin.sh
   ```
   Primeira execução configura a árvore de build do servidor MariaDB
   (`mysql_release`) — é lento. Execuções seguintes reusam o volume
   `tideflow-build`, só recompilam o que mudou.
3. Resultado esperado: `build/ha_tideflow.so`.

## Contrato de saída

A saída do `cmake`/`ninja` é verbosa. **Delegue a execução e leitura ao
agent `tideflow-build-runner`** — ele devolve só:

```json
{"status": "success|fail", "arquivo": "build/ha_tideflow.so", "erro": "..."}
```

Em caso de falha, o `build-runner` deve apontar o arquivo/linha do erro de
compilação (C++) ou o crate/módulo (Rust), não o log completo do ninja.

## Quem executa

Agente principal despacha; **`tideflow-build-runner`** executa e lê.
