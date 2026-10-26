#!/usr/bin/env bash
# Cria as filas SQS no LocalStack (idempotente).
set -euo pipefail
cd "$(dirname "$0")/.."

bun run scripts/init-sqs.ts