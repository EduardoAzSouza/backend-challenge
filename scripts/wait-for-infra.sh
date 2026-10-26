#!/usr/bin/env bash
# Espera PostgreSQL e LocalStack ficarem prontos para uso.
#
# Roda antes do `migration:run`: o `docker compose up -d` volta assim que o
# container sobe, e o PostgreSQL pode ainda nao estar aceitando conexao.
set -euo pipefail
cd "$(dirname "$0")/.."

# As portas podem ter sido trocadas no .env (para conviver com outra stack na
# mesma maquina), entao lemos de la. Variavel de ambiente tem precedencia.
valor_do_env() {
  [ -f .env ] && sed -n "s/^${1}=//p" .env | tail -n 1 | tr -d '\r"' || true
}

LOCALSTACK_PORT="${LOCALSTACK_PORT:-$(valor_do_env LOCALSTACK_PORT)}"
AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-$(valor_do_env AWS_ENDPOINT_URL)}"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:${LOCALSTACK_PORT:-4566}}"
POSTGRES_USER="${POSTGRES_USER:-$(valor_do_env POSTGRES_USER)}"
POSTGRES_DB="${POSTGRES_DB:-$(valor_do_env POSTGRES_DB)}"

aguardar() {
  local rotulo="$1"
  shift

  echo "aguardando ${rotulo}..."
  for _ in $(seq 1 45); do
    if "$@" >/dev/null 2>&1; then
      echo "  ${rotulo} ok"
      return 0
    fi
    sleep 2
  done

  echo "  ${rotulo} nao respondeu. Rode 'docker compose up -d' e confira as portas no .env." >&2
  exit 1
}

aguardar "PostgreSQL" docker compose exec -T postgres pg_isready \
  -U "${POSTGRES_USER:-jungle}" -d "${POSTGRES_DB:-wagering_db}"
aguardar "LocalStack" curl -sf "${ENDPOINT%/}/_localstack/health"
