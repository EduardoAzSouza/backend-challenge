import { createHash } from 'node:crypto';

import type { MoneyProps } from '../domain/money';

/** Campos de negocio de uma requisicao de aposta. */
export interface WagerRequestLike {
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  money: MoneyProps;
  referenceExternalTransactionId?: string | null;
}

/**
 * JSON canonico: ordena as chaves recursivamente para que dois objetos
 * equivalentes produzam exatamente a mesma string.
 */
export function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }

  if (value !== null && typeof value === 'object') {
    const entrada = value as Record<string, unknown>;
    const saida: Record<string, unknown> = {};

    for (const chave of Object.keys(entrada).sort()) {
      if (entrada[chave] !== undefined) {
        saida[chave] = canonicalize(entrada[chave]);
      }
    }

    return saida;
  }

  return value;
}

export function sha256(value: unknown): string {
  return createHash('sha256').update(JSON.stringify(canonicalize(value))).digest('hex');
}

/**
 * Subconjunto EXATO de campos que entram no hash.
 *
 * O `Idempotency-Key`, o `messageId`, o horario e qualquer metadado de
 * transporte ficam de fora: reenviar a mesma operacao por HTTP ou por SQS tem
 * que gerar o mesmo hash.
 */
export function canonicalPayload(input: WagerRequestLike): Record<string, unknown> {
  return {
    externalTransactionId: input.externalTransactionId,
    gameId: input.gameId,
    kind: input.kind,
    money: {
      amount: input.money.amount,
      currency: input.money.currency,
    },
    playerId: input.playerId,
    providerId: input.providerId,
    referenceExternalTransactionId: input.referenceExternalTransactionId ?? null,
    roundId: input.roundId,
    walletId: input.walletId,
  };
}

export function canonicalPayloadHash(input: WagerRequestLike): string {
  return sha256(canonicalPayload(input));
}