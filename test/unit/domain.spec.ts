import { describe, expect, test } from 'bun:test';

import { canonicalPayload, canonicalPayloadHash, sha256 } from '../../src/application/canonical-hash';
import { CurrencyMismatchError, FailureCode, InsufficientFundsError, InvalidLedgerEntryError, InvalidMoneyError, InvalidTransactionStateError, ValidationError } from '../../src/domain/errors';
import { Money } from '../../src/domain/money';
import { LedgerDirection, WalletLedgerEntry } from '../../src/domain/wallet-ledger-entry';
import { Wallet } from '../../src/domain/wallet';
import { WagerKind, WagerStatus, WagerTransaction } from '../../src/domain/wager-transaction';

const brl = (amount: string) => Money.from({ amount, currency: 'BRL' });

function novaCarteira(saldo = '100.00'): Wallet {
  return Wallet.open({ id: 'w-1', playerId: 'p-1', initialBalance: brl(saldo) });
}

function transacao(overrides: Partial<Parameters<typeof WagerTransaction.create>[0]> = {}): WagerTransaction {
  return WagerTransaction.create({
    id: `t-${Math.random().toString(36).slice(2, 8)}`,
    providerId: 'provider-a',
    externalTransactionId: 'ext-1',
    idempotencyKey: 'provider-a:ext-1',
    payloadHash: 'hash-1',
    playerId: 'p-1',
    walletId: 'w-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: WagerKind.Bet,
    money: brl('25.00'),
    referenceExternalTransactionId: null,
    ...overrides,
  });
}

/* ------------------------------------------------------------------- Money */

describe('Money', () => {
  test('aceita string decimal com exatamente 2 casas', () => {
    expect(Money.from({ amount: '25.00', currency: 'BRL' }).toJSON()).toEqual({ amount: '25.00', currency: 'BRL' });
    expect(Money.from({ amount: '0.01', currency: 'BRL' }).toJSON().amount).toBe('0.01');
    expect(Money.from({ amount: '1234567.89', currency: 'BRL' }).toJSON().amount).toBe('1234567.89');
  });

  test('rejeita "25.0" (escala diferente de 2)', () => {
    expect(() => Money.from({ amount: '25.0', currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('rejeita "25" (sem casas decimais)', () => {
    expect(() => Money.from({ amount: '25', currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('rejeita "25.000" (mais de 2 casas)', () => {
    expect(() => Money.from({ amount: '25.000', currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('rejeita valor negativo no contrato de entrada', () => {
    expect(() => Money.from({ amount: '-10.00', currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('rejeita NaN, Infinity, notacao cientifica e string vazia', () => {
    for (const invalido of ['NaN', 'Infinity', '1e5', '', '  ', 'abc', '25,00', '1.2.3']) {
      expect(() => Money.from({ amount: invalido, currency: 'BRL' })).toThrow(InvalidMoneyError);
    }
  });

  test('rejeita amount que nao e string (number proibido para dinheiro)', () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: 'BRL' })).toThrow(InvalidMoneyError);
  });

  test('rejeita moeda fora do padrao ISO-4217', () => {
    expect(() => Money.from({ amount: '10.00', currency: 'brl' })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: '10.00', currency: 'BR' })).toThrow(InvalidMoneyError);
  });

  test('zero e as consultas de sinal', () => {
    const zero = Money.zero('BRL');
    expect(zero.isZero()).toBe(true);
    expect(zero.isPositive()).toBe(false);
    expect(zero.isNegative()).toBe(false);
    expect(brl('1.00').isPositive()).toBe(true);
  });

  test('soma decimal exata (0.10 + 0.20 = 0.30 - float erraria)', () => {
    expect(brl('0.10').add(brl('0.20')).toJSON().amount).toBe('0.30');
  });

  test('subtracao e negate', () => {
    expect(brl('100.00').subtract(brl('99.99')).toJSON().amount).toBe('0.01');
    expect(brl('10.00').negate().toJSON().amount).toBe('-10.00');
  });

  test('e imutavel: as operacoes devolvem nova instancia', () => {
    const original = brl('50.00');
    const soma = original.add(brl('10.00'));
    expect(original.toJSON().amount).toBe('50.00');
    expect(soma.toJSON().amount).toBe('60.00');
  });

  test('comparacoes', () => {
    expect(brl('10.00').isLessThan(brl('10.01'))).toBe(true);
    expect(brl('10.01').isGreaterThan(brl('10.00'))).toBe(true);
    expect(brl('10.00').equals(brl('10.00'))).toBe(true);
    expect(brl('10.00').equals(brl('10.01'))).toBe(false);
  });

  test('operacao entre moedas diferentes lanca erro', () => {
    expect(() => brl('10.00').add(Money.from({ amount: '10.00', currency: 'USD' }))).toThrow(CurrencyMismatchError);
    expect(() => brl('10.00').equals(Money.from({ amount: '10.00', currency: 'USD' }))).toThrow(CurrencyMismatchError);
  });

  test('toString usa 2 casas', () => {
    expect(brl('7.50').toString()).toBe('7.50 BRL');
  });
});

/* ------------------------------------------------------------------ Wallet */

describe('Wallet', () => {
  test('nasce com version 1 e o saldo inicial', () => {
    const wallet = novaCarteira('100.00');
    expect(wallet.version).toBe(1);
    expect(wallet.balance.toJSON().amount).toBe('100.00');
    expect(wallet.currency).toBe('BRL');
  });

  test('debit devolve o saldo anterior e incrementa version', () => {
    const wallet = novaCarteira('100.00');
    const anterior = wallet.debit(brl('25.00'), new Date());

    expect(anterior.toJSON().amount).toBe('100.00');
    expect(wallet.balance.toJSON().amount).toBe('75.00');
    expect(wallet.version).toBe(2);
  });

  test('credit incrementa saldo e version', () => {
    const wallet = novaCarteira('100.00');
    wallet.credit(brl('50.00'), new Date());

    expect(wallet.balance.toJSON().amount).toBe('150.00');
    expect(wallet.version).toBe(2);
  });

  test('debit acima do saldo lanca INSUFFICIENT_FUNDS', () => {
    const wallet = novaCarteira('100.00');

    try {
      wallet.debit(brl('100.01'), new Date());
      throw new Error('deveria ter lancado');
    } catch (erro) {
      expect(erro).toBeInstanceOf(InsufficientFundsError);
      expect((erro as InsufficientFundsError).failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
    }

    // Invariante: nada mudou.
    expect(wallet.balance.toJSON().amount).toBe('100.00');
    expect(wallet.version).toBe(1);
  });

  test('reversao sem saldo tem failureCode proprio', () => {
    const wallet = novaCarteira('100.00');
    wallet.debit(brl('90.00'), new Date());

    try {
      wallet.debit(brl('20.00'), new Date(), FailureCode.INSUFFICIENT_FUNDS_FOR_REVERSAL);
      throw new Error('deveria ter lancado');
    } catch (erro) {
      expect((erro as InsufficientFundsError).failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS_FOR_REVERSAL);
    }
  });

  test('debita exatamente o saldo ate zero (fronteira)', () => {
    const wallet = novaCarteira('100.00');
    wallet.debit(brl('100.00'), new Date());
    expect(wallet.balance.toJSON().amount).toBe('0.00');
    expect(wallet.balance.isNegative()).toBe(false);
  });

  test('moeda diferente da carteira lanca erro', () => {
    const wallet = novaCarteira('100.00');
    expect(() => wallet.debit(Money.from({ amount: '10.00', currency: 'USD' }), new Date())).toThrow(CurrencyMismatchError);
  });

  test('valor zero ou negativo na movimentacao e invalido', () => {
    const wallet = novaCarteira('100.00');
    expect(() => wallet.debit(brl('0.00'), new Date())).toThrow(ValidationError);
    expect(() => wallet.credit(brl('0.00'), new Date())).toThrow(ValidationError);
  });

  test('rehydrate reconstroi o estado sem revalidar transicoes', () => {
    const wallet = Wallet.rehydrate({
      id: 'w-9',
      playerId: 'p-9',
      currency: 'BRL',
      balance: brl('12.34'),
      version: 7,
      createdAt: new Date('2026-01-01T00:00:00Z'),
      updatedAt: new Date('2026-01-02T00:00:00Z'),
    });

    expect(wallet.balance.toJSON().amount).toBe('12.34');
    expect(wallet.version).toBe(7);
  });
});

/* ------------------------------------------------------------ LedgerEntry */

describe('WalletLedgerEntry', () => {
  const base = {
    id: 'l-1',
    walletId: 'w-1',
    transactionId: 't-1',
    money: brl('25.00'),
    createdAt: new Date(),
  };

  test('create valida a aritmetica do debito', () => {
    const entry = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Debit,
      balanceBefore: brl('100.00'),
      balanceAfter: brl('75.00'),
    });

    expect(entry.isBalanced()).toBe(true);
  });

  test('create valida a aritmetica do credito', () => {
    const entry = WalletLedgerEntry.create({
      ...base,
      direction: LedgerDirection.Credit,
      balanceBefore: brl('75.00'),
      balanceAfter: brl('100.00'),
    });

    expect(entry.isBalanced()).toBe(true);
  });

  test('create recusa lancamento que nao fecha', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        direction: LedgerDirection.Debit,
        balanceBefore: brl('100.00'),
        balanceAfter: brl('80.00'),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });

  test('create recusa valor nao positivo', () => {
    expect(() =>
      WalletLedgerEntry.create({
        ...base,
        money: Money.zero('BRL'),
        direction: LedgerDirection.Debit,
        balanceBefore: brl('100.00'),
        balanceAfter: brl('100.00'),
      }),
    ).toThrow(InvalidLedgerEntryError);
  });
});

/* ------------------------------------------------------- WagerTransaction */

describe('WagerTransaction', () => {
  test('nasce em PENDING', () => {
    expect(transacao().status).toBe(WagerStatus.Pending);
    expect(transacao().isTerminal()).toBe(false);
  });

  test('REFUND/ROLLBACK exigem referencia', () => {
    expect(() => transacao({ kind: WagerKind.Refund })).toThrow(ValidationError);
    expect(() => transacao({ kind: WagerKind.Rollback })).toThrow(ValidationError);
    expect(transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' }).requiresReference()).toBe(true);
  });

  test('BET/WIN/LOSS nao exigem referencia', () => {
    expect(transacao().requiresReference()).toBe(false);
    expect(transacao({ kind: WagerKind.Loss }).requiresReference()).toBe(false);
  });

  test('valor precisa ser positivo', () => {
    expect(() => transacao({ money: Money.zero('BRL') })).toThrow(ValidationError);
  });

  test('LOSS nao afeta saldo', () => {
    expect(transacao({ kind: WagerKind.Loss }).affectsBalance()).toBe(false);
    expect(transacao().affectsBalance()).toBe(true);
  });

  test('markProcessed grava referencia e horario', () => {
    const t = transacao();
    const at = new Date();

    t.markProcessed('ref-id', at);

    expect(t.status).toBe(WagerStatus.Processed);
    expect(t.referenceTransactionId).toBe('ref-id');
    expect(t.processedAt).toEqual(at);
    expect(t.isTerminal()).toBe(true);
  });

  test('PROCESSED e terminal: transicionar de novo e erro de programacao', () => {
    const t = transacao();
    t.markProcessed(null);

    expect(() => t.reject(FailureCode.ALREADY_REVERSED)).toThrow(InvalidTransactionStateError);
    expect(() => t.markPendingReference(new Date())).toThrow(InvalidTransactionStateError);
  });

  test('REJECTED guarda o failureCode e e terminal', () => {
    const t = transacao();
    t.reject(FailureCode.INSUFFICIENT_FUNDS);

    expect(t.status).toBe(WagerStatus.Rejected);
    expect(t.failureCode).toBe(FailureCode.INSUFFICIENT_FUNDS);
    expect(t.isTerminal()).toBe(true);
    expect(() => t.markProcessed(null)).toThrow(InvalidTransactionStateError);
  });

  test('FAILED guarda o failureCode e e terminal', () => {
    const t = transacao();
    t.fail(FailureCode.REFERENCE_NOT_FOUND_TIMEOUT);

    expect(t.status).toBe(WagerStatus.Failed);
    expect(t.failureCode).toBe(FailureCode.REFERENCE_NOT_FOUND_TIMEOUT);
    expect(t.isTerminal()).toBe(true);
  });

  test('FAILED nao sai para nenhum outro estado', () => {
    const t = transacao();
    t.fail(FailureCode.REFERENCE_NOT_FOUND_TIMEOUT);

    expect(() => t.markProcessed(null)).toThrow(InvalidTransactionStateError);
    expect(() => t.markPendingReference(new Date(Date.now() + 60_000))).toThrow(InvalidTransactionStateError);
    expect(() => t.reject(FailureCode.INSUFFICIENT_FUNDS)).toThrow(InvalidTransactionStateError);
  });

  test('markPendingReference agenda nova tentativa', () => {
    const t = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });
    const proxima = new Date(Date.now() + 1000);

    t.markPendingReference(proxima);

    expect(t.status).toBe(WagerStatus.PendingReference);
    expect(t.nextRetryAt).toEqual(proxima);
    expect(t.retryCount).toBe(0);
    expect(t.isTerminal()).toBe(false);
  });

  test('reagendar conta a tentativa', () => {
    const t = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });
    t.reagendar(3, new Date());

    expect(t.retryCount).toBe(3);
    expect(t.status).toBe(WagerStatus.PendingReference);
  });

  test('matchesPayload compara o hash', () => {
    expect(transacao({ payloadHash: 'abc' }).matchesPayload('abc')).toBe(true);
    expect(transacao({ payloadHash: 'abc' }).matchesPayload('outro')).toBe(false);
  });

  describe('checkReference', () => {
    function referencia(overrides: Partial<Parameters<typeof WagerTransaction.create>[0]> = {}): WagerTransaction {
      const ref = transacao({
        id: 't-ref',
        externalTransactionId: 'ext-ref',
        kind: WagerKind.Bet,
        money: brl('25.00'),
        referenceExternalTransactionId: null,
        ...overrides,
      });
      ref.markProcessed(null);
      return ref;
    }

    test('REFUND de uma BET valida passa', () => {
      const refund = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });
      expect(refund.checkReference(referencia())).toBeNull();
    });

    test('REFUND de um WIN e INVALID_REFERENCE_KIND', () => {
      const refund = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });
      expect(refund.checkReference(referencia({ kind: WagerKind.Win }))).toBe(FailureCode.INVALID_REFERENCE_KIND);
    });

    test('ROLLBACK de LOSS e INVALID_REFERENCE_KIND', () => {
      const rollback = transacao({ kind: WagerKind.Rollback, referenceExternalTransactionId: 'ext-ref' });
      expect(rollback.checkReference(referencia({ kind: WagerKind.Loss }))).toBe(FailureCode.INVALID_REFERENCE_KIND);
    });

    test('ROLLBACK de BET, WIN e REFUND e aceito', () => {
      const rollback = transacao({ kind: WagerKind.Rollback, referenceExternalTransactionId: 'ext-ref' });
      expect(rollback.checkReference(referencia({ kind: WagerKind.Bet }))).toBeNull();
      expect(rollback.checkReference(referencia({ kind: WagerKind.Win }))).toBeNull();
      expect(
        rollback.checkReference(
          referencia({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-bet-anterior' }),
        ),
      ).toBeNull();
    });

    test('contexto diferente e REFERENCE_CONTEXT_MISMATCH', () => {
      const refund = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });

      expect(refund.checkReference(referencia({ playerId: 'outro-player' }))).toBe(FailureCode.REFERENCE_CONTEXT_MISMATCH);
      expect(refund.checkReference(referencia({ walletId: 'outra-wallet' }))).toBe(FailureCode.REFERENCE_CONTEXT_MISMATCH);
      expect(refund.checkReference(referencia({ roundId: 'outra-rodada' }))).toBe(FailureCode.REFERENCE_CONTEXT_MISMATCH);
      expect(refund.checkReference(referencia({ providerId: 'outro-provider' }))).toBe(FailureCode.REFERENCE_CONTEXT_MISMATCH);
    });

    test('valor diferente e REFERENCE_AMOUNT_MISMATCH', () => {
      const refund = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });
      expect(refund.checkReference(referencia({ money: brl('30.00') }))).toBe(FailureCode.REFERENCE_AMOUNT_MISMATCH);
    });

    test('referencia rejeitada e REFERENCE_NOT_PROCESSED', () => {
      const refund = transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' });
      const rejeitada = transacao({ kind: WagerKind.Bet, externalTransactionId: 'ext-ref' });
      rejeitada.reject(FailureCode.INSUFFICIENT_FUNDS);

      expect(refund.checkReference(rejeitada)).toBe(FailureCode.REFERENCE_NOT_PROCESSED);
    });
  });

  describe('ledgerDirectionFor', () => {
    test('BET debita, WIN e REFUND creditam', () => {
      expect(transacao().ledgerDirectionFor()).toBe(LedgerDirection.Debit);
      expect(transacao({ kind: WagerKind.Win }).ledgerDirectionFor()).toBe(LedgerDirection.Credit);
      expect(
        transacao({ kind: WagerKind.Refund, referenceExternalTransactionId: 'ext-ref' }).ledgerDirectionFor(),
      ).toBe(LedgerDirection.Credit);
    });

    test('LOSS nao gera lancamento', () => {
      expect(transacao({ kind: WagerKind.Loss }).ledgerDirectionFor()).toBeNull();
    });

    test('ROLLBACK inverte a direcao da referencia', () => {
      const rollback = transacao({ kind: WagerKind.Rollback, referenceExternalTransactionId: 'ext-ref' });
      const bet = transacao({ kind: WagerKind.Bet, externalTransactionId: 'ext-ref' });
      const win = transacao({ kind: WagerKind.Win, externalTransactionId: 'ext-ref' });

      expect(rollback.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
      expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    });
  });
});

/* ------------------------------------------------------- payload canonico */

describe('Hash canonico do payload', () => {
  const base = {
    providerId: 'provider-a',
    externalTransactionId: 'ext-1',
    playerId: 'p-1',
    walletId: 'w-1',
    roundId: 'round-1',
    gameId: 'game-1',
    kind: 'BET',
    money: { amount: '25.00', currency: 'BRL' },
    referenceExternalTransactionId: null,
  };

  test('e deterministico', () => {
    expect(canonicalPayloadHash(base)).toBe(canonicalPayloadHash({ ...base }));
    expect(canonicalPayloadHash(base)).toHaveLength(64);
  });

  test('a ordem das chaves do objeto nao muda o hash', () => {
    const reordenado = {
      referenceExternalTransactionId: null,
      money: { currency: 'BRL', amount: '25.00' },
      kind: 'BET',
      gameId: 'game-1',
      roundId: 'round-1',
      walletId: 'w-1',
      playerId: 'p-1',
      externalTransactionId: 'ext-1',
      providerId: 'provider-a',
    };

    expect(canonicalPayloadHash(reordenado)).toBe(canonicalPayloadHash(base));
  });

  test('assina exatamente os campos de negocio', () => {
    expect(canonicalPayload(base)).toEqual({
      externalTransactionId: 'ext-1',
      gameId: 'game-1',
      kind: 'BET',
      money: { amount: '25.00', currency: 'BRL' },
      playerId: 'p-1',
      providerId: 'provider-a',
      referenceExternalTransactionId: null,
      roundId: 'round-1',
      walletId: 'w-1',
    });
  });

  test('referencia ausente e normalizada para null', () => {
    const semCampo = { ...base } as Record<string, unknown>;
    delete semCampo.referenceExternalTransactionId;

    expect(canonicalPayloadHash(semCampo as typeof base)).toBe(canonicalPayloadHash(base));
  });

  test('qualquer campo de negocio alterado muda o hash', () => {
    const variacoes = [
      { ...base, providerId: 'provider-b' },
      { ...base, externalTransactionId: 'ext-2' },
      { ...base, playerId: 'p-2' },
      { ...base, walletId: 'w-2' },
      { ...base, roundId: 'round-2' },
      { ...base, gameId: 'game-2' },
      { ...base, kind: 'WIN' },
      { ...base, money: { amount: '25.01', currency: 'BRL' } },
      { ...base, money: { amount: '25.00', currency: 'USD' } },
      { ...base, referenceExternalTransactionId: 'ext-0' },
    ];

    for (const variacao of variacoes) {
      expect(canonicalPayloadHash(variacao)).not.toBe(canonicalPayloadHash(base));
    }
  });

  test('sha256 de documento arbitrario tambem e canonico', () => {
    expect(sha256({ b: 1, a: 2 })).toBe(sha256({ a: 2, b: 1 }));
    expect(sha256({ a: 1 })).not.toBe(sha256({ a: 2 }));
  });
});