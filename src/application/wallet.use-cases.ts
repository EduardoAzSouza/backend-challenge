import { randomUUID } from 'node:crypto';
import type { DataSource, QueryRunner } from 'typeorm';

import { ConflictError, NotFoundError, ValidationError } from '../domain/errors';
import { WagerTransactionProcessed, WalletBalanceChanged } from '../domain/events';
import { Money, type MoneyProps } from '../domain/money';
import { LedgerDirection, WalletLedgerEntry } from '../domain/wallet-ledger-entry';
import { Wallet } from '../domain/wallet';
import { WagerKind, WagerTransaction } from '../domain/wager-transaction';
import { withTransaction } from '../infrastructure/database/data-source';
import {
  toLedgerEntry,
  toWagerTransaction,
  toWallet,
  type LedgerEntryRow,
  type WagerTransactionRow,
  type WalletRow,
} from '../infrastructure/database/mappers';
import {
  enqueueEvent,
  insertLedgerEntry,
  insertTransaction,
  insertWallet,
  isUniqueViolation,
} from '../infrastructure/database/sql';
import { logger } from '../infrastructure/observability/logger';
import { metrics } from '../infrastructure/observability/metrics';

import { canonicalPayloadHash } from './canonical-hash';

export interface CreateWalletInput {
  playerId: string;
  initialBalance: MoneyProps;
}

export interface WalletView {
  id: string;
  playerId: string;
  balance: MoneyProps;
  version: number;
}

export interface LedgerEntryView {
  id: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  createdAt: string;
}

export interface LedgerPage {
  walletId: string;
  entries: LedgerEntryView[];
  nextCursor: string | null;
}

export interface ReconciliationView {
  walletId: string;
  storedBalance: MoneyProps;
  calculatedBalance: MoneyProps;
  difference: MoneyProps;
  consistent: boolean;
  checkedEntries: number;
}

export interface TransactionView {
  transactionId: string;
  providerId: string;
  externalTransactionId: string;
  playerId: string;
  walletId: string;
  roundId: string;
  gameId: string;
  kind: string;
  status: string;
  money: MoneyProps;
  failureCode: string | null;
  referenceExternalTransactionId: string | null;
  createdAt: string;
  processedAt: string | null;
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const LIMITE_MAXIMO_LEDGER = 100;

/** Consultas e criacao de carteira. */
export class WalletUseCases {
  constructor(private readonly dataSource: DataSource) {}

  /** POST /wallets - cria a carteira e, se houver saldo inicial, a transacao OPENING. */
  async createWallet(input: CreateWalletInput): Promise<WalletView> {
    if (typeof input.playerId !== 'string' || input.playerId.trim().length === 0) {
      throw new ValidationError('playerId e obrigatorio');
    }

    const saldoInicial = Money.from(input.initialBalance);

    return withTransaction(this.dataSource, async (qr) => {
      const wallet = Wallet.open({ id: randomUUID(), playerId: input.playerId.trim(), initialBalance: saldoInicial });

      try {
        await insertWallet(qr, wallet);
      } catch (erro) {
        // Quem decide a unicidade e o banco (UNIQUE player_id + currency).
        if (isUniqueViolation(erro)) {
          throw new ConflictError(
            `ja existe carteira para o player ${wallet.playerId} na moeda ${wallet.currency}`,
            'WALLET_ALREADY_EXISTS',
            { playerId: wallet.playerId, currency: wallet.currency },
          );
        }
        throw erro;
      }

      // Saldo inicial > 0 vira uma transacao interna OPENING + lancamento CREDIT,
      // na MESMA transacao SQL da criacao da carteira.
      if (saldoInicial.isPositive()) {
        await this.registrarAbertura(qr, wallet, saldoInicial);
      }

      logger.info('wallet_created', {
        walletId: wallet.id,
        playerId: wallet.playerId,
        currency: wallet.currency,
        version: wallet.version,
      });

      return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
    });
  }

  async getWallet(walletId: string): Promise<WalletView> {
    const wallet = await this.carregarCarteira(walletId);
    return { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  }

  /**
   * Paginacao por cursor opaco (base64 do `seq` do ledger).
   * O `seq` e um BIGSERIAL: ordena sem depender de timestamp e nao "pula"
   * linhas quando duas entradas nascem no mesmo milissegundo.
   */
  async getLedger(walletId: string, cursor?: string, limit?: number): Promise<LedgerPage> {
    await this.carregarCarteira(walletId);

    const tamanho = Math.min(Math.max(limit ?? 50, 1), LIMITE_MAXIMO_LEDGER);
    const seqDoCursor = cursor ? this.decodificarCursor(cursor) : null;

    const linhas: Array<LedgerEntryRow & { seq: string }> = await this.dataSource.query(
      `SELECT seq, id, wallet_id, wager_transaction_id, direction, amount, currency,
              balance_before, balance_after, created_at
         FROM wallet_ledger_entries
        WHERE wallet_id = $1
          AND ($2::bigint IS NULL OR seq < $2::bigint)
        ORDER BY seq DESC
        LIMIT $3`,
      [walletId, seqDoCursor, tamanho + 1],
    );

    const pagina = linhas.slice(0, tamanho);
    const temMais = linhas.length > tamanho;

    return {
      walletId,
      entries: pagina.map((linha) => this.paraViewDeLancamento(linha)),
      nextCursor: temMais && pagina.length > 0 ? this.codificarCursor(pagina[pagina.length - 1]!.seq) : null,
    };
  }

  async getTransactionById(transactionId: string): Promise<TransactionView> {
    if (!UUID.test(transactionId)) {
      throw new ValidationError('transactionId invalido', { transactionId });
    }

    const linhas: WagerTransactionRow[] = await this.dataSource.query(
      `SELECT * FROM wager_transactions WHERE id = $1`,
      [transactionId],
    );

    if (!linhas[0]) {
      throw new NotFoundError(`transacao ${transactionId} nao encontrada`, { transactionId });
    }

    return this.paraViewDeTransacao(linhas[0]);
  }

  async getTransactionByExternalId(providerId: string, externalTransactionId: string): Promise<TransactionView> {
    const linhas: WagerTransactionRow[] = await this.dataSource.query(
      `SELECT * FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
      [providerId, externalTransactionId],
    );

    if (!linhas[0]) {
      throw new NotFoundError(
        `transacao ${externalTransactionId} do provider ${providerId} nao encontrada`,
        { providerId, externalTransactionId },
      );
    }

    return this.paraViewDeTransacao(linhas[0]);
  }

  /**
   * POST /wallets/:walletId/reconciliation
   *
   * Reconstroi o saldo pelo extrato (SUM(CREDIT) - SUM(DEBIT)) e compara com o
   * saldo materializado. Divergencia NAO e corrigida em silencio: e logada,
   * contada em metrica e devolvida na resposta.
   */
  async reconcileWallet(walletId: string): Promise<ReconciliationView> {
    const wallet = await this.carregarCarteira(walletId);

    const agregados: Array<{ checked_entries: number; calculated: string }> = await this.dataSource.query(
      `SELECT COUNT(*)::int AS checked_entries,
              COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount ELSE -amount END), 0)::numeric(18,2) AS calculated
         FROM wallet_ledger_entries
        WHERE wallet_id = $1`,
      [walletId],
    );

    const agregado = agregados[0]!;
    const calculado = Money.from({ amount: agregado.calculated, currency: wallet.currency });
    const diferenca = wallet.balance.subtract(calculado);
    const consistente = diferenca.isZero();

    if (!consistente) {
      metrics.increment('reconciliation_divergences_total', { walletId });
      logger.error('reconciliation_divergence', {
        walletId,
        storedBalance: wallet.balance.toJSON().amount,
        calculatedBalance: calculado.toJSON().amount,
        checkedEntries: agregado.checked_entries,
      });
    }

    return {
      walletId,
      storedBalance: wallet.balance.toJSON(),
      calculatedBalance: calculado.toJSON(),
      difference: diferenca.toJSON(),
      consistent: consistente,
      checkedEntries: agregado.checked_entries,
    };
  }

  /* --------------------------------------------------------------- interno */

  private async carregarCarteira(walletId: string): Promise<Wallet> {
    if (!UUID.test(walletId)) {
      throw new ValidationError('walletId invalido', { walletId });
    }

    const linhas: WalletRow[] = await this.dataSource.query(
      `SELECT id, player_id, currency, balance_amount, version, created_at, updated_at FROM wallets WHERE id = $1`,
      [walletId],
    );

    if (!linhas[0]) {
      throw new NotFoundError(`carteira ${walletId} nao encontrada`, { walletId });
    }

    return toWallet(linhas[0]);
  }

  private async registrarAbertura(qr: QueryRunner, wallet: Wallet, saldoInicial: Money): Promise<void> {
    const agora = new Date();
    const externalTransactionId = `opening:${wallet.id}`;

    // OPENING nao pertence a uma rodada de jogo: roundId/gameId ficam como INTERNAL.
    const abertura = WagerTransaction.create({
      id: randomUUID(),
      providerId: 'internal',
      externalTransactionId,
      idempotencyKey: `internal:opening:${wallet.id}`,
      payloadHash: canonicalPayloadHash({
        providerId: 'internal',
        externalTransactionId,
        playerId: wallet.playerId,
        walletId: wallet.id,
        roundId: 'INTERNAL',
        gameId: 'INTERNAL',
        kind: WagerKind.Opening,
        money: saldoInicial.toJSON(),
        referenceExternalTransactionId: null,
      }),
      playerId: wallet.playerId,
      walletId: wallet.id,
      roundId: 'INTERNAL',
      gameId: 'INTERNAL',
      kind: WagerKind.Opening,
      money: saldoInicial,
      createdAt: agora,
    });

    abertura.markProcessed(null, agora);
    await insertTransaction(qr, abertura);

    // A carteira ja nasceu com o saldo; o lancamento de abertura vai de 0.00 -> saldo inicial.
    const lancamento = WalletLedgerEntry.create({
      id: randomUUID(),
      walletId: wallet.id,
      transactionId: abertura.id,
      direction: LedgerDirection.Credit,
      money: saldoInicial,
      balanceBefore: Money.zero(wallet.currency),
      balanceAfter: wallet.balance,
      createdAt: agora,
    });

    await insertLedgerEntry(qr, lancamento);

    await enqueueEvent(
      qr,
      WagerTransactionProcessed.from(
        {
          transactionId: abertura.id,
          providerId: abertura.providerId,
          externalTransactionId: abertura.externalTransactionId,
          playerId: abertura.playerId,
          walletId: abertura.walletId,
          kind: abertura.kind,
          status: abertura.status,
          money: abertura.money.toJSON(),
          balanceAfter: wallet.balance.toJSON(),
          balanceChanged: true,
        },
        { aggregateId: abertura.id },
      ),
    );

    await enqueueEvent(
      qr,
      WalletBalanceChanged.from(
        {
          walletId: wallet.id,
          transactionId: abertura.id,
          direction: LedgerDirection.Credit,
          money: saldoInicial.toJSON(),
          balanceBefore: Money.zero(wallet.currency).toJSON(),
          balanceAfter: wallet.balance.toJSON(),
          walletVersion: wallet.version,
        },
        { aggregateId: wallet.id },
      ),
    );
  }

  private codificarCursor(seq: string): string {
    return Buffer.from(String(seq), 'utf8').toString('base64');
  }

  private decodificarCursor(cursor: string): string {
    const decodificado = Buffer.from(cursor, 'base64').toString('utf8');

    if (!/^\d+$/.test(decodificado)) {
      throw new ValidationError('cursor invalido', { cursor });
    }

    return decodificado;
  }

  private paraViewDeLancamento(linha: LedgerEntryRow): LedgerEntryView {
    const entry = toLedgerEntry(linha);

    return {
      id: entry.id,
      transactionId: entry.transactionId,
      direction: entry.direction,
      money: entry.money.toJSON(),
      balanceBefore: entry.balanceBefore.toJSON(),
      balanceAfter: entry.balanceAfter.toJSON(),
      createdAt: entry.createdAt.toISOString(),
    };
  }

  private paraViewDeTransacao(linha: WagerTransactionRow): TransactionView {
    const transaction = toWagerTransaction(linha);

    return {
      transactionId: transaction.id,
      providerId: transaction.providerId,
      externalTransactionId: transaction.externalTransactionId,
      playerId: transaction.playerId,
      walletId: transaction.walletId,
      roundId: transaction.roundId,
      gameId: transaction.gameId,
      kind: transaction.kind,
      status: transaction.status,
      money: transaction.money.toJSON(),
      failureCode: transaction.failureCode,
      referenceExternalTransactionId: transaction.referenceExternalTransactionId,
      createdAt: transaction.createdAt.toISOString(),
      processedAt: transaction.processedAt?.toISOString() ?? null,
    };
  }
}