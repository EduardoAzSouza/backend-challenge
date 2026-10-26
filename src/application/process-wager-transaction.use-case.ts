import { randomUUID } from 'node:crypto';
import type { DataSource, QueryRunner } from 'typeorm';

import { config } from '../config';
import {
  BusinessRuleError,
  ConflictError,
  CurrencyMismatchError,
  FailureCode,
  NotFoundError,
  OpeningNotAllowedError,
  ValidationError,
} from '../domain/errors';
import {
  WagerTransactionPendingReference,
  WagerTransactionProcessed,
  WagerTransactionRejected,
  WalletBalanceChanged,
  type EventContext,
} from '../domain/events';
import { Money, type MoneyProps } from '../domain/money';
import { LedgerDirection, WalletLedgerEntry } from '../domain/wallet-ledger-entry';
import type { Wallet } from '../domain/wallet';
import { KINDS_DA_API, WagerKind, WagerStatus, WagerTransaction } from '../domain/wager-transaction';
import { withTransaction } from '../infrastructure/database/data-source';
import { toWagerTransaction, toWallet, type WagerTransactionRow, type WalletRow } from '../infrastructure/database/mappers';
import {
  enqueueEvent,
  insertLedgerEntry,
  insertTransaction,
  isUniqueViolation,
  uniqueViolationConstraint,
  updateTransaction,
  updateWallet,
} from '../infrastructure/database/sql';
import { logger } from '../infrastructure/observability/logger';
import { metrics } from '../infrastructure/observability/metrics';

import { canonicalPayloadHash, type WagerRequestLike } from './canonical-hash';

/** Requisicao ja validada: `kind` e `money` chegam tipados. */
export interface WagerRequest extends WagerRequestLike {
  idempotencyKey: string;
  kind: WagerKind;
}

/** Contexto de inbox: so existe quando a entrada e SQS. */
export interface InboxContext {
  messageId: string;
  consumerName: string;
  payloadHash: string;
}

export interface ProcessOptions {
  correlationId?: string | null;
  inbox?: InboxContext;
  /**
   * Modo do worker de PENDING_REFERENCE: em vez de responder com o replay da
   * linha parada, tenta resolver a referencia DE NOVO, reaproveitando a mesma
   * linha (e portanto as mesmas regras de negocio e a mesma taxonomia de erro).
   */
  retryPendingReference?: boolean;
}

export interface ProcessResult {
  transactionId: string;
  status: WagerStatus;
  money: MoneyProps;
  balance: MoneyProps;
  failureCode: FailureCode | null;
  /** true quando a resposta repete uma decisao ja tomada para a mesma key. */
  idempotentReplay: boolean;
  /** true quando a mensagem da fila ja havia sido processada (inbox). */
  duplicateMessage?: boolean;
}

/** Mapeamento resultado -> HTTP, unico para todos os endpoints. */
export function httpStatusForStatus(status: WagerStatus): number {
  switch (status) {
    case WagerStatus.Processed:
      return 200;
    case WagerStatus.Pending:
    case WagerStatus.PendingReference:
      return 202;
    case WagerStatus.Rejected:
      return 422;
    default:
      return 500;
  }
}

/**
 * CASO DE USO CENTRAL - o mesmo codigo atende HTTP, consumidor SQS e worker de
 * PENDING_REFERENCE.
 *
 * Invariantes garantidas aqui:
 *  1. dinheiro e sempre `Money` sobre NUMERIC(18,2), nunca float;
 *  2. idempotencia e UNIQUE (idempotency_key) + comparacao de payload_hash;
 *  3. a carteira e sempre lida com `SELECT ... FOR UPDATE`;
 *  4. o lock e por wallet_id: carteiras diferentes nao se bloqueiam;
 *  5. a outbox e gravada na MESMA transacao SQL - SQS nunca e tocado aqui;
 *  6. REJECTED e COMMITADO (nunca rollback): fica auditavel e idempotente.
 */
export class ProcessWagerTransactionUseCase {
  constructor(private readonly dataSource: DataSource) {}

  async execute(input: WagerRequest, options: ProcessOptions = {}): Promise<ProcessResult> {
    const request = this.validate(input);
    const payloadHash = canonicalPayloadHash(request);
    const iniciadoEm = Date.now();

    const resultado = await withTransaction(this.dataSource, (qr) =>
      this.run(qr, request, payloadHash, options),
    );

    this.registrar(resultado, request, options, Date.now() - iniciadoEm);

    return resultado;
  }

  /**
   * Log estruturado + metricas da decisao, em um lugar so.
   *
   * HTTP, fila e worker de PENDING_REFERENCE passam todos por aqui, entao nenhuma
   * entrada fica sem observabilidade. O log identifica a transacao
   * (correlationId, messageId, transactionId, walletId, providerId) e NAO carrega
   * payload financeiro nem dado sensivel.
   */
  private registrar(
    resultado: ProcessResult,
    request: WagerRequest,
    options: ProcessOptions,
    duracaoMs: number,
  ): void {
    metrics.observeLatency('wager_processing_latency', duracaoMs, { status: resultado.status });

    // O reprocessamento do worker nao cria transacao nova: ela ja foi contada
    // quando chegou. Das tentativas cuida a metrica do proprio worker.
    if (!options.retryPendingReference) {
      metrics.increment('wager_transactions_total', { status: resultado.status });
    }

    // Duplicata detectada: replay da mesma Idempotency-Key (http) ou redelivery
    // da mesma mensagem do SQS (sqs).
    if (resultado.idempotentReplay) {
      metrics.increment('wager_duplicates_total', { source: 'http' });
    }
    if (resultado.duplicateMessage) {
      metrics.increment('wager_duplicates_total', { source: 'sqs' });
    }

    const contexto = {
      transactionId: resultado.transactionId,
      status: resultado.status,
      kind: request.kind,
      walletId: request.walletId,
      playerId: request.playerId,
      providerId: request.providerId,
      externalTransactionId: request.externalTransactionId,
      failureCode: resultado.failureCode,
      idempotentReplay: resultado.idempotentReplay,
      correlationId: options.correlationId ?? null,
      messageId: options.inbox?.messageId ?? null,
      durationMs: duracaoMs,
    };

    if (resultado.failureCode) {
      logger.warn('wager_transaction_decided', contexto);
      return;
    }

    logger.info('wager_transaction_decided', contexto);
  }

  /**
   * Reprocessa uma linha parada em PENDING_REFERENCE.
   * Usado pelo worker; o worker nao precisa conhecer as regras de negocio.
   */
  async retryPendingReference(transactionId: string): Promise<ProcessResult | null> {
    const linhas: WagerTransactionRow[] = await this.dataSource.query(
      `SELECT * FROM wager_transactions WHERE id = $1`,
      [transactionId],
    );
    const row = linhas[0];

    if (!row || row.status !== WagerStatus.PendingReference) {
      return null;
    }

    return this.execute(
      {
        providerId: row.provider_id,
        externalTransactionId: row.external_transaction_id,
        playerId: row.player_id,
        walletId: row.wallet_id,
        roundId: row.round_id,
        gameId: row.game_id,
        kind: row.kind as WagerKind,
        money: { amount: row.amount, currency: row.currency },
        referenceExternalTransactionId: row.reference_external_transaction_id,
        idempotencyKey: row.idempotency_key,
      },
      { retryPendingReference: true },
    );
  }

  /* --------------------------------------------------------------- interno */

  private async run(
    qr: QueryRunner,
    request: WagerRequest,
    payloadHash: string,
    options: ProcessOptions,
  ): Promise<ProcessResult> {
    // ---- 1. Inbox: deduplicacao persistente (somente entrada por SQS) ----
    if (options.inbox) {
      const jaProcessada = await this.registrarInbox(qr, options.inbox);
      if (jaProcessada) {
        const resultado = await this.resultadoDeReplayPorKey(qr, request);
        return { ...resultado, duplicateMessage: true };
      }
    }

    // ---- 2. Trava pessimista da carteira (granularidade = wallet_id) ----
    const wallet = await this.lockWallet(qr, request.walletId);

    // ---- 3. Idempotencia persistente ----
    const existente = await this.encontrarExistente(qr, request);

    let transaction: WagerTransaction;
    let isNew: boolean;

    if (existente) {
      if (!existente.matchesPayload(payloadHash)) {
        // A mesma key com payload diferente e um conflito de idempotencia.
        // Ja a mesma transacao do provedor reenviada com OUTRA key e um
        // conflito de identidade: o provedor esta dizendo que aquela
        // transacao e outra coisa. Sao situacoes diferentes para quem
        // chamou, entao cada uma tem o seu codigo.
        const mesmaKey = existente.idempotencyKey === request.idempotencyKey;

        throw new ConflictError(
          mesmaKey
            ? `Idempotency-Key "${request.idempotencyKey}" ja foi usada com um payload diferente.`
            : `a transacao ${request.externalTransactionId} do provider ${request.providerId} ja existe com outro conteudo.`,
          mesmaKey ? 'IDEMPOTENCY_PAYLOAD_MISMATCH' : 'EXTERNAL_TRANSACTION_CONFLICT',
          {
            idempotencyKey: request.idempotencyKey,
            externalTransactionId: request.externalTransactionId,
            providerId: request.providerId,
            transactionId: existente.id,
          },
        );
      }

      const podeRetentar = options.retryPendingReference === true && existente.status === WagerStatus.PendingReference;
      if (!podeRetentar) {
        return this.resultadoDeReplay(qr, wallet, existente);
      }

      transaction = existente;
      isNew = false;
    } else {
      transaction = WagerTransaction.create({
        id: randomUUID(),
        providerId: request.providerId,
        externalTransactionId: request.externalTransactionId,
        idempotencyKey: request.idempotencyKey,
        payloadHash,
        playerId: request.playerId,
        walletId: request.walletId,
        roundId: request.roundId,
        gameId: request.gameId,
        kind: request.kind,
        money: Money.from(request.money),
        referenceExternalTransactionId: request.referenceExternalTransactionId ?? null,
      });
      isNew = true;
    }

    // ---- 4. Moeda da operacao tem que ser a da carteira ----
    if (request.money.currency !== wallet.currency) {
      throw new CurrencyMismatchError(wallet.currency, request.money.currency);
    }

    // ---- 5. Resolucao da referencia (REFUND / ROLLBACK) ----
    let reference: WagerTransaction | null = null;

    if (transaction.requiresReference()) {
      reference = await this.findByExternalId(qr, request.providerId, transaction.referenceExternalTransactionId!);

      // Referencia ausente ou ainda nao aplicada -> 202 e o worker assume.
      if (
        reference === null ||
        reference.status === WagerStatus.Pending ||
        reference.status === WagerStatus.PendingReference
      ) {
        return this.aguardarReferencia(qr, transaction, wallet, isNew, options);
      }

      const falha = transaction.checkReference(reference);
      if (falha) {
        return this.rejeitar(qr, transaction, wallet, falha, isNew, options);
      }

      const jaRevertida = await this.jaRevertida(
        qr,
        request.providerId,
        transaction.referenceExternalTransactionId!,
        transaction.id,
      );
      if (jaRevertida) {
        return this.rejeitar(qr, transaction, wallet, FailureCode.ALREADY_REVERSED, isNew, options);
      }
    }

    // ---- 6. Efeito no saldo (LOSS nao mexe em saldo nenhum) ----
    const direction = transaction.ledgerDirectionFor(reference ?? undefined);
    const agora = new Date();

    let saldoAnterior: Money | null = null;
    let carteiraAtualizada: Wallet = wallet;

    if (direction !== null) {
      try {
        saldoAnterior =
          direction === LedgerDirection.Credit
            ? carteiraAtualizada.credit(transaction.money, agora)
            : carteiraAtualizada.debit(
                transaction.money,
                agora,
                transaction.kind === WagerKind.Rollback
                  ? FailureCode.INSUFFICIENT_FUNDS_FOR_REVERSAL
                  : FailureCode.INSUFFICIENT_FUNDS,
              );
      } catch (erro) {
        if (erro instanceof BusinessRuleError) {
          return this.rejeitar(qr, transaction, wallet, erro.failureCode, isNew, options);
        }
        throw erro;
      }
    }

    // ---- 7. Persistencia: transacao -> ledger -> carteira -> outbox ----
    transaction.markProcessed(reference?.id ?? null, agora);
    await this.persistir(qr, transaction, isNew);

    if (direction !== null && saldoAnterior !== null) {
      // `create` valida a aritmetica do lancamento antes de gravar; o trigger
      // do banco confere de novo contra o proprio extrato.
      const lancamento = WalletLedgerEntry.create({
        id: randomUUID(),
        walletId: carteiraAtualizada.id,
        transactionId: transaction.id,
        direction,
        money: transaction.money,
        balanceBefore: saldoAnterior,
        balanceAfter: carteiraAtualizada.balance,
        createdAt: agora,
      });

      await insertLedgerEntry(qr, lancamento);
      await updateWallet(qr, carteiraAtualizada);
    }

    await enqueueEvent(
      qr,
      WagerTransactionProcessed.from(
        {
          transactionId: transaction.id,
          providerId: transaction.providerId,
          externalTransactionId: transaction.externalTransactionId,
          playerId: transaction.playerId,
          walletId: transaction.walletId,
          kind: transaction.kind,
          status: WagerStatus.Processed,
          money: transaction.money.toJSON(),
          balanceAfter: carteiraAtualizada.balance.toJSON(),
          balanceChanged: direction !== null,
        },
        this.eventContext(transaction.id, options),
      ),
    );

    // WalletBalanceChanged SO quando o saldo muda de verdade.
    if (direction !== null && saldoAnterior !== null) {
      await enqueueEvent(
        qr,
        WalletBalanceChanged.from(
          {
            walletId: carteiraAtualizada.id,
            transactionId: transaction.id,
            direction,
            money: transaction.money.toJSON(),
            balanceBefore: saldoAnterior.toJSON(),
            balanceAfter: carteiraAtualizada.balance.toJSON(),
            walletVersion: carteiraAtualizada.version,
          },
          this.eventContext(carteiraAtualizada.id, options),
        ),
      );
    }

    await this.marcarInboxProcessada(qr, options.inbox);

    return {
      transactionId: transaction.id,
      status: WagerStatus.Processed,
      money: transaction.money.toJSON(),
      balance: carteiraAtualizada.balance.toJSON(),
      failureCode: null,
      idempotentReplay: false,
    };
  }

  /* ------------------------------------------------------------- decisoes */

  /**
   * Referencia ainda nao disponivel.
   *
   * Primeira submissao: grava PENDING_REFERENCE (HTTP 202) e devolve a bola
   * para o worker. Reprocessamento: conta a tentativa; passando do limite,
   * REJECTED com REFERENCE_NOT_FOUND_TIMEOUT (auditavel, sem mexer em saldo).
   */
  private async aguardarReferencia(
    qr: QueryRunner,
    transaction: WagerTransaction,
    wallet: Wallet,
    isNew: boolean,
    options: ProcessOptions,
  ): Promise<ProcessResult> {
    const agora = new Date();

    if (!isNew) {
      const tentativas = transaction.retryCount + 1;

      if (tentativas > config.workers.pendingReferenceMaxAttempts) {
        return this.rejeitar(qr, transaction, wallet, FailureCode.REFERENCE_NOT_FOUND_TIMEOUT, false, options);
      }

      const atraso = config.workers.pendingReferenceBaseDelayMs * 2 ** tentativas;
      const proxima = new Date(agora.getTime() + atraso);

      transaction.reagendar(tentativas, proxima);
      await this.persistir(qr, transaction, isNew);

      return this.resultadoPendente(qr, transaction, wallet, options, proxima);
    }

    const proxima = new Date(agora.getTime() + config.workers.pendingReferenceBaseDelayMs * 2);
    transaction.markPendingReference(proxima);
    await this.persistir(qr, transaction, isNew);

    return this.resultadoPendente(qr, transaction, wallet, options, proxima);
  }

  /**
   * Grava o REJECTED e COMMITA.
   *
   * O rollback da transacao SQL NAO acontece: a decisao precisa ficar
   * auditavel e o replay futuro da mesma key precisa devolver o mesmo
   * resultado, sem mexer em saldo, version ou ledger.
   */
  private async rejeitar(
    qr: QueryRunner,
    transaction: WagerTransaction,
    wallet: Wallet,
    failureCode: FailureCode,
    isNew: boolean,
    options: ProcessOptions,
  ): Promise<ProcessResult> {
    transaction.reject(failureCode);
    await this.persistir(qr, transaction, isNew);

    await enqueueEvent(
      qr,
      WagerTransactionRejected.from(
        {
          transactionId: transaction.id,
          providerId: transaction.providerId,
          externalTransactionId: transaction.externalTransactionId,
          playerId: transaction.playerId,
          walletId: transaction.walletId,
          kind: transaction.kind,
          status: WagerStatus.Rejected,
          money: transaction.money.toJSON(),
          failureCode,
          balance: wallet.balance.toJSON(),
        },
        this.eventContext(transaction.id, options),
      ),
    );

    await this.marcarInboxProcessada(qr, options.inbox);

    return {
      transactionId: transaction.id,
      status: WagerStatus.Rejected,
      money: transaction.money.toJSON(),
      balance: wallet.balance.toJSON(),
      failureCode,
      idempotentReplay: false,
    };
  }

  private async resultadoPendente(
    qr: QueryRunner,
    transaction: WagerTransaction,
    wallet: Wallet,
    options: ProcessOptions,
    proximaTentativa: Date,
  ): Promise<ProcessResult> {
    await enqueueEvent(
      qr,
      WagerTransactionPendingReference.from(
        {
          transactionId: transaction.id,
          providerId: transaction.providerId,
          externalTransactionId: transaction.externalTransactionId,
          referenceExternalTransactionId: transaction.referenceExternalTransactionId!,
          playerId: transaction.playerId,
          walletId: transaction.walletId,
          kind: transaction.kind,
          status: WagerStatus.PendingReference,
          money: transaction.money.toJSON(),
          nextRetryAt: proximaTentativa.toISOString(),
        },
        this.eventContext(transaction.id, options),
      ),
    );

    await this.marcarInboxProcessada(qr, options.inbox);

    return {
      transactionId: transaction.id,
      status: WagerStatus.PendingReference,
      money: transaction.money.toJSON(),
      balance: wallet.balance.toJSON(),
      failureCode: null,
      idempotentReplay: false,
    };
  }

  /**
   * Replay idempotente: devolve exatamente o resultado historico.
   *
   * O saldo reportado e o `balance_after` do lancamento criado por esta
   * transacao (nao o saldo atual, que pode ter mudado depois). Quando nao
   * existe lancamento (LOSS/REJECTED), o saldo atual esta correto.
   */
  private async resultadoDeReplay(qr: QueryRunner, wallet: Wallet, transaction: WagerTransaction): Promise<ProcessResult> {
    const lancamentos: Array<{ balance_after: string }> = await qr.query(
      `SELECT balance_after FROM wallet_ledger_entries WHERE wager_transaction_id = $1`,
      [transaction.id],
    );

    const balance =
      lancamentos[0] !== undefined
        ? Money.from({ amount: lancamentos[0].balance_after, currency: transaction.money.currency })
        : wallet.balance;

    return {
      transactionId: transaction.id,
      status: transaction.status,
      money: transaction.money.toJSON(),
      balance: balance.toJSON(),
      failureCode: transaction.failureCode,
      idempotentReplay: true,
    };
  }

  private async resultadoDeReplayPorKey(qr: QueryRunner, request: WagerRequest): Promise<ProcessResult> {
    const transaction = await this.encontrarExistente(qr, request);

    if (!transaction) {
      // Nao deveria acontecer: inbox e transacao commitam juntas.
      throw new NotFoundError(
        `transacao da idempotency key ${request.idempotencyKey} nao encontrada`,
      );
    }

    const wallet = await this.lockWallet(qr, transaction.walletId);
    return this.resultadoDeReplay(qr, wallet, transaction);
  }

  /* ----------------------------------------------------------- persistencia */

  private async lockWallet(qr: QueryRunner, walletId: string): Promise<Wallet> {
    // A linha fica travada ate o commit: e isso que serializa quem mexe NESTA
    // carteira. Carteiras diferentes usam linhas diferentes e nao se bloqueiam.
    const linhas: WalletRow[] = await qr.query(
      `SELECT id, player_id, currency, balance_amount, version, created_at, updated_at
         FROM wallets
        WHERE id = $1
        FOR UPDATE`,
      [walletId],
    );

    const row = linhas[0];
    if (!row) {
      throw new NotFoundError(`carteira ${walletId} nao encontrada`, { walletId });
    }

    return toWallet(row);
  }

  private async findByIdempotencyKey(qr: QueryRunner, idempotencyKey: string): Promise<WagerTransaction | null> {
    const linhas: WagerTransactionRow[] = await qr.query(
      `SELECT * FROM wager_transactions WHERE idempotency_key = $1`,
      [idempotencyKey],
    );

    return linhas[0] ? toWagerTransaction(linhas[0]) : null;
  }

  private async findByExternalId(
    qr: QueryRunner,
    providerId: string,
    externalTransactionId: string,
  ): Promise<WagerTransaction | null> {
    const linhas: WagerTransactionRow[] = await qr.query(
      `SELECT * FROM wager_transactions WHERE provider_id = $1 AND external_transaction_id = $2`,
      [providerId, externalTransactionId],
    );

    return linhas[0] ? toWagerTransaction(linhas[0]) : null;
  }

  /**
   * Ja existe uma transacao para esta requisicao?
   *
   * Duas chaves de busca, nesta ordem:
   *  1. `idempotency_key` — a chave que o chamador mandou;
   *  2. `(provider_id, external_transaction_id)` — a identidade natural da
   *     transacao, que o schema tambem torna unica.
   *
   * A segunda existe porque provedor que gera uma key nova a cada tentativa nao
   * pode receber 500: payload identico e a MESMA operacao (replay), payload
   * diferente e conflito (`EXTERNAL_TRANSACTION_CONFLICT`) — nunca um caminho
   * novo para um id de transacao do provedor que ja foi usado.
   */
  private async encontrarExistente(qr: QueryRunner, request: WagerRequest): Promise<WagerTransaction | null> {
    const porKey = await this.findByIdempotencyKey(qr, request.idempotencyKey);

    if (porKey) {
      return porKey;
    }

    return this.findByExternalId(qr, request.providerId, request.externalTransactionId);
  }

  /**
   * Insere ou atualiza a transacao.
   *
   * O insert deixa a unicidade com o banco: quem perde a corrida (duas
   * requisicoes concorrentes com a mesma key, ou com o mesmo par
   * provider/external id) recebe 409, e nao um 500 de constraint.
   */
  private async persistir(qr: QueryRunner, transaction: WagerTransaction, isNew: boolean): Promise<void> {
    if (!isNew) {
      await updateTransaction(qr, transaction);
      return;
    }

    try {
      // O INSERT precisa de savepoint porque uma violacao de unicidade aborta a
      // transacao corrente: sem voltar o savepoint, nem a consulta usada para
      // rotular o conflito consegue rodar (`25P02`).
      await qr.query('SAVEPOINT inserir_transacao');
      await insertTransaction(qr, transaction);
    } catch (erro) {
      if (!isUniqueViolation(erro)) {
        throw erro;
      }

      const restricao = uniqueViolationConstraint(erro);
      await qr.query('ROLLBACK TO SAVEPOINT inserir_transacao');

      // O mesmo INSERT pode violar os dois indices (mesma key E mesmo par
      // provider/external id) e o Postgres reporta apenas um deles. Quem decide
      // o rotulo e o dado, nao o indice que o banco escolheu: mesma key
      // continua sendo conflito de idempotencia, igual ao caminho sequencial.
      const vencedora =
        restricao === 'uq_wager_provider_external'
          ? await this.findByExternalId(qr, transaction.providerId, transaction.externalTransactionId)
          : null;

      const porKey =
        restricao === 'uq_wager_idempotency_key' || vencedora?.idempotencyKey === transaction.idempotencyKey;

      throw new ConflictError(
        porKey
          ? `Idempotency-Key "${transaction.idempotencyKey}" ja foi usada por outra requisicao concorrente.`
          : `a transacao ${transaction.externalTransactionId} do provider ${transaction.providerId} ja existe com outro conteudo.`,
        porKey ? 'IDEMPOTENCY_PAYLOAD_MISMATCH' : 'EXTERNAL_TRANSACTION_CONFLICT',
        {
          idempotencyKey: transaction.idempotencyKey,
          providerId: transaction.providerId,
          externalTransactionId: transaction.externalTransactionId,
        },
      );
    }
  }

  private async jaRevertida(
    qr: QueryRunner,
    providerId: string,
    referenceExternalTransactionId: string,
    transactionId: string,
  ): Promise<boolean> {
    // O indice unico parcial `uq_single_processed_reversal` garante o mesmo
    // no banco; aqui evitamos gravar um REJECTED que seria constrangimento.
    const linhas: unknown[] = await qr.query(
      `SELECT 1
         FROM wager_transactions
        WHERE provider_id = $1
          AND reference_external_transaction_id = $2
          AND status = 'PROCESSED'
          AND kind IN ('REFUND', 'ROLLBACK')
          AND id <> $3
        LIMIT 1`,
      [providerId, referenceExternalTransactionId, transactionId],
    );

    return linhas.length > 0;
  }

  private async registrarInbox(qr: QueryRunner, inbox: InboxContext): Promise<boolean> {
    const inseridos: Array<{ id: string }> = await qr.query(
      `INSERT INTO inbox_messages (id, consumer_name, message_id, payload_hash)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (consumer_name, message_id) DO NOTHING
       RETURNING id`,
      [randomUUID(), inbox.consumerName, inbox.messageId, inbox.payloadHash],
    );

    if (inseridos.length > 0) {
      return false; // primeira vez que esta mensagem aparece
    }

    const existentes: Array<{ payload_hash: string; processed_at: Date | null }> = await qr.query(
      `SELECT payload_hash, processed_at FROM inbox_messages WHERE consumer_name = $1 AND message_id = $2`,
      [inbox.consumerName, inbox.messageId],
    );

    const existente = existentes[0];

    if (existente && existente.payload_hash !== inbox.payloadHash) {
      throw new ConflictError(
        `mensagem ${inbox.messageId} ja foi consumida com outro conteudo.`,
        'INBOX_PAYLOAD_MISMATCH',
        { messageId: inbox.messageId },
      );
    }

    return existente?.processed_at !== null && existente?.processed_at !== undefined;
  }

  private async marcarInboxProcessada(qr: QueryRunner, inbox?: InboxContext): Promise<void> {
    if (!inbox) {
      return;
    }

    await qr.query(
      `UPDATE inbox_messages SET processed_at = NOW() WHERE consumer_name = $1 AND message_id = $2`,
      [inbox.consumerName, inbox.messageId],
    );
  }

  private eventContext(aggregateId: string, options: ProcessOptions): EventContext {
    return { aggregateId, correlationId: options.correlationId ?? null };
  }

  /* ------------------------------------------------------------- validacao */

  /** Validacao semantica da entrada (erros 400). */
  private validate(input: WagerRequest): WagerRequest {
    const obrigatorios = [
      'providerId',
      'externalTransactionId',
      'playerId',
      'walletId',
      'roundId',
      'gameId',
      'idempotencyKey',
    ] as const;

    for (const campo of obrigatorios) {
      const valor = input[campo];
      if (typeof valor !== 'string' || valor.trim().length === 0) {
        throw new ValidationError(`campo "${campo}" e obrigatorio`, { field: campo });
      }
    }

    if (!KINDS_DA_API.includes(input.kind)) {
      if (input.kind === WagerKind.Opening) {
        throw new OpeningNotAllowedError({ kind: input.kind });
      }
      throw new ValidationError(`kind invalido: ${JSON.stringify(input.kind)}`, {
        kind: input.kind,
        aceitos: KINDS_DA_API,
      });
    }

    // A validacao do formato do dinheiro acontece aqui: "25.0", "-10.00",
    // "NaN" e afins viram 400 sem tocar no banco.
    const money = Money.from(input.money);

    if (!money.isPositive()) {
      throw new ValidationError('money.amount deve ser maior que zero', { amount: input.money.amount });
    }

    return {
      ...input,
      money: money.toJSON(),
      referenceExternalTransactionId: input.referenceExternalTransactionId ?? null,
    };
  }
}