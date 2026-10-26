import { Body, Controller, Get, Headers, Param, Post, Res, UseGuards } from '@nestjs/common';
import type { Response } from 'express';

import {
  ProcessWagerTransactionUseCase,
  httpStatusForStatus,
  type ProcessResult,
} from '../application/process-wager-transaction.use-case';
import { WalletUseCases, type TransactionView } from '../application/wallet.use-cases';
import type { MoneyProps } from '../domain/money';
import { WagerKind } from '../domain/wager-transaction';
import { ProviderIdentityGuard } from '../infrastructure/auth/provider-identity.guard';

interface SubmeterBody {
  providerId?: string;
  externalTransactionId?: string;
  playerId?: string;
  walletId?: string;
  roundId?: string;
  gameId?: string;
  kind?: string;
  money?: MoneyProps;
  referenceExternalTransactionId?: string | null;
}

export interface SubmeterResposta {
  transactionId: string;
  status: string;
  balance: MoneyProps;
  failureCode: string | null;
  idempotentReplay: boolean;
}

@Controller('wagering')
@UseGuards(ProviderIdentityGuard)
export class WageringController {
  constructor(
    private readonly useCase: ProcessWagerTransactionUseCase,
    private readonly wallets: WalletUseCases,
  ) {}

  /**
   * POST /wagering/transactions
   *
   * 200 PROCESSED (inclusive replay) | 202 PENDING_REFERENCE | 422 REJECTED.
   * 400 para payload/formato de dinheiro invalido, 404 carteira inexistente e
   * 409 para Idempotency-Key reusada com outro payload.
   */
  @Post('transactions')
  async submeter(
    @Body() body: SubmeterBody,
    @Res({ passthrough: true }) response: Response,
    @Headers('idempotency-key') idempotencyKey?: string,
    @Headers('x-correlation-id') correlationId?: string,
  ): Promise<SubmeterResposta> {
    const providerId = String(body.providerId ?? '').trim();
    const externalTransactionId = String(body.externalTransactionId ?? '').trim();

    // O header e a fonte da verdade; sem ele, o default recomendado.
    const chave = (idempotencyKey ?? '').trim() || `${providerId}:${externalTransactionId}`;

    const resultado: ProcessResult = await this.useCase.execute(
      {
        providerId,
        externalTransactionId,
        playerId: String(body.playerId ?? ''),
        walletId: String(body.walletId ?? ''),
        roundId: String(body.roundId ?? ''),
        gameId: String(body.gameId ?? ''),
        kind: body.kind as WagerKind,
        money: body.money as MoneyProps,
        referenceExternalTransactionId: body.referenceExternalTransactionId ?? null,
        idempotencyKey: chave,
      },
      { correlationId: correlationId ?? null },
    );

    response.status(httpStatusForStatus(resultado.status));

    return {
      transactionId: resultado.transactionId,
      status: resultado.status,
      balance: resultado.balance,
      failureCode: resultado.failureCode,
      idempotentReplay: resultado.idempotentReplay,
    };
  }

  @Get('transactions/:transactionId')
  async buscar(@Param('transactionId') transactionId: string): Promise<TransactionView> {
    return this.wallets.getTransactionById(transactionId);
  }
}

/** GET /providers/:providerId/wagering/transactions/:externalTransactionId */
@Controller('providers')
@UseGuards(ProviderIdentityGuard)
export class ProviderTransactionsController {
  constructor(private readonly wallets: WalletUseCases) {}

  @Get(':providerId/wagering/transactions/:externalTransactionId')
  async buscar(
    @Param('providerId') providerId: string,
    @Param('externalTransactionId') externalTransactionId: string,
  ): Promise<TransactionView> {
    return this.wallets.getTransactionByExternalId(providerId, externalTransactionId);
  }
}