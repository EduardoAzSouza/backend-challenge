import { Body, Controller, Get, HttpCode, Ip, Param, Post, Query, UseGuards } from '@nestjs/common';

import { WalletUseCases, type ReconciliationView, type WalletView } from '../application/wallet.use-cases';
import { ValidationError } from '../domain/errors';
import type { MoneyProps } from '../domain/money';
import { ProviderIdentityGuard } from '../infrastructure/auth/provider-identity.guard';
import { logger } from '../infrastructure/observability/logger';

interface CriarCarteiraBody {
  playerId?: string;
  initialBalance?: MoneyProps;
}

@Controller('wallets')
@UseGuards(ProviderIdentityGuard)
export class WalletsController {
  constructor(private readonly wallets: WalletUseCases) {}

  /** POST /wallets - 201 na criacao, 409 quando ja existe (playerId + currency). */
  @Post()
  async criar(@Body() body: CriarCarteiraBody, @Ip() ip: string): Promise<WalletView> {
    if (!body.initialBalance) {
      throw new ValidationError('initialBalance e obrigatorio (amount + currency)');
    }

    const wallet = await this.wallets.createWallet({
      playerId: String(body.playerId ?? ''),
      initialBalance: body.initialBalance,
    });

    logger.info('wallet_request_created', { walletId: wallet.id, playerId: wallet.playerId, origin: ip });

    return wallet;
  }

  @Get(':walletId')
  async buscar(@Param('walletId') walletId: string): Promise<WalletView> {
    return this.wallets.getWallet(walletId);
  }

  /** GET /wallets/:walletId/ledger?cursor=...&limit=50 (cursor opaco). */
  @Get(':walletId/ledger')
  async extrato(
    @Param('walletId') walletId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ) {
    const tamanho = limit === undefined ? undefined : Number(limit);

    if (tamanho !== undefined && (!Number.isInteger(tamanho) || tamanho < 1)) {
      throw new ValidationError('limit deve ser um inteiro positivo', { limit });
    }

    return this.wallets.getLedger(walletId, cursor, tamanho);
  }

  /** POST /wallets/:walletId/reconciliation - confere saldo materializado x extrato. */
  @Post(':walletId/reconciliation')
  @HttpCode(200)
  async reconciliar(@Param('walletId') walletId: string): Promise<ReconciliationView> {
    return this.wallets.reconcileWallet(walletId);
  }
}