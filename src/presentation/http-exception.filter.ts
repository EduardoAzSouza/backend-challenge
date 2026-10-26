import { Catch, HttpException, HttpStatus, type ArgumentsHost, type ExceptionFilter } from '@nestjs/common';
import type { Request, Response } from 'express';

import { DomainError } from '../domain/errors';
import { logger } from '../infrastructure/observability/logger';
import { metrics } from '../infrastructure/observability/metrics';

/** Codigos de erro do PostgreSQL que indicam disputa de lock. */
const CODIGOS_DE_LOCK = ['40001', '40P01'];

/**
 * Traduz erro -> HTTP em um lugar so.
 *
 * O status vem do proprio erro de dominio (`httpStatus`), garantindo que
 * payload invalido, conflito de idempotencia, rejeicao de negocio, aceite
 * pendente e falha de infraestrutura sejam distinguiveis entre si.
 */
@Catch()
export class HttpExceptionFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const contexto = host.switchToHttp();
    const response = contexto.getResponse<Response>();
    const request = contexto.getRequest<Request>();
    const correlationId = (request.headers['x-correlation-id'] as string | undefined) ?? null;

    if (exception instanceof DomainError) {
      response.status(exception.httpStatus).json({
        error: exception.code,
        message: exception.message,
        details: exception.details,
        correlationId,
      });
      return;
    }

    if (exception instanceof HttpException) {
      response.status(exception.getStatus()).json({
        error: 'HTTP_ERROR',
        message: exception.message,
        correlationId,
      });
      return;
    }

    const erro = exception as Error;
    const codigo = (erro as unknown as { code?: string }).code;

    if (typeof codigo === 'string' && CODIGOS_DE_LOCK.includes(codigo)) {
      // Disputa de lock: vale medir, porque indica contencao real em carteira quente.
      metrics.increment('wallet_lock_conflicts_total', { code: codigo });
    }

    logger.error('unhandled_error', {
      method: request.method,
      url: request.url,
      error: erro?.message,
      code: codigo,
    });

    response.status(HttpStatus.INTERNAL_SERVER_ERROR).json({
      error: 'INTERNAL_ERROR',
      message: 'erro interno inesperado',
      correlationId,
    });
  }
}