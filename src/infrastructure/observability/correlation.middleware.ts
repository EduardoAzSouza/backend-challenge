import { Injectable, type NestMiddleware } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import type { NextFunction, Request, Response } from 'express';

/**
 * Propaga um `correlationId` por toda a requisicao.
 *
 * Se o cliente enviar `X-Correlation-Id`, ele e respeitado (isso permite
 * correlacionar a requisicao com a chamada do provedor); caso contrario,
 * geramos um e devolvemos no header da resposta.
 */
@Injectable()
export class CorrelationMiddleware implements NestMiddleware {
  use(request: Request, response: Response, next: NextFunction): void {
    const recebido = request.headers['x-correlation-id'];
    const correlationId = typeof recebido === 'string' && recebido.trim().length > 0 ? recebido : randomUUID();

    request.headers['x-correlation-id'] = correlationId;
    response.setHeader('x-correlation-id', correlationId);

    next();
  }
}