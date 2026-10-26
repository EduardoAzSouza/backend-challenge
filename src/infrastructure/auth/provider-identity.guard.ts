import { Injectable, type CanActivate, type ExecutionContext } from '@nestjs/common';

import { logger } from '../observability/logger';

/**
 * Ponto de extensao de autenticacao.
 *
 * Decisao (justificada no ARCHITECTURE.md): um IdP externo exigiria container,
 * realm, client e JWKS sem exercitar as invariantes que importam aqui.
 *
 * este guard:
 *  - nao bloqueia nada (pass-through);
 *  - registra a identidade que o provedor declarou no header
 *    `X-Provider-Identity`, para dar visibilidade do que viria do token;
 *  - NAO confia nesse header para nada: o `providerId` do payload continua
 *    passando pelas validacoes de dominio (incluindo as regras de referencia).
 *
 * Com OIDC/Keycloak, o corpo seria: validar o JWT (assinatura via JWKS,
 * issuer, audience, exp), extrair o claim `provider_id` do token JÁ validado e
 * comparar com o providerId do payload - 401 quando nao bater.
 */
@Injectable()
export class ProviderIdentityGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const request = context
      .switchToHttp()
      .getRequest<{ headers: Record<string, string | undefined>; method: string; url: string }>();

    const identidadeDeclarada = request.headers['x-provider-identity'];

    if (identidadeDeclarada) {
      logger.debug('provider_identity_declared', {
        providerIdentity: identidadeDeclarada,
        method: request.method,
        url: request.url,
      });
    }

    return true;
  }
}