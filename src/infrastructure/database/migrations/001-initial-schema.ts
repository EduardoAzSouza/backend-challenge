import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * 001 - schema inicial.
 *
 *  - saldo nunca negativo ................ CHECK (balance_amount >= 0)
 *  - uma carteira por player/moeda ....... UNIQUE (player_id, currency)
 *  - uma transacao por external id ....... UNIQUE (provider_id, external_transaction_id)
 *  - idempotencia persistente ............ UNIQUE (idempotency_key) + payload_hash
 *  - uma unica reversao ativa ............ indice unico parcial
 *  - ledger imutavel ..................... trigger que recusa UPDATE/DELETE
 *  - aritmetica do ledger ................ trigger que confere balance_before/after
 *  - um lancamento por transacao ......... UNIQUE (wager_transaction_id)
 */
export class InitialSchema1700000000000 implements MigrationInterface {
  name = 'InitialSchema1700000000000';

  public async up(queryRunner: QueryRunner): Promise<void> {
    /* ------------------------------------------------------------- wallets */
    await queryRunner.query(`
      CREATE TABLE "wallets" (
        "id"             UUID          NOT NULL,
        "player_id"      TEXT          NOT NULL,
        "currency"       CHAR(3)       NOT NULL,
        "balance_amount" NUMERIC(18,2) NOT NULL DEFAULT 0,
        "version"        INTEGER       NOT NULL DEFAULT 1,
        "created_at"     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        "updated_at"     TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT "pk_wallets" PRIMARY KEY ("id"),
        CONSTRAINT "uq_wallet_player_currency" UNIQUE ("player_id", "currency"),
        CONSTRAINT "chk_wallet_balance_non_negative" CHECK ("balance_amount" >= 0),
        CONSTRAINT "chk_wallet_version_non_negative" CHECK ("version" >= 0),
        CONSTRAINT "chk_wallet_currency_format" CHECK ("currency" ~ '^[A-Z]{3}$')
      )
    `);

    /* -------------------------------------------------- wager_transactions */
    await queryRunner.query(`
      CREATE TABLE "wager_transactions" (
        "id"                                UUID          NOT NULL,
        "provider_id"                       TEXT          NOT NULL,
        "external_transaction_id"           TEXT          NOT NULL,
        "idempotency_key"                   TEXT          NOT NULL,
        "payload_hash"                      CHAR(64)      NOT NULL,
        "player_id"                         TEXT          NOT NULL,
        "wallet_id"                         UUID          NOT NULL,
        "round_id"                          TEXT          NOT NULL,
        "game_id"                           TEXT          NOT NULL,
        "kind"                              TEXT          NOT NULL,
        "status"                            TEXT          NOT NULL,
        "amount"                            NUMERIC(18,2) NOT NULL,
        "currency"                          CHAR(3)       NOT NULL,
        "reference_external_transaction_id" TEXT,
        "reference_transaction_id"          UUID,
        "failure_code"                      TEXT,
        "retry_count"                       INTEGER       NOT NULL DEFAULT 0,
        "next_retry_at"                     TIMESTAMPTZ,
        "processed_at"                      TIMESTAMPTZ,
        "created_at"                        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        "updated_at"                        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT "pk_wager_transactions" PRIMARY KEY ("id"),
        CONSTRAINT "uq_wager_provider_external" UNIQUE ("provider_id", "external_transaction_id"),
        CONSTRAINT "uq_wager_idempotency_key" UNIQUE ("idempotency_key"),
        CONSTRAINT "fk_wager_wallet" FOREIGN KEY ("wallet_id") REFERENCES "wallets" ("id"),
        CONSTRAINT "chk_wager_amount_positive" CHECK ("amount" > 0),
        CONSTRAINT "chk_wager_retry_count_non_negative" CHECK ("retry_count" >= 0),
        CONSTRAINT "chk_wager_kind" CHECK ("kind" IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
        CONSTRAINT "chk_wager_status" CHECK ("status" IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
        CONSTRAINT "chk_wager_failure_code" CHECK (
          ("status" IN ('REJECTED','FAILED')) = ("failure_code" IS NOT NULL)
        ),
        CONSTRAINT "chk_wager_reference_required" CHECK (
          "kind" NOT IN ('REFUND','ROLLBACK') OR "reference_external_transaction_id" IS NOT NULL
        ),
        CONSTRAINT "chk_wager_pending_reference_deadline" CHECK (
          "status" <> 'PENDING_REFERENCE' OR "next_retry_at" IS NOT NULL
        )
      )
    `);

    // Uma referencia so pode ser revertida (REFUND/ROLLBACK) uma unica vez.
    await queryRunner.query(`
      CREATE UNIQUE INDEX "uq_single_processed_reversal"
        ON "wager_transactions" ("provider_id", "reference_external_transaction_id")
        WHERE "status" = 'PROCESSED' AND "kind" IN ('REFUND','ROLLBACK')
    `);

    await queryRunner.query(`
      CREATE INDEX "idx_wager_pending_reference"
        ON "wager_transactions" ("next_retry_at")
        WHERE "status" = 'PENDING_REFERENCE'
    `);

    await queryRunner.query(`
      CREATE INDEX "idx_wager_reference_lookup"
        ON "wager_transactions" ("provider_id", "reference_external_transaction_id")
    `);

    await queryRunner.query(`
      CREATE INDEX "idx_wager_wallet_created"
        ON "wager_transactions" ("wallet_id", "created_at" DESC)
    `);

    /* ------------------------------------------- wallet_ledger_entries (append-only) */
    await queryRunner.query(`
      CREATE TABLE "wallet_ledger_entries" (
        "seq"                  BIGSERIAL     NOT NULL,
        "id"                   UUID          NOT NULL,
        "wallet_id"            UUID          NOT NULL,
        "wager_transaction_id" UUID          NOT NULL,
        "direction"            TEXT          NOT NULL,
        "amount"               NUMERIC(18,2) NOT NULL,
        "currency"             CHAR(3)       NOT NULL,
        "balance_before"       NUMERIC(18,2) NOT NULL,
        "balance_after"        NUMERIC(18,2) NOT NULL,
        "created_at"           TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
        CONSTRAINT "pk_wallet_ledger_entries" PRIMARY KEY ("id"),
        CONSTRAINT "uq_ledger_seq" UNIQUE ("seq"),
        CONSTRAINT "uq_ledger_wager_transaction" UNIQUE ("wager_transaction_id"),
        CONSTRAINT "fk_ledger_wallet" FOREIGN KEY ("wallet_id") REFERENCES "wallets" ("id"),
        CONSTRAINT "fk_ledger_wager_transaction" FOREIGN KEY ("wager_transaction_id")
          REFERENCES "wager_transactions" ("id"),
        CONSTRAINT "chk_ledger_direction" CHECK ("direction" IN ('DEBIT','CREDIT')),
        CONSTRAINT "chk_ledger_amount_positive" CHECK ("amount" > 0),
        CONSTRAINT "chk_ledger_balance_before_non_negative" CHECK ("balance_before" >= 0),
        CONSTRAINT "chk_ledger_balance_after_non_negative" CHECK ("balance_after" >= 0)
      )
    `);

    await queryRunner.query(`
      CREATE INDEX "idx_ledger_wallet_seq" ON "wallet_ledger_entries" ("wallet_id", "seq" DESC)
    `);

    // Ledger imutavel: nenhum UPDATE/DELETE passa, nem por SQL manual.
    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION "fn_bloquear_mutacao_ledger"() RETURNS TRIGGER AS $$
      BEGIN
        RAISE EXCEPTION 'wallet_ledger_entries e append-only: % nao e permitido', TG_OP
          USING ERRCODE = 'restrict_violation';
      END;
      $$ LANGUAGE plpgsql
    `);

    await queryRunner.query(`
      CREATE TRIGGER "trg_immutable_ledger"
        BEFORE UPDATE OR DELETE ON "wallet_ledger_entries"
        FOR EACH ROW EXECUTE FUNCTION "fn_bloquear_mutacao_ledger"()
    `);

    // Aritmetica do ledger: o saldo tem que continuar a serie do proprio extrato.
    await queryRunner.query(`
      CREATE OR REPLACE FUNCTION "fn_conferir_aritmetica_ledger"() RETURNS TRIGGER AS $$
      DECLARE
        saldo_anterior NUMERIC(18,2);
        saldo_esperado NUMERIC(18,2);
      BEGIN
        SELECT l."balance_after" INTO saldo_anterior
          FROM "wallet_ledger_entries" l
         WHERE l."wallet_id" = NEW."wallet_id"
         ORDER BY l."seq" DESC
         LIMIT 1;

        saldo_anterior := COALESCE(saldo_anterior, 0.00);

        IF NEW."direction" = 'CREDIT' THEN
          saldo_esperado := saldo_anterior + NEW."amount";
        ELSE
          saldo_esperado := saldo_anterior - NEW."amount";
        END IF;

        IF saldo_esperado < 0 THEN
          RAISE EXCEPTION 'lancamento deixaria a carteira negativa: %, saldo %', NEW."wallet_id", saldo_esperado
            USING ERRCODE = 'check_violation';
        END IF;

        IF NEW."balance_before" <> saldo_anterior THEN
          RAISE EXCEPTION 'balance_before % nao continua o extrato (ultimo saldo %)',
            NEW."balance_before", saldo_anterior USING ERRCODE = 'check_violation';
        END IF;

        IF NEW."balance_after" <> saldo_esperado THEN
          RAISE EXCEPTION 'balance_after % nao confere com % (% %)',
            NEW."balance_after", saldo_esperado, NEW."direction", NEW."amount"
            USING ERRCODE = 'check_violation';
        END IF;

        RETURN NEW;
      END;
      $$ LANGUAGE plpgsql
    `);

    await queryRunner.query(`
      CREATE TRIGGER "trg_ledger_arithmetic"
        BEFORE INSERT ON "wallet_ledger_entries"
        FOR EACH ROW EXECUTE FUNCTION "fn_conferir_aritmetica_ledger"()
    `);

    /* ------------------------------------------------------- inbox_messages */
    await queryRunner.query(`
      CREATE TABLE "inbox_messages" (
        "id"            UUID        NOT NULL,
        "consumer_name" TEXT        NOT NULL,
        "message_id"    TEXT        NOT NULL,
        "payload_hash"  CHAR(64)    NOT NULL,
        "received_at"   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        "processed_at"  TIMESTAMPTZ,
        CONSTRAINT "pk_inbox_messages" PRIMARY KEY ("id"),
        CONSTRAINT "uq_inbox_consumer_message" UNIQUE ("consumer_name", "message_id")
      )
    `);

    /* ------------------------------------------------------ outbox_messages */
    await queryRunner.query(`
      CREATE TABLE "outbox_messages" (
        "id"              UUID        NOT NULL,
        "aggregate_id"    TEXT        NOT NULL,
        "event_type"      TEXT        NOT NULL,
        "payload"         JSONB       NOT NULL,
        "correlation_id"  TEXT,
        "occurred_at"     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        "attempts"        INTEGER     NOT NULL DEFAULT 0,
        "next_attempt_at" TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        "published_at"    TIMESTAMPTZ,
        CONSTRAINT "pk_outbox_messages" PRIMARY KEY ("id"),
        CONSTRAINT "chk_outbox_attempts_non_negative" CHECK ("attempts" >= 0)
      )
    `);

    await queryRunner.query(`
      CREATE INDEX "idx_outbox_pending"
        ON "outbox_messages" ("next_attempt_at", "occurred_at")
        WHERE "published_at" IS NULL
    `);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TRIGGER IF EXISTS "trg_ledger_arithmetic" ON "wallet_ledger_entries"`);
    await queryRunner.query(`DROP FUNCTION IF EXISTS "fn_conferir_aritmetica_ledger"`);
    await queryRunner.query(`DROP TRIGGER IF EXISTS "trg_immutable_ledger" ON "wallet_ledger_entries"`);
    await queryRunner.query(`DROP FUNCTION IF EXISTS "fn_bloquear_mutacao_ledger"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "outbox_messages"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "inbox_messages"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "wallet_ledger_entries"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "wager_transactions"`);
    await queryRunner.query(`DROP TABLE IF EXISTS "wallets"`);
  }
}