import { entityKind } from 'drizzle-orm/entity';
import type { Logger } from 'drizzle-orm/logger';
import { NoopLogger } from 'drizzle-orm/logger';
import { PgTransaction } from 'drizzle-orm/pg-core';
import type { SelectedFieldsOrdered } from 'drizzle-orm/pg-core/query-builders/select.types';
import type {
  PgTransactionConfig,
  PreparedQueryConfig,
  PgQueryResultHKT,
} from 'drizzle-orm/pg-core/session';
import { PgPreparedQuery, PgSession } from 'drizzle-orm/pg-core/session';
import type {
  RelationalSchemaConfig,
  TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { fillPlaceholders, type Query, SQL, sql } from 'drizzle-orm/sql/sql';
import type { Assume } from 'drizzle-orm/utils';
import { mapResultRow } from './sql/result-mapper.ts';
import { TransactionRollbackError } from 'drizzle-orm/errors';
import type { DatabendDialect } from './dialect.ts';
import type {
  DatabendClientLike,
  DatabendConnectionPool,
  RowData,
} from './client.ts';
import {
  executeArraysOnClient,
  executeOnClient,
  prepareParams,
  isPool,
} from './client.ts';
import type { Connection } from 'databend-driver';

export type { DatabendClientLike, RowData } from './client.ts';

export class DatabendPreparedQuery<
  T extends PreparedQueryConfig,
> extends PgPreparedQuery<T> {
  static readonly [entityKind]: string = 'DatabendPreparedQuery';

  constructor(
    private client: DatabendClientLike,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: SelectedFieldsOrdered | undefined,
    private _isResponseInArrayMode: boolean,
    private customResultMapper:
      | ((rows: unknown[][]) => T['execute'])
      | undefined
  ) {
    super({ sql: queryString, params });
  }

  async execute(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T['execute']> {
    const params = prepareParams(
      fillPlaceholders(this.params, placeholderValues)
    );
    this.logger.logQuery(this.queryString, params);

    const { fields, joinsNotNullableMap, customResultMapper } =
      this as typeof this & { joinsNotNullableMap?: Record<string, boolean> };

    if (fields) {
      const { rows } = await executeArraysOnClient(
        this.client,
        this.queryString,
        params
      );

      if (rows.length === 0) {
        return [] as T['execute'];
      }

      return customResultMapper
        ? customResultMapper(rows)
        : rows.map((row) =>
            mapResultRow<T['execute']>(fields, row, joinsNotNullableMap)
          );
    }

    const rows = await executeOnClient(this.client, this.queryString, params);

    return rows as T['execute'];
  }

  all(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<T['all']> {
    return this.execute(placeholderValues);
  }

  isResponseInArrayMode(): boolean {
    return this._isResponseInArrayMode;
  }
}

export interface DatabendSessionOptions {
  logger?: Logger;
}

export class DatabendSession<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> extends PgSession<DatabendQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DatabendSession';

  protected override dialect: DatabendDialect;
  private logger: Logger;
  private rollbackOnly = false;

  constructor(
    private client: DatabendClientLike,
    dialect: DatabendDialect,
    private schema: RelationalSchemaConfig<TSchema> | undefined,
    private options: DatabendSessionOptions = {}
  ) {
    super(dialect);
    this.dialect = dialect;
    this.logger = options.logger ?? new NoopLogger();
  }

  prepareQuery<T extends PreparedQueryConfig = PreparedQueryConfig>(
    query: Query,
    fields: SelectedFieldsOrdered | undefined,
    name: string | undefined,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][]) => T['execute']
  ): PgPreparedQuery<T> {
    void name;
    return new DatabendPreparedQuery(
      this.client,
      query.sql,
      query.params,
      this.logger,
      fields,
      isResponseInArrayMode,
      customResultMapper
    );
  }

  override async transaction<T>(
    transaction: (tx: DatabendTransaction<TFullSchema, TSchema>) => Promise<T>,
    config?: PgTransactionConfig
  ): Promise<T> {
    let pinnedConnection: Connection | undefined;
    let pool: DatabendConnectionPool | undefined;

    let clientForTx: DatabendClientLike = this.client;
    if (isPool(this.client)) {
      pool = this.client;
      pinnedConnection = await pool.acquire();
      clientForTx = pinnedConnection;
    }

    const session = new DatabendSession(
      clientForTx,
      this.dialect,
      this.schema,
      this.options
    );

    const tx = new DatabendTransaction<TFullSchema, TSchema>(
      this.dialect,
      session,
      this.schema
    );

    try {
      await tx.execute(sql`BEGIN`);

      if (config) {
        await tx.setTransaction(config);
      }

      try {
        const result = await transaction(tx);
        if (session.isRollbackOnly()) {
          await tx.execute(sql`ROLLBACK`);
          throw new TransactionRollbackError();
        }
        await tx.execute(sql`COMMIT`);
        return result;
      } catch (error) {
        await tx.execute(sql`ROLLBACK`);
        throw error;
      }
    } finally {
      if (pinnedConnection && pool) {
        await pool.release(pinnedConnection);
      }
    }
  }

  markRollbackOnly(): void {
    this.rollbackOnly = true;
  }

  isRollbackOnly(): boolean {
    return this.rollbackOnly;
  }
}

type DatabendTransactionInternals<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> = {
  dialect: DatabendDialect;
  session: DatabendSession<TFullSchema, TSchema>;
};

type DatabendTransactionWithInternals<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig = Record<string, never>,
> = DatabendTransactionInternals<TFullSchema, TSchema> &
  DatabendTransaction<TFullSchema, TSchema>;

const VALID_TRANSACTION_ISOLATION_LEVELS = new Set<string>([
  'read uncommitted',
  'read committed',
  'repeatable read',
  'serializable',
]);

const VALID_TRANSACTION_ACCESS_MODES = new Set<string>([
  'read only',
  'read write',
]);

export class DatabendTransaction<
  TFullSchema extends Record<string, unknown>,
  TSchema extends TablesRelationalConfig,
> extends PgTransaction<DatabendQueryResultHKT, TFullSchema, TSchema> {
  static readonly [entityKind]: string = 'DatabendTransaction';

  rollback(): never {
    throw new TransactionRollbackError();
  }

  getTransactionConfigSQL(config: PgTransactionConfig): SQL {
    if (
      config.isolationLevel &&
      !VALID_TRANSACTION_ISOLATION_LEVELS.has(config.isolationLevel)
    ) {
      throw new Error(
        `Invalid transaction isolation level "${config.isolationLevel}". Expected one of: ${Array.from(
          VALID_TRANSACTION_ISOLATION_LEVELS
        ).join(', ')}.`
      );
    }

    if (
      config.accessMode &&
      !VALID_TRANSACTION_ACCESS_MODES.has(config.accessMode)
    ) {
      throw new Error(
        `Invalid transaction access mode "${config.accessMode}". Expected one of: ${Array.from(
          VALID_TRANSACTION_ACCESS_MODES
        ).join(', ')}.`
      );
    }

    const chunks: string[] = [];
    if (config.isolationLevel) {
      chunks.push(`isolation level ${config.isolationLevel}`);
    }
    if (config.accessMode) {
      chunks.push(config.accessMode);
    }
    return sql.raw(chunks.join(' '));
  }

  setTransaction(config: PgTransactionConfig): Promise<void> {
    type Tx = DatabendTransactionWithInternals<TFullSchema, TSchema>;
    return (this as unknown as Tx).session.execute(
      sql`SET TRANSACTION ${this.getTransactionConfigSQL(config)}`
    );
  }

  override async transaction<T>(
    transaction: (tx: DatabendTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    // Databend does not support savepoints. Use rollback-only fallback.
    type Tx = DatabendTransactionWithInternals<TFullSchema, TSchema>;
    const internals = this as unknown as Tx;

    const nestedTx = new DatabendTransaction<TFullSchema, TSchema>(
      internals.dialect,
      internals.session,
      this.schema,
      this.nestedIndex + 1
    );

    return transaction(nestedTx).catch((error) => {
      (
        internals.session as DatabendSession<TFullSchema, TSchema>
      ).markRollbackOnly();
      throw error;
    });
  }
}

export type GenericRowData<T extends RowData = RowData> = T;

export type GenericTableData<T = RowData> = T[];

export interface DatabendQueryResultHKT extends PgQueryResultHKT {
  type: GenericTableData<Assume<this['row'], RowData>>;
}
