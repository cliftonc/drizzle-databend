import type { Connection } from 'databend-driver';
import { entityKind } from 'drizzle-orm/entity';
import { TransactionRollbackError } from 'drizzle-orm/errors';
import type { Logger } from 'drizzle-orm/logger';
import { NoopLogger } from 'drizzle-orm/logger';
import type {
  RelationalSchemaConfig,
  TablesRelationalConfig,
} from 'drizzle-orm/relations';
import { fillPlaceholders, type Query, type QueryTypingsValue, type SQL, sql } from 'drizzle-orm/sql/sql';
import type { Assume } from 'drizzle-orm/utils';
import type {
  DatabendClientLike,
  DatabendConnectionPool,
  RowData,
} from './client.ts';
import {
  executeArraysOnClient,
  executeOnClient,
  isPool,
} from './client.ts';
import type { DatabendDialect } from './databend-core/dialect.ts';
import {
  DatabendPreparedQuery as DatabendPreparedQueryBase,
  DatabendSession as DatabendSessionBase,
  DatabendTransaction as DatabendTransactionBase,
} from './databend-core/session.ts';
import { mapResultRow } from './sql/result-mapper.ts';

export type { DatabendClientLike, RowData } from './client.ts';

export interface DatabendQueryResultHKT {
  readonly row: RowData;
  type: GenericTableData<Assume<this['row'], RowData>>;
}

export class DatabendPreparedQuery extends DatabendPreparedQueryBase {
  static override readonly [entityKind]: string = 'DatabendPreparedQuery';

  constructor(
    private client: DatabendClientLike,
    private queryString: string,
    private params: unknown[],
    private logger: Logger,
    private fields: any[] | undefined,
    private _isResponseInArrayMode: boolean,
    private customResultMapper:
      | ((rows: unknown[][]) => any)
      | undefined,
    private typings?: QueryTypingsValue[]
  ) {
    super({ sql: queryString, params });
  }

  async execute(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<any> {
    const params = fillPlaceholders(this.params, placeholderValues);
    this.logger.logQuery(this.queryString, params);

    const { fields, joinsNotNullableMap, customResultMapper, typings } =
      this as typeof this & { joinsNotNullableMap?: Record<string, boolean> };

    if (fields) {
      const { rows } = await executeArraysOnClient(
        this.client,
        this.queryString,
        params,
        typings
      );

      if (rows.length === 0) {
        return [];
      }

      return customResultMapper
        ? customResultMapper(rows)
        : rows.map((row) =>
            mapResultRow(fields, row, joinsNotNullableMap)
          );
    }

    const rows = await executeOnClient(this.client, this.queryString, params, typings);

    return rows;
  }

  all(
    placeholderValues: Record<string, unknown> | undefined = {}
  ): Promise<any> {
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
> extends DatabendSessionBase {
  static override readonly [entityKind]: string = 'DatabendSession';

  override dialect: DatabendDialect;
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

  override prepareQuery(
    query: Query,
    fields: any[] | undefined,
    name: string | undefined,
    isResponseInArrayMode: boolean,
    customResultMapper?: (rows: unknown[][]) => any
  ): DatabendPreparedQuery {
    void name;
    return new DatabendPreparedQuery(
      this.client,
      query.sql,
      query.params,
      this.logger,
      fields,
      isResponseInArrayMode,
      customResultMapper,
      (query as any).typings
    );
  }

  override async transaction<T>(
    transaction: (tx: DatabendTransaction<TFullSchema, TSchema>) => Promise<T>,
    config?: DatabendTransactionConfig
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

export interface DatabendTransactionConfig {
  isolationLevel?: string;
  accessMode?: string;
}

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
> extends DatabendTransactionBase {
  static override readonly [entityKind]: string = 'DatabendTransaction';

  declare schema: any;

  override rollback(): never {
    throw new TransactionRollbackError();
  }

  getTransactionConfigSQL(config: DatabendTransactionConfig): SQL {
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

  setTransaction(config: DatabendTransactionConfig): Promise<void> {
    return (this as any).session.execute(
      sql`SET TRANSACTION ${this.getTransactionConfigSQL(config)}`
    );
  }

  override async transaction<T>(
    transaction: (tx: DatabendTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    // Databend does not support savepoints. Use rollback-only fallback.
    const internals = this as any;

    const nestedTx = new DatabendTransaction<TFullSchema, TSchema>(
      internals.dialect,
      internals.session,
      this.schema,
      this.nestedIndex + 1
    );

    return transaction(nestedTx).catch((error) => {
      (internals.session as DatabendSession<TFullSchema, TSchema>).markRollbackOnly();
      throw error;
    });
  }
}

export type GenericRowData<T extends RowData = RowData> = T;

export type GenericTableData<T = RowData> = T[];
