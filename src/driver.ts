import type { Connection } from 'databend-driver';
import { Client } from 'databend-driver';
import { entityKind } from 'drizzle-orm/entity';
import type { Logger } from 'drizzle-orm/logger';
import { DefaultLogger } from 'drizzle-orm/logger';
import {
  createTableRelationsHelpers,
  type ExtractTablesWithRelations,
  extractTablesRelationalConfig,
  type RelationalSchemaConfig,
  type TablesRelationalConfig,
} from 'drizzle-orm/relations';
import type { DrizzleConfig } from 'drizzle-orm/utils';
import { closeClientConnection, isPool } from './client.ts';
import { DatabendDatabase as DatabendDatabaseBase } from './databend-core/db.ts';
import { DatabendDialect } from './databend-core/dialect.ts';
import {
  createDatabendConnectionPool,
  type DatabendPoolConfig,
} from './pool.ts';
import type {
  DatabendClientLike,
  DatabendTransaction,
} from './session.ts';
import { DatabendSession } from './session.ts';

export interface DatabendDriverOptions {
  logger?: Logger;
}

export class DatabendDriver {
  static readonly [entityKind]: string = 'DatabendDriver';

  constructor(
    private client: DatabendClientLike,
    private dialect: DatabendDialect,
    private options: DatabendDriverOptions = {}
  ) {}

  createSession(
    schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined
  ): DatabendSession<Record<string, unknown>, TablesRelationalConfig> {
    return new DatabendSession(this.client, this.dialect, schema, {
      logger: this.options.logger,
    });
  }
}

export interface DatabendDrizzleConfig<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DrizzleConfig<TSchema> {
  /** Pool configuration. Use size config or false to disable. */
  pool?: DatabendPoolConfig | false;
}

export interface DatabendDrizzleConfigWithConnection<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DatabendDrizzleConfig<TSchema> {
  /** Connection DSN string */
  connection: string;
}

export interface DatabendDrizzleConfigWithClient<
  TSchema extends Record<string, unknown> = Record<string, never>,
> extends DatabendDrizzleConfig<TSchema> {
  /** Explicit client (connection or pool) */
  client: DatabendClientLike;
}

function isConfigObject(data: unknown): data is Record<string, unknown> {
  if (typeof data !== 'object' || data === null) return false;
  if (data.constructor?.name !== 'Object') return false;
  return (
    'connection' in data ||
    'client' in data ||
    'pool' in data ||
    'schema' in data ||
    'logger' in data
  );
}

function createFromClient<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: DatabendClientLike,
  config: DatabendDrizzleConfig<TSchema> = {},
  databendClient?: Client
): DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>> {
  const dialect = new DatabendDialect();

  const logger =
    config.logger === true ? new DefaultLogger() : config.logger || undefined;

  let schema: RelationalSchemaConfig<TablesRelationalConfig> | undefined;

  if (config.schema) {
    const tablesConfig = extractTablesRelationalConfig(
      config.schema,
      createTableRelationsHelpers
    );
    schema = {
      fullSchema: config.schema,
      schema: tablesConfig.tables,
      tableNamesMap: tablesConfig.tableNamesMap,
    };
  }

  const driver = new DatabendDriver(client, dialect, { logger });
  const session = driver.createSession(schema);

  const db = new DatabendDatabase(
    dialect,
    session,
    schema,
    client,
    databendClient
  );
  return db as DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;
}

async function createFromDsn<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  dsn: string,
  config: DatabendDrizzleConfig<TSchema> = {}
): Promise<DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>> {
  const databendClient = new Client(dsn);

  if (config.pool === false) {
    const connection = await databendClient.getConn();
    return createFromClient(connection, config, databendClient);
  }

  const poolSize = config.pool?.size ?? 4;
  const pool = createDatabendConnectionPool(databendClient, { size: poolSize });
  return createFromClient(pool, config, databendClient);
}

// Overload 1: DSN string (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  dsn: string
): Promise<DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 2: DSN string + config (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  dsn: string,
  config: DatabendDrizzleConfig<TSchema>
): Promise<DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 3: Config with connection (async, auto-pools)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  config: DatabendDrizzleConfigWithConnection<TSchema>
): Promise<DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>>;

// Overload 4: Config with explicit client (sync)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  config: DatabendDrizzleConfigWithClient<TSchema>
): DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;

// Overload 5: Explicit client (sync, backward compatible)
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  client: DatabendClientLike,
  config?: DatabendDrizzleConfig<TSchema>
): DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>;

// Implementation
export function drizzle<
  TSchema extends Record<string, unknown> = Record<string, never>,
>(
  clientOrConfigOrDsn:
    | string
    | DatabendClientLike
    | DatabendDrizzleConfigWithConnection<TSchema>
    | DatabendDrizzleConfigWithClient<TSchema>,
  config?: DatabendDrizzleConfig<TSchema>
):
  | DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>
  | Promise<DatabendDatabase<TSchema, ExtractTablesWithRelations<TSchema>>> {
  // String DSN -> async with auto-pool
  if (typeof clientOrConfigOrDsn === 'string') {
    return createFromDsn(clientOrConfigOrDsn, config);
  }

  // Config object with connection or client
  if (isConfigObject(clientOrConfigOrDsn)) {
    const configObj = clientOrConfigOrDsn as
      | DatabendDrizzleConfigWithConnection<TSchema>
      | DatabendDrizzleConfigWithClient<TSchema>;

    if ('connection' in configObj) {
      const connConfig =
        configObj as DatabendDrizzleConfigWithConnection<TSchema>;
      const { connection, ...restConfig } = connConfig;
      return createFromDsn(
        connection,
        restConfig as DatabendDrizzleConfig<TSchema>
      );
    }

    if ('client' in configObj) {
      const clientConfig = configObj as DatabendDrizzleConfigWithClient<TSchema>;
      const { client: clientValue, ...restConfig } = clientConfig;
      return createFromClient(
        clientValue,
        restConfig as DatabendDrizzleConfig<TSchema>
      );
    }

    throw new Error(
      'Invalid drizzle config: either connection or client must be provided'
    );
  }

  // Direct client (backward compatible)
  return createFromClient(clientOrConfigOrDsn as DatabendClientLike, config);
}

export class DatabendDatabase<
  TFullSchema extends Record<string, unknown> = Record<string, never>,
  TSchema extends TablesRelationalConfig =
    ExtractTablesWithRelations<TFullSchema>,
> extends DatabendDatabaseBase {
  static override readonly [entityKind]: string = 'DatabendDatabase';

  /** The underlying connection or pool */
  readonly $client: DatabendClientLike;

  /** The Databend Client instance (when created from DSN) */
  readonly $databendClient?: Client;

  constructor(
    override readonly dialect: DatabendDialect,
    override readonly session: DatabendSession<TFullSchema, TSchema>,
    schema: RelationalSchemaConfig<TSchema> | undefined,
    client: DatabendClientLike,
    databendClient?: Client
  ) {
    super(dialect, session, schema);
    this.$client = client;
    this.$databendClient = databendClient;
  }

  async close(): Promise<void> {
    if (isPool(this.$client) && this.$client.close) {
      await this.$client.close();
    }
    if (!isPool(this.$client)) {
      await closeClientConnection(this.$client as Connection);
    }
  }

  override async transaction<T>(
    transaction: (tx: DatabendTransaction<TFullSchema, TSchema>) => Promise<T>
  ): Promise<T> {
    return await this.session.transaction<T>(transaction);
  }
}
