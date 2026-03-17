import type { Connection } from 'databend-driver';
import type { QueryTypingsValue } from 'drizzle-orm';

export interface DatabendConnectionPool {
  acquire(): Promise<Connection>;
  release(connection: Connection): void | Promise<void>;
  close?(): Promise<void> | void;
}

export type DatabendClientLike = Connection | DatabendConnectionPool;
export type RowData = Record<string, unknown>;

export type ExecuteArraysResult = { columns: string[]; rows: unknown[][] };

export function isPool(
  client: DatabendClientLike
): client is DatabendConnectionPool {
  return typeof (client as DatabendConnectionPool).acquire === 'function';
}

/**
 * Convert Drizzle param array to a JSON value accepted by databend-driver's Params.
 * Databend's Params is serde_json::Value, so we pass an array of JSON-serializable values.
 *
 * The databend-driver does client-side parameter substitution with no string escaping,
 * so we must pre-escape single quotes here (SQL standard '' escaping).
 */
export function prepareParams(params: unknown[], typings?: QueryTypingsValue[]): unknown[] {
  return params.map((param, i) => {
    if (param === undefined) return null;
    if (param instanceof Date) return param.toISOString().replace(/'/g, "''");
    if (typeof param === 'bigint') return param.toString();
    if (typeof param === 'string') {
      const typing = typings?.[i];
      if (typing === 'decimal' && /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(param)) {
        return Number(param);
      }
      return param.replace(/'/g, "''");
    }
    if (typeof param === 'object' && param !== null) {
      return JSON.stringify(param).replace(/'/g, "''");
    }
    return param;
  });
}

function isTransientError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  const msg = error.message?.toLowerCase() ?? '';
  return msg.includes('connection closed') || msg.includes('econnreset') ||
         msg.includes('epipe') || msg.includes('socket hang up') ||
         msg.includes('connection refused');
}

async function withRetry<T>(fn: () => Promise<T>, maxRetries = 2): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try { return await fn(); }
    catch (error) {
      lastError = error;
      if (!isTransientError(error) || attempt === maxRetries) throw error;
      await new Promise(r => setTimeout(r, 100 * 2 ** attempt));
    }
  }
  throw lastError;
}

function deduplicateColumns(columns: string[]): string[] {
  const counts = new Map<string, number>();
  let hasDuplicates = false;

  for (const column of columns) {
    const next = (counts.get(column) ?? 0) + 1;
    counts.set(column, next);
    if (next > 1) {
      hasDuplicates = true;
      break;
    }
  }

  if (!hasDuplicates) {
    return columns;
  }

  counts.clear();
  return columns.map((column) => {
    const count = counts.get(column) ?? 0;
    counts.set(column, count + 1);
    return count === 0 ? column : `${column}_${count}`;
  });
}

export async function executeOnClient(
  client: DatabendClientLike,
  query: string,
  params: unknown[],
  typings?: QueryTypingsValue[]
): Promise<RowData[]> {
  if (isPool(client)) {
    return withRetry(async () => {
      const connection = await client.acquire();
      try {
        return await executeOnClient(connection, query, params, typings);
      } finally {
        await client.release(connection);
      }
    });
  }

  const prepared = prepareParams(params, typings);
  const paramValue = prepared.length > 0 ? prepared : undefined;
  const rows = await client.queryAll(query, paramValue);

  if (!rows || rows.length === 0) {
    return [];
  }

  return rows.map((r) => r.data());
}

export async function executeArraysOnClient(
  client: DatabendClientLike,
  query: string,
  params: unknown[],
  typings?: QueryTypingsValue[]
): Promise<ExecuteArraysResult> {
  if (isPool(client)) {
    return withRetry(async () => {
      const connection = await client.acquire();
      try {
        return await executeArraysOnClient(connection, query, params, typings);
      } finally {
        await client.release(connection);
      }
    });
  }

  const prepared = prepareParams(params, typings);
  const paramValue = prepared.length > 0 ? prepared : undefined;
  const iter = await client.queryIter(query, paramValue);
  const schema = iter.schema();
  const fields = schema.fields();
  const columns = deduplicateColumns(fields.map((f) => f.name));

  const rows: unknown[][] = [];
  while (true) {
    const row = await iter.next();
    if (row === null) break;
    if (row instanceof Error) throw row;
    rows.push(row.values());
  }

  return { columns, rows };
}

export async function execOnClient(
  client: DatabendClientLike,
  query: string,
  params: unknown[],
  typings?: QueryTypingsValue[]
): Promise<number> {
  if (isPool(client)) {
    return withRetry(async () => {
      const connection = await client.acquire();
      try {
        return await execOnClient(connection, query, params, typings);
      } finally {
        await client.release(connection);
      }
    });
  }

  const prepared = prepareParams(params, typings);
  const paramValue = prepared.length > 0 ? prepared : undefined;
  return await client.exec(query, paramValue);
}

export async function closeClientConnection(
  connection: Connection
): Promise<void> {
  if ('close' in connection && typeof connection.close === 'function') {
    await connection.close();
  }
}
