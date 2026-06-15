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
 * Convert a Drizzle param array into the JSON values databend-driver binds to `?`
 * placeholders. Databend's Params is a serde_json::Value, so we pass an array of
 * JSON-serializable values.
 *
 * As of databend-driver 0.34.0 the driver binds parameters server-side (or, against
 * older servers, interpolates them with its own SQL-standard '' escaping). Either way
 * the driver owns escaping, so we must NOT pre-escape here - doing so would corrupt
 * values via double-escaping. Our only job is to coerce JS types the JSON params array
 * cannot carry verbatim (bigint, Date, structured values) into a form the server binds
 * to the target column.
 */
export function prepareParams(params: unknown[], typings?: QueryTypingsValue[]): unknown[] {
  return params.map((param, i) => {
    if (param === undefined) return null;
    if (param === null) return null;
    // JSON has no Date type: send an ISO string for the server to bind to TIMESTAMP.
    if (param instanceof Date) return param.toISOString();
    // JSON cannot carry a bigint: send it as a string for the server to parse.
    if (typeof param === 'bigint') return param.toString();
    if (typeof param === 'string') {
      // Drizzle hands decimal/numeric values over as strings. Coerce them to a JS number
      // so the server binds a numeric value rather than a string for numeric columns.
      const typing = typings?.[i];
      if (typing === 'decimal' && /^-?\d+(\.\d+)?([eE][+-]?\d+)?$/.test(param)) {
        return Number(param);
      }
      return param;
    }
    // VARIANT / ARRAY / MAP / TUPLE: serialize structured values to a JSON string, which
    // the server parses into the semi-structured column type.
    if (typeof param === 'object') {
      return JSON.stringify(param);
    }
    // number / boolean pass through unchanged.
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
