import {
  type AnyColumn,
  Column,
  type DriverValueDecoder,
  getTableName,
  is,
  type SelectedFieldsOrdered,
  SQL,
  Subquery,
} from 'drizzle-orm';
import { DatabendCustomColumn } from '../databend-core/columns/custom.ts';
import { DatabendDate } from '../databend-core/columns/date.ts';
import { DatabendTimestamp } from '../databend-core/columns/timestamp.ts';

type SQLInternal<T = unknown> = SQL<T> & {
  decoder: DriverValueDecoder<T, any>;
};

/** Extract SQL from Aliased (has .sql) or Subquery (has _.sql) */
function getFieldSql(field: unknown): SQL {
  const f = field as any;
  if (f.sql instanceof SQL) return f.sql;
  if (f._?.sql instanceof SQL) return f._.sql;
  throw new Error('Cannot extract SQL from field');
}

type DecoderInput<TDecoder extends DriverValueDecoder<unknown, unknown>> =
  Parameters<TDecoder['mapFromDriverValue']>[0];

function toDecoderInput<TDecoder extends DriverValueDecoder<unknown, unknown>>(
  decoder: TDecoder,
  value: unknown
): DecoderInput<TDecoder> {
  void decoder;
  return value as DecoderInput<TDecoder>;
}

export function normalizeTimestampString(
  value: unknown,
  _withTimezone: boolean
): string | unknown {
  if (value instanceof Date) {
    const iso = value.toISOString().replace('T', ' ');
    return iso.replace('Z', '');
  }
  if (typeof value === 'string') {
    return value.replace('T', ' ');
  }
  return value;
}

export function normalizeTimestamp(
  value: unknown,
  _withTimezone: boolean
): Date | unknown {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === 'string') {
    const hasOffset =
      value.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(value.trim());
    const spaced = value.replace(' ', 'T');
    const normalized = hasOffset ? spaced : `${spaced}Z`;
    return new Date(normalized);
  }
  return value;
}

export function normalizeDateString(value: unknown): string | unknown {
  if (value instanceof Date) {
    return value.toISOString().slice(0, 10);
  }
  if (typeof value === 'string') {
    return value.slice(0, 10);
  }
  return value;
}

export function normalizeDateValue(value: unknown): Date | unknown {
  if (value instanceof Date) {
    return value;
  }
  if (typeof value === 'string') {
    return new Date(`${value.slice(0, 10)}T00:00:00Z`);
  }
  return value;
}

function mapDriverValue(
  decoder: DriverValueDecoder<unknown, unknown>,
  rawValue: unknown
): unknown {
  if (is(decoder, DatabendTimestamp)) {
    const normalized = normalizeTimestamp(rawValue, false);
    if (normalized instanceof Date) {
      return normalized;
    }
    return decoder.mapFromDriverValue(toDecoderInput(decoder, normalized));
  }

  if (is(decoder, DatabendDate)) {
    return decoder.mapFromDriverValue(
      toDecoderInput(decoder, normalizeDateString(rawValue))
    );
  }

  return decoder.mapFromDriverValue(toDecoderInput(decoder, rawValue));
}

export function mapResultRow<TResult>(
  columns: SelectedFieldsOrdered<AnyColumn>,
  row: unknown[],
  joinsNotNullableMap: Record<string, boolean> | undefined
): TResult {
  const nullifyMap: Record<string, string | false> = {};

  const result = columns.reduce<Record<string, any>>(
    (acc, { path, field }, columnIndex) => {
      let decoder: DriverValueDecoder<unknown, unknown>;
      if (is(field, Column)) {
        decoder = field;
      } else if (is(field, SQL)) {
        decoder = (field as SQLInternal).decoder;
      } else {
        const fieldSql = getFieldSql(field);
        const col = fieldSql.queryChunks.find((chunk) => is(chunk, Column));

        if (is(col, DatabendCustomColumn)) {
          decoder = col;
        } else {
          decoder = (fieldSql as SQLInternal).decoder;
        }
      }
      let node = acc;
      for (const [pathChunkIndex, pathChunk] of path.entries()) {
        if (pathChunkIndex < path.length - 1) {
          if (!(pathChunk in node)) {
            node[pathChunk] = {};
          }
          node = node[pathChunk];
          continue;
        }

        const rawValue = row[columnIndex]!;

        const value = (node[pathChunk] =
          rawValue === null ? null : mapDriverValue(decoder, rawValue));

        if (joinsNotNullableMap && is(field, Column) && path.length === 2) {
          const objectName = path[0]!;
          if (!(objectName in nullifyMap)) {
            nullifyMap[objectName] =
              value === null ? getTableName(field.table) : false;
          } else if (
            typeof nullifyMap[objectName] === 'string' &&
            nullifyMap[objectName] !== getTableName(field.table)
          ) {
            nullifyMap[objectName] = false;
          }
          continue;
        }

        if (
          joinsNotNullableMap &&
          is(field, SQL.Aliased) &&
          path.length === 2
        ) {
          const col = field.sql.queryChunks.find((chunk) => is(chunk, Column));
          const tableName = col?.table && getTableName(col?.table);

          if (!tableName) {
            continue;
          }

          const objectName = path[0]!;

          if (!(objectName in nullifyMap)) {
            nullifyMap[objectName] = value === null ? tableName : false;
            continue;
          }

          if (nullifyMap[objectName] && nullifyMap[objectName] !== tableName) {
            nullifyMap[objectName] = false;
          }
        }
      }
      return acc;
    },
    {}
  );

  if (joinsNotNullableMap && Object.keys(nullifyMap).length > 0) {
    for (const [objectName, tableName] of Object.entries(nullifyMap)) {
      if (typeof tableName === 'string' && !joinsNotNullableMap[tableName]) {
        result[objectName] = null;
      }
    }
  }

  return result as TResult;
}
