import { customType } from 'drizzle-orm/pg-core';
import type { SQL } from 'drizzle-orm';

/**
 * Databend VARIANT column type.
 * Stores semi-structured JSON-like data. Maps to `unknown` in TypeScript.
 */
export const databendVariant = <TData = unknown>(name: string) =>
  customType<{ data: TData; driverData: string | TData }>({
    dataType() {
      return 'VARIANT';
    },
    toDriver(value: TData): string {
      if (typeof value === 'string') {
        return value;
      }
      return JSON.stringify(value);
    },
    fromDriver(value: string | TData): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value as TData;
    },
  })(name);

/**
 * Databend ARRAY column type.
 * Stores typed arrays. Maps to `T[]` in TypeScript.
 */
export const databendArray = <TData = unknown>(
  name: string,
  elementType: string
) =>
  customType<{ data: TData[]; driverData: TData[] | string }>({
    dataType() {
      return `ARRAY(${elementType})`;
    },
    toDriver(value: TData[]): TData[] {
      return value;
    },
    fromDriver(value: TData[] | string): TData[] {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData[];
        } catch {
          return [] as TData[];
        }
      }
      return value;
    },
  })(name);

/**
 * Databend TUPLE column type.
 * Stores positional tuples. Maps to typed tuple in TypeScript.
 */
export const databendTuple = <TData extends unknown[]>(
  name: string,
  types: string[]
) =>
  customType<{ data: TData; driverData: TData | string }>({
    dataType() {
      return `TUPLE(${types.join(', ')})`;
    },
    toDriver(value: TData): TData {
      return value;
    },
    fromDriver(value: TData | string): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value;
    },
  })(name);

/**
 * Databend MAP column type.
 * Stores key-value maps. Maps to `Record<K, V>` in TypeScript.
 */
export const databendMap = <TData extends Record<string, any>>(
  name: string,
  keyType: string,
  valueType: string
) =>
  customType<{ data: TData; driverData: TData | string }>({
    dataType() {
      return `MAP(${keyType}, ${valueType})`;
    },
    toDriver(value: TData): TData {
      return value;
    },
    fromDriver(value: TData | string): TData {
      if (typeof value === 'string') {
        try {
          return JSON.parse(value) as TData;
        } catch {
          return value as unknown as TData;
        }
      }
      return value;
    },
  })(name);

/**
 * Databend TIMESTAMP column type.
 */
export const databendTimestamp = (name: string) =>
  customType<{
    data: Date | string;
    driverData: string | Date;
  }>({
    dataType() {
      return 'TIMESTAMP';
    },
    toDriver(value: Date | string): string {
      if (value instanceof Date) {
        return value.toISOString();
      }
      return value;
    },
    fromDriver(value: string | Date): Date {
      if (value instanceof Date) {
        return value;
      }
      const str = String(value);
      const hasOffset =
        str.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(str);
      const normalized = hasOffset
        ? str.replace(' ', 'T')
        : `${str.replace(' ', 'T')}Z`;
      return new Date(normalized);
    },
  })(name);

/**
 * Databend DATE column type.
 */
export const databendDate = (name: string) =>
  customType<{ data: string | Date; driverData: string | Date }>({
    dataType() {
      return 'DATE';
    },
    toDriver(value: string | Date): string {
      if (value instanceof Date) {
        return value.toISOString().slice(0, 10);
      }
      return value;
    },
    fromDriver(value: string | Date): string {
      if (value instanceof Date) {
        return value.toISOString().slice(0, 10);
      }
      return value.slice(0, 10);
    },
  })(name);
