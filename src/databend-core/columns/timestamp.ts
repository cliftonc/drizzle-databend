import { entityKind } from 'drizzle-orm/entity';
import { sql } from 'drizzle-orm/sql/sql';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendTimestampBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendTimestampBuilder';

  constructor(name: string) {
    super(name, 'date', 'DatabendTimestamp');
  }

  defaultNow() {
    return this.default(sql`now()`);
  }

  /** @internal */
  build(table: any): DatabendTimestamp {
    return new DatabendTimestamp(table, this.config);
  }
}

export class DatabendTimestamp extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendTimestamp';

  getSQLType(): string {
    return 'timestamp';
  }

  override mapFromDriverValue(value: any): Date {
    if (value instanceof Date) {
      return value;
    }
    const str = String(value);
    const hasOffset = str.endsWith('Z') || /[+-]\d{2}:?\d{2}$/.test(str);
    const normalized = hasOffset
      ? str.replace(' ', 'T')
      : `${str.replace(' ', 'T')}Z`;
    return new Date(normalized);
  }

  override mapToDriverValue(value: Date | string): string {
    if (value instanceof Date) {
      return value.toISOString();
    }
    return value;
  }
}

export function timestamp(name?: string) {
  return new DatabendTimestampBuilder(name ?? '');
}
