import { entityKind } from 'drizzle-orm/entity';
import { sql } from 'drizzle-orm/sql/sql';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendDateBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendDateBuilder';

  constructor(name: string) {
    super(name, 'string', 'DatabendDate');
  }

  defaultNow() {
    return this.default(sql`now()`);
  }

  /** @internal */
  build(table: any): DatabendDate {
    return new DatabendDate(table, this.config);
  }
}

export class DatabendDate extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendDate';

  getSQLType(): string {
    return 'date';
  }

  override mapFromDriverValue(value: any): string {
    if (value instanceof Date) {
      return value.toISOString().slice(0, 10);
    }
    if (typeof value === 'string') {
      return value.slice(0, 10);
    }
    return value;
  }

  override mapToDriverValue(value: string | Date): string {
    if (value instanceof Date) {
      return value.toISOString().slice(0, 10);
    }
    return value;
  }
}

export function date(name?: string) {
  return new DatabendDateBuilder(name ?? '');
}
