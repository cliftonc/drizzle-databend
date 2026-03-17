import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendDoublePrecisionBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendDoublePrecisionBuilder';

  constructor(name: string) {
    super(name, 'number', 'DatabendDoublePrecision');
  }

  /** @internal */
  build(table: any): DatabendDoublePrecision {
    return new DatabendDoublePrecision(table, this.config);
  }
}

export class DatabendDoublePrecision extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendDoublePrecision';

  getSQLType(): string {
    return 'double';
  }

  mapFromDriverValue(value: any): number {
    if (typeof value === 'string') {
      return Number.parseFloat(value);
    }
    return value;
  }
}

export function doublePrecision(name?: string) {
  return new DatabendDoublePrecisionBuilder(name ?? '');
}
