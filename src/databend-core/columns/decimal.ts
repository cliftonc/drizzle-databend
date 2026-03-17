import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendDecimalBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendDecimalBuilder';

  constructor(name: string, precision?: number, scale?: number) {
    super(name, 'string', 'DatabendDecimal');
    (this.config as any).precision = precision;
    (this.config as any).scale = scale;
  }

  /** @internal */
  build(table: any): DatabendDecimal {
    return new DatabendDecimal(table, this.config);
  }
}

export class DatabendDecimal extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendDecimal';

  precision: number | undefined;
  scale: number | undefined;

  constructor(table: any, config: any) {
    super(table, config);
    this.precision = config.precision;
    this.scale = config.scale;
  }

  getSQLType(): string {
    if (this.precision !== undefined && this.scale !== undefined) {
      return `decimal(${this.precision}, ${this.scale})`;
    }
    if (this.precision !== undefined) {
      return `decimal(${this.precision})`;
    }
    return 'decimal';
  }
}

export function decimal(name?: string, config?: { precision?: number; scale?: number }) {
  return new DatabendDecimalBuilder(name ?? '', config?.precision, config?.scale);
}
