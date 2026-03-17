import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendBigInt53Builder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendBigInt53Builder';

  constructor(name: string) {
    super(name, 'number', 'DatabendBigInt53');
  }

  /** @internal */
  build(table: any): DatabendBigInt53 {
    return new DatabendBigInt53(table, this.config);
  }
}

export class DatabendBigInt53 extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendBigInt53';

  getSQLType(): string {
    return 'bigint';
  }
}

export class DatabendBigInt64Builder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendBigInt64Builder';

  constructor(name: string) {
    super(name, 'bigint', 'DatabendBigInt64');
  }

  /** @internal */
  build(table: any): DatabendBigInt64 {
    return new DatabendBigInt64(table, this.config);
  }
}

export class DatabendBigInt64 extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendBigInt64';

  getSQLType(): string {
    return 'bigint';
  }

  mapFromDriverValue(value: any): bigint {
    return BigInt(value);
  }
}

export function bigint(name?: string) {
  return new DatabendBigInt53Builder(name ?? '');
}
