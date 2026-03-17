import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendBinaryBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendBinaryBuilder';

  constructor(name: string) {
    super(name, 'string', 'DatabendBinary');
  }

  /** @internal */
  build(table: any): DatabendBinary {
    return new DatabendBinary(table, this.config);
  }
}

export class DatabendBinary extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendBinary';

  getSQLType(): string {
    return 'binary';
  }
}

export function binary(name?: string) {
  return new DatabendBinaryBuilder(name ?? '');
}
