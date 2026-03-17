import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendTinyIntBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendTinyIntBuilder';

  constructor(name: string) {
    super(name, 'number', 'DatabendTinyInt');
  }

  /** @internal */
  build(table: any): DatabendTinyInt {
    return new DatabendTinyInt(table, this.config);
  }
}

export class DatabendTinyInt extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendTinyInt';

  getSQLType(): string {
    return 'tinyint';
  }
}

export function tinyint(name?: string) {
  return new DatabendTinyIntBuilder(name ?? '');
}
