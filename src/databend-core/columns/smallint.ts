import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendSmallIntBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendSmallIntBuilder';

  constructor(name: string) {
    super(name, 'number', 'DatabendSmallInt');
  }

  /** @internal */
  build(table: any): DatabendSmallInt {
    return new DatabendSmallInt(table, this.config);
  }
}

export class DatabendSmallInt extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendSmallInt';

  getSQLType(): string {
    return 'smallint';
  }
}

export function smallint(name?: string) {
  return new DatabendSmallIntBuilder(name ?? '');
}
