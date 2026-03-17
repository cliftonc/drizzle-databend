import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendIntegerBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendIntegerBuilder';

  constructor(name: string) {
    super(name, 'number', 'DatabendInteger');
  }

  /** @internal */
  build(table: any): DatabendInteger {
    return new DatabendInteger(table, this.config);
  }
}

export class DatabendInteger extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendInteger';

  getSQLType(): string {
    return 'integer';
  }
}

export function integer(name?: string) {
  return new DatabendIntegerBuilder(name ?? '');
}
