import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendBooleanBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendBooleanBuilder';

  constructor(name: string) {
    super(name, 'boolean', 'DatabendBoolean');
  }

  /** @internal */
  build(table: any): DatabendBoolean {
    return new DatabendBoolean(table, this.config);
  }
}

export class DatabendBoolean extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendBoolean';

  getSQLType(): string {
    return 'boolean';
  }
}

export function boolean(name?: string) {
  return new DatabendBooleanBuilder(name ?? '');
}
