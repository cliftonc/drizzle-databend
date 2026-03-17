import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendRealBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendRealBuilder';

  constructor(name: string) {
    super(name, 'number', 'DatabendReal');
  }

  /** @internal */
  build(table: any): DatabendReal {
    return new DatabendReal(table, this.config);
  }
}

export class DatabendReal extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendReal';

  getSQLType(): string {
    return 'real';
  }
}

export function real(name?: string) {
  return new DatabendRealBuilder(name ?? '');
}

export function float(name?: string) {
  return new DatabendRealBuilder(name ?? '');
}
