import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendVarcharBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendVarcharBuilder';

  constructor(name: string, length?: number) {
    super(name, 'string', 'DatabendVarchar');
    (this.config as any).length = length;
  }

  /** @internal */
  build(table: any): DatabendVarchar {
    return new DatabendVarchar(table, this.config);
  }
}

export class DatabendVarchar extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendVarchar';

  length: number | undefined;

  constructor(table: any, config: any) {
    super(table, config);
    this.length = config.length;
  }

  getSQLType(): string {
    return this.length ? `varchar(${this.length})` : 'varchar';
  }
}

export function varchar(name?: string, config?: { length?: number }) {
  return new DatabendVarcharBuilder(name ?? '', config?.length);
}
