import { entityKind } from 'drizzle-orm/entity';
import { DatabendTable } from './table.ts';

export function primaryKey(...config: any[]): PrimaryKeyBuilder {
  if (config[0].columns) {
    return new PrimaryKeyBuilder(config[0].columns, config[0].name);
  }
  return new PrimaryKeyBuilder(config);
}

export class PrimaryKeyBuilder {
  static readonly [entityKind]: string = 'DatabendPrimaryKeyBuilder';

  /** @internal */
  columns: any[];
  /** @internal */
  name?: string;

  constructor(columns: any[], name?: string) {
    this.columns = columns;
    this.name = name;
  }

  /** @internal */
  build(table: any) {
    return new PrimaryKey(table, this.columns, this.name);
  }
}

export class PrimaryKey {
  static readonly [entityKind]: string = 'DatabendPrimaryKey';

  columns: any[];
  name?: string;

  constructor(
    public table: any,
    columns: any[],
    name?: string
  ) {
    this.columns = columns;
    this.name = name;
  }

  getName() {
    return this.name ?? `${this.table[DatabendTable.Symbol.Name]}_${this.columns.map((column: any) => column.name).join('_')}_pk`;
  }
}
