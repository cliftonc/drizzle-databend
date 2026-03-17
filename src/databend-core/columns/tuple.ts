import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendTupleBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendTupleBuilder';

  constructor(name: string, types: string[]) {
    super(name, 'json', 'DatabendTuple');
    (this.config as any).tupleTypes = types;
  }

  /** @internal */
  build(table: any): DatabendTuple {
    return new DatabendTuple(table, this.config);
  }
}

export class DatabendTuple extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendTuple';

  tupleTypes: string[];

  constructor(table: any, config: any) {
    super(table, config);
    this.tupleTypes = config.tupleTypes;
  }

  getSQLType(): string {
    return `tuple(${this.tupleTypes.join(', ')})`;
  }

  override mapFromDriverValue(value: any): unknown {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return value;
      }
    }
    return value;
  }
}

export function tuple(name: string, types: string[]) {
  return new DatabendTupleBuilder(name, types);
}
