import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendMapBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendMapBuilder';

  constructor(name: string, keyType: string, valueType: string) {
    super(name, 'json', 'DatabendMap');
    (this.config as any).keyType = keyType;
    (this.config as any).valueType = valueType;
  }

  /** @internal */
  build(table: any): DatabendMap {
    return new DatabendMap(table, this.config);
  }
}

export class DatabendMap extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendMap';

  keyType: string;
  valueType: string;

  constructor(table: any, config: any) {
    super(table, config);
    this.keyType = config.keyType;
    this.valueType = config.valueType;
  }

  getSQLType(): string {
    return `map(${this.keyType}, ${this.valueType})`;
  }

  override mapFromDriverValue(value: any): Record<string, unknown> {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return {};
      }
    }
    return value ?? {};
  }
}

export function map(name: string, keyType: string, valueType: string) {
  return new DatabendMapBuilder(name, keyType, valueType);
}
