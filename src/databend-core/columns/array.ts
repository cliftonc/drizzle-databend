import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendArrayBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendArrayBuilder';

  constructor(name: string, elementType: string) {
    super(name, 'json', 'DatabendArray');
    (this.config as any).elementType = elementType;
  }

  /** @internal */
  build(table: any): DatabendArray {
    return new DatabendArray(table, this.config);
  }
}

export class DatabendArray extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendArray';

  elementType: string;

  constructor(table: any, config: any) {
    super(table, config);
    this.elementType = config.elementType;
  }

  getSQLType(): string {
    return `array(${this.elementType})`;
  }

  override mapFromDriverValue(value: any): unknown[] {
    if (typeof value === 'string') {
      try {
        return JSON.parse(value);
      } catch {
        return [];
      }
    }
    return value ?? [];
  }
}

export function array(name: string, elementType: string) {
  return new DatabendArrayBuilder(name, elementType);
}
