import { entityKind } from 'drizzle-orm/entity';
import { DatabendColumn, DatabendColumnBuilder } from './common.ts';

export class DatabendVariantBuilder extends DatabendColumnBuilder {
  static readonly [entityKind]: string = 'DatabendVariantBuilder';

  constructor(name: string) {
    super(name, 'json', 'DatabendVariant');
  }

  /** @internal */
  build(table: any): DatabendVariant {
    return new DatabendVariant(table, this.config);
  }
}

export class DatabendVariant extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendVariant';

  getSQLType(): string {
    return 'variant';
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

  override mapToDriverValue(value: unknown): string {
    if (typeof value === 'string') {
      return value;
    }
    return JSON.stringify(value);
  }
}

export function variant(name?: string) {
  return new DatabendVariantBuilder(name ?? '');
}
