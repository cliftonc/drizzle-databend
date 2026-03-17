import { Column } from 'drizzle-orm/column';
import { ColumnBuilder } from 'drizzle-orm/column-builder';
import { entityKind } from 'drizzle-orm/entity';

export class DatabendColumnBuilder extends ColumnBuilder {
  static readonly [entityKind]: string = 'DatabendColumnBuilder';

  generatedAlwaysAs(as: any, _config?: any): any {
    (this as any).config.generated = { as, type: 'always', mode: 'stored' };
    return this;
  }

  unique(name?: string, config?: { nulls?: string }): this {
    this.config.isUnique = true;
    this.config.uniqueName = name;
    this.config.uniqueType = config?.nulls;
    return this;
  }

  /** @internal */
  buildForeignKeys(_column: any, _table: any): any[] {
    return [];
  }

  /** @internal */
  buildExtraConfigColumn(table: any): DatabendExtraConfigColumn {
    return new DatabendExtraConfigColumn(table, this.config);
  }
}

export class DatabendColumn extends Column {
  static readonly [entityKind]: string = 'DatabendColumn';

  constructor(table: any, config: any) {
    super(table, config);
  }

  getSQLType(): string {
    return '';
  }
}

export class DatabendExtraConfigColumn extends DatabendColumn {
  static readonly [entityKind]: string = 'DatabendExtraConfigColumn';

  getSQLType(): string {
    return this.getSQLType();
  }

  indexConfig = {
    order: (this as any).config.order ?? 'asc',
    nulls: (this as any).config.nulls ?? 'last',
    opClass: (this as any).config.opClass,
  };

  defaultConfig = {
    order: 'asc' as const,
    nulls: 'last' as const,
    opClass: undefined,
  };

  asc(): this {
    this.indexConfig.order = 'asc';
    return this;
  }

  desc(): this {
    this.indexConfig.order = 'desc';
    return this;
  }

  nullsFirst(): this {
    this.indexConfig.nulls = 'first';
    return this;
  }

  nullsLast(): this {
    this.indexConfig.nulls = 'last';
    return this;
  }
}

export class IndexedColumn {
  static readonly [entityKind]: string = 'IndexedColumn';

  name: string;
  keyAsName: boolean;
  type: string;
  indexConfig: any;

  constructor(name: string, keyAsName: boolean, type: string, indexConfig: any) {
    this.name = name;
    this.keyAsName = keyAsName;
    this.type = type;
    this.indexConfig = indexConfig;
  }
}
