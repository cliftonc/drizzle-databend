import { entityKind, is } from 'drizzle-orm/entity';
import { SelectionProxyHandler } from 'drizzle-orm/selection-proxy';
import { getTableColumns } from 'drizzle-orm/utils';
import { QueryBuilder } from './query-builders/query-builder.ts';
import { databendTable } from './table.ts';
import { DatabendViewBase } from './view-base.ts';
import { DatabendViewConfig } from './view-common.ts';

class DefaultViewBuilderCore {
  static readonly [entityKind]: string = 'DatabendDefaultViewBuilderCore';
  config: any = {};

  constructor(protected name: string, protected schema?: string) {}

  with(config: any) {
    this.config.with = config;
    return this;
  }
}

export class ViewBuilder extends DefaultViewBuilderCore {
  static override readonly [entityKind]: string = 'DatabendViewBuilder';

  as(qb: any) {
    if (typeof qb === 'function') {
      qb = qb(new QueryBuilder());
    }
    const selectionProxy = new SelectionProxyHandler({
      alias: this.name,
      sqlBehavior: 'error',
      sqlAliasedBehavior: 'alias',
      replaceOriginalName: true,
    });
    const aliasedSelection = new Proxy(qb.getSelectedFields(), selectionProxy);
    return new Proxy(
      new DatabendView({
        databendConfig: this.config,
        config: {
          name: this.name,
          schema: this.schema,
          selectedFields: aliasedSelection,
          query: qb.getSQL().inlineParams(),
        },
      }),
      selectionProxy
    );
  }
}

export class ManualViewBuilder extends DefaultViewBuilderCore {
  static override readonly [entityKind]: string = 'DatabendManualViewBuilder';
  columns: any;

  constructor(name: string, columns: any, schema?: string) {
    super(name, schema);
    this.columns = getTableColumns(databendTable(name, columns));
  }

  existing() {
    return new Proxy(
      new DatabendView({
        databendConfig: undefined,
        config: {
          name: this.name,
          schema: this.schema,
          selectedFields: this.columns,
          query: undefined,
        },
      }),
      new SelectionProxyHandler({
        alias: this.name,
        sqlBehavior: 'error',
        sqlAliasedBehavior: 'alias',
        replaceOriginalName: true,
      })
    );
  }

  as(query: any) {
    return new Proxy(
      new DatabendView({
        databendConfig: this.config,
        config: {
          name: this.name,
          schema: this.schema,
          selectedFields: this.columns,
          query: query.inlineParams(),
        },
      }),
      new SelectionProxyHandler({
        alias: this.name,
        sqlBehavior: 'error',
        sqlAliasedBehavior: 'alias',
        replaceOriginalName: true,
      })
    );
  }
}

export class DatabendView extends DatabendViewBase {
  static override readonly [entityKind]: string = 'DatabendView';
  [DatabendViewConfig]: any;

  constructor({ databendConfig, config }: any) {
    super(config);
    if (databendConfig) {
      this[DatabendViewConfig] = {
        with: databendConfig.with,
      };
    }
  }
}

function databendViewWithSchema(name: string, selection?: any, schema?: string) {
  if (selection) {
    return new ManualViewBuilder(name, selection, schema);
  }
  return new ViewBuilder(name, schema);
}

export function databendView(name: string, columns?: any) {
  return databendViewWithSchema(name, columns, undefined);
}

export function isDatabendView(obj: unknown): obj is DatabendView {
  return is(obj, DatabendView);
}
