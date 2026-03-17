import { aliasedTable, aliasedTableColumn, mapColumnsInAliasedSQLToAlias, mapColumnsInSQLToAlias } from 'drizzle-orm/alias';
import { CasingCache } from 'drizzle-orm/casing';
import { Column } from 'drizzle-orm/column';
import { entityKind, is } from 'drizzle-orm/entity';
import { DrizzleError } from 'drizzle-orm/errors';
import type { MigrationConfig, MigrationMeta } from 'drizzle-orm/migrator';
import {
  getOperators,
  getOrderByOperators,
  Many,
  normalizeRelation,
  One,
} from 'drizzle-orm/relations';
import { and, eq, View } from 'drizzle-orm/sql';
import { Param, SQL, sql } from 'drizzle-orm/sql/sql';
import { Subquery } from 'drizzle-orm/subquery';
import { getTableName, getTableUniqueName, Table } from 'drizzle-orm/table';
// @ts-expect-error - orderSelectedFields is exported at runtime but not in .d.ts
import { orderSelectedFields } from 'drizzle-orm/utils';
import { ViewBaseConfig } from 'drizzle-orm/view-common';
import { DatabendColumn, DatabendDecimal } from './columns/index.ts';
import { DatabendTimestamp } from './columns/timestamp.ts';
import { DatabendVariant } from './columns/variant.ts';
import { DatabendTable } from './table.ts';
import { DatabendViewBase } from './view-base.ts';

export class DatabendDialect {
  static readonly [entityKind]: string = 'DatabendDialect';

  /** @internal */
  casing: any;

  constructor(config?: any) {
    this.casing = new CasingCache(config?.casing);
  }

  async migrate(
    migrations: MigrationMeta[],
    session: any,
    config: MigrationConfig | string
  ): Promise<void> {
    const migrationConfig: MigrationConfig =
      typeof config === 'string' ? { migrationsFolder: config } : config;

    const migrationsSchema = migrationConfig.migrationsSchema ?? 'default';
    const migrationsTable =
      migrationConfig.migrationsTable ?? '__drizzle_migrations';

    const migrationTableCreate = sql`
      CREATE TABLE IF NOT EXISTS ${sql.identifier(migrationsSchema)}.${sql.identifier(
        migrationsTable
      )} (
        id INT NOT NULL,
        hash VARCHAR NOT NULL,
        created_at BIGINT
      )
    `;

    await session.execute(migrationTableCreate);

    const dbMigrations = await session.all(
      sql`SELECT id, hash, created_at FROM ${sql.identifier(
        migrationsSchema
      )}.${sql.identifier(migrationsTable)} ORDER BY created_at DESC LIMIT 1`
    );

    const lastDbMigration = dbMigrations[0] as any;

    await session.transaction(async (tx: any) => {
      for await (const migration of migrations) {
        if (
          !lastDbMigration ||
          Number(lastDbMigration.created_at) < migration.folderMillis
        ) {
          for (const stmt of migration.sql) {
            await tx.execute(sql.raw(stmt));
          }

          await tx.execute(
            sql`INSERT INTO ${sql.identifier(
              migrationsSchema
            )}.${sql.identifier(migrationsTable)} (id, hash, created_at)
              VALUES (
                (SELECT COALESCE(MAX(id), 0) + 1 FROM ${sql.identifier(
                  migrationsSchema
                )}.${sql.identifier(migrationsTable)}),
                ${migration.hash},
                ${migration.folderMillis}
              )`
          );
        }
      }
    });
  }

  escapeName(name: string): string {
    return `"${name}"`;
  }

  escapeParam(num: number): string {
    return `$${num + 1}`;
  }

  escapeString(str: string): string {
    return `'${str.replace(/'/g, "''")}'`;
  }

  buildWithCTE(queries: any[] | undefined) {
    if (!queries?.length) return undefined;
    const withSqlChunks = [sql`with `];
    for (const [i, w] of queries.entries()) {
      withSqlChunks.push(sql`${sql.identifier(w._.alias)} as (${w._.sql})`);
      if (i < queries.length - 1) {
        withSqlChunks.push(sql`, `);
      }
    }
    withSqlChunks.push(sql` `);
    return sql.join(withSqlChunks);
  }

  buildDeleteQuery({ table, where, withList }: any) {
    const withSql = this.buildWithCTE(withList);
    const whereSql = where ? sql` where ${where}` : undefined;
    return sql`${withSql}delete from ${table}${whereSql}`;
  }

  buildUpdateSet(table: any, set: any) {
    const tableColumns = table[(Table as any).Symbol.Columns];
    const columnNames = Object.keys(tableColumns).filter(
      (colName) => set[colName] !== undefined || tableColumns[colName]?.onUpdateFn !== undefined
    );
    const setSize = columnNames.length;
    return sql.join(
      columnNames.flatMap((colName, i) => {
        const col = tableColumns[colName];
        const value = set[colName] ?? sql.param(col.onUpdateFn(), col);
        const res = sql`${sql.identifier(this.casing.getColumnCasing(col))} = ${value}`;
        if (i < setSize - 1) {
          return [res, sql.raw(', ')];
        }
        return [res];
      })
    );
  }

  buildUpdateQuery({ table, set, where, withList }: any) {
    const withSql = this.buildWithCTE(withList);
    const tableName = (table as any)[(DatabendTable as any).Symbol.Name];
    const tableSchema = (table as any)[(DatabendTable as any).Symbol.Schema];
    const origTableName = (table as any)[(DatabendTable as any).Symbol.OriginalName];
    const alias = tableName === origTableName ? undefined : tableName;
    const tableSql = sql`${tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined}${sql.identifier(origTableName)}${alias && sql` ${sql.identifier(alias)}`}`;
    const setSql = this.buildUpdateSet(table, set);
    const whereSql = where ? sql` where ${where}` : undefined;
    return sql`${withSql}update ${tableSql} set ${setSql}${whereSql}`;
  }

  buildSelection(fields: any[], { isSingleTable = false } = {}) {
    const columnsLen = fields.length;
    const chunks = fields.flatMap(({ field }: any, i: number) => {
      const chunk: any[] = [];
      if (is(field, SQL.Aliased) && (field as any).isSelectionField) {
        chunk.push(sql.identifier(field.fieldAlias));
      } else if (is(field, SQL.Aliased) || is(field, SQL)) {
        const query = is(field, SQL.Aliased) ? field.sql : field;
        if (isSingleTable) {
          chunk.push(
            new SQL(
              query.queryChunks.map((c: any) => {
                if (is(c, DatabendColumn)) {
                  return sql.identifier(this.casing.getColumnCasing(c));
                }
                return c;
              })
            )
          );
        } else {
          chunk.push(query);
        }
        if (is(field, SQL.Aliased)) {
          chunk.push(sql` as ${sql.identifier(field.fieldAlias)}`);
        }
      } else if (is(field, Column)) {
        if (isSingleTable) {
          chunk.push(sql.identifier(this.casing.getColumnCasing(field)));
        } else {
          chunk.push(field);
        }
      }
      if (i < columnsLen - 1) {
        chunk.push(sql`, `);
      }
      return chunk;
    });
    return sql.join(chunks);
  }

  buildJoins(joins: any[] | undefined) {
    if (!joins || joins.length === 0) {
      return undefined;
    }
    const joinsArray: any[] = [];
    for (const [index, joinMeta] of joins.entries()) {
      if (index === 0) {
        joinsArray.push(sql` `);
      }
      const table = joinMeta.table;
      const lateralSql = joinMeta.lateral ? sql` lateral` : undefined;
      const onClause = joinMeta.on ? sql` on ${joinMeta.on}` : undefined;
      if (is(table, DatabendTable)) {
        const t = table as any;
        const tableName = t[(DatabendTable as any).Symbol.Name];
        const tableSchema = t[(DatabendTable as any).Symbol.Schema];
        const origTableName = t[(DatabendTable as any).Symbol.OriginalName];
        const alias = tableName === origTableName ? undefined : joinMeta.alias;
        joinsArray.push(
          sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${tableSchema ? sql`${sql.identifier(tableSchema)}.` : undefined}${sql.identifier(origTableName)}${alias && sql` ${sql.identifier(alias)}`}${onClause}`
        );
      } else if (is(table, View)) {
        const viewName = (table as any)[ViewBaseConfig].name;
        const viewSchema = (table as any)[ViewBaseConfig].schema;
        const origViewName = (table as any)[ViewBaseConfig].originalName;
        const alias = viewName === origViewName ? undefined : joinMeta.alias;
        joinsArray.push(
          sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${viewSchema ? sql`${sql.identifier(viewSchema)}.` : undefined}${sql.identifier(origViewName)}${alias && sql` ${sql.identifier(alias)}`}${onClause}`
        );
      } else {
        joinsArray.push(
          sql`${sql.raw(joinMeta.joinType)} join${lateralSql} ${table}${onClause}`
        );
      }
      if (index < joins.length - 1) {
        joinsArray.push(sql` `);
      }
    }
    return sql.join(joinsArray);
  }

  buildFromTable(table: any) {
    if (is(table, Table)) {
      const t = table as any;
      if (t[(Table as any).Symbol.OriginalName] !== t[(Table as any).Symbol.Name]) {
        let fullName = sql`${sql.identifier(t[(Table as any).Symbol.OriginalName])}`;
        if (t[(Table as any).Symbol.Schema]) {
          fullName = sql`${sql.identifier(t[(Table as any).Symbol.Schema])}.${fullName}`;
        }
        return sql`${fullName} ${sql.identifier(t[(Table as any).Symbol.Name])}`;
      }
    }
    return table;
  }

  buildSelectQuery({
    withList,
    fields,
    fieldsFlat,
    where,
    having,
    table,
    joins,
    orderBy,
    groupBy,
    limit,
    offset,
    distinct,
    setOperators,
  }: any) {
    const fieldsList = fieldsFlat ?? orderSelectedFields(fields);
    for (const f of fieldsList) {
      if (
        is(f.field, Column) &&
        getTableName(f.field.table) !==
          (is(table, Subquery)
            ? table._.alias
            : is(table, DatabendViewBase)
              ? (table as any)[ViewBaseConfig].name
              : is(table, SQL)
                ? undefined
                : getTableName(table)) &&
        !((table2: any) =>
          joins?.some(
            ({ alias }: any) =>
              alias ===
              (table2[(Table as any).Symbol.IsAlias]
                ? getTableName(table2)
                : table2[(Table as any).Symbol.BaseName])
          ))(f.field.table)
      ) {
        const tableName = getTableName(f.field.table);
        throw new Error(
          `Your "${f.path.join('->')}" field references a column "${tableName}"."${f.field.name}", but the table "${tableName}" is not part of the query! Did you forget to join it?`
        );
      }
    }

    const isSingleTable = !joins || joins.length === 0;
    const withSql = this.buildWithCTE(withList);
    let distinctSql: SQL | undefined;
    if (distinct) {
      distinctSql = distinct === true ? sql` distinct` : sql` distinct on (${sql.join(distinct.on, sql`, `)})`;
    }
    const selection = this.buildSelection(fieldsList, { isSingleTable });
    const tableSql = this.buildFromTable(table);
    const joinsSql = this.buildJoins(joins);
    const whereSql = where ? sql` where ${where}` : undefined;
    const havingSql = having ? sql` having ${having}` : undefined;
    let orderBySql: SQL | undefined;
    if (orderBy && orderBy.length > 0) {
      orderBySql = sql` order by ${sql.join(orderBy, sql`, `)}`;
    }
    let groupBySql: SQL | undefined;
    if (groupBy && groupBy.length > 0) {
      groupBySql = sql` group by ${sql.join(groupBy, sql`, `)}`;
    }
    const limitSql =
      typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
        ? sql` limit ${limit}`
        : undefined;
    const offsetSql = offset ? sql` offset ${offset}` : undefined;
    const finalQuery = sql`${withSql}select${distinctSql} ${selection} from ${tableSql}${joinsSql}${whereSql}${groupBySql}${havingSql}${orderBySql}${limitSql}${offsetSql}`;
    if (setOperators.length > 0) {
      return this.buildSetOperations(finalQuery, setOperators);
    }
    return finalQuery;
  }

  buildSetOperations(leftSelect: any, setOperators: any[]): any {
    const [setOperator, ...rest] = setOperators;
    if (!setOperator) {
      throw new Error('Cannot pass undefined values to any set operator');
    }
    if (rest.length === 0) {
      return this.buildSetOperationQuery({ leftSelect, setOperator });
    }
    return this.buildSetOperations(
      this.buildSetOperationQuery({ leftSelect, setOperator }),
      rest
    );
  }

  buildSetOperationQuery({
    leftSelect,
    setOperator: { type, isAll, rightSelect, limit, orderBy, offset },
  }: any) {
    const leftChunk = sql`(${leftSelect.getSQL()}) `;
    const rightChunk = sql`(${rightSelect.getSQL()})`;
    let orderBySql: SQL | undefined;
    if (orderBy && orderBy.length > 0) {
      const orderByValues: any[] = [];
      for (const singleOrderBy of orderBy) {
        if (is(singleOrderBy, DatabendColumn)) {
          orderByValues.push(sql.identifier(singleOrderBy.name));
        } else if (is(singleOrderBy, SQL)) {
          for (let i = 0; i < singleOrderBy.queryChunks.length; i++) {
            const chunk = singleOrderBy.queryChunks[i];
            if (is(chunk, DatabendColumn)) {
              singleOrderBy.queryChunks[i] = sql.identifier(chunk.name);
            }
          }
          orderByValues.push(sql`${singleOrderBy}`);
        } else {
          orderByValues.push(sql`${singleOrderBy}`);
        }
      }
      orderBySql = sql` order by ${sql.join(orderByValues, sql`, `)} `;
    }
    const limitSql =
      typeof limit === 'object' || (typeof limit === 'number' && limit >= 0)
        ? sql` limit ${limit}`
        : undefined;
    const operatorChunk = sql.raw(`${type} ${isAll ? 'all ' : ''}`);
    const offsetSql = offset ? sql` offset ${offset}` : undefined;
    return sql`${leftChunk}${operatorChunk}${rightChunk}${orderBySql}${limitSql}${offsetSql}`;
  }

  buildInsertQuery({ table, values: valuesOrSelect, withList, select }: any) {
    const valuesSqlList: any[] = [];
    const columns = table[(Table as any).Symbol.Columns];
    const colEntries = Object.entries(columns).filter(([_, col]: [string, any]) => !col.shouldDisableInsert());
    const insertOrder = colEntries.map(
      ([, column]: [string, any]) => sql.identifier(this.casing.getColumnCasing(column))
    );

    if (select) {
      const select2 = valuesOrSelect;
      if (is(select2, SQL)) {
        valuesSqlList.push(select2);
      } else {
        valuesSqlList.push(select2.getSQL());
      }
    } else {
      const values = valuesOrSelect;
      valuesSqlList.push(sql.raw('values '));
      for (const [valueIndex, value] of values.entries()) {
        const valueList: any[] = [];
        for (const [fieldName, col] of colEntries) {
          const colValue = value[fieldName];
          if (colValue === undefined || (is(colValue, Param) && colValue.value === undefined)) {
            if ((col as any).defaultFn !== undefined) {
              const defaultFnResult = (col as any).defaultFn();
              const defaultValue = is(defaultFnResult, SQL)
                ? defaultFnResult
                : sql.param(defaultFnResult, col as any);
              valueList.push(defaultValue);
            } else if (!(col as any).default && (col as any).onUpdateFn !== undefined) {
              const onUpdateFnResult = (col as any).onUpdateFn();
              const newValue = is(onUpdateFnResult, SQL)
                ? onUpdateFnResult
                : sql.param(onUpdateFnResult, col as any);
              valueList.push(newValue);
            } else {
              valueList.push(sql`default`);
            }
          } else {
            valueList.push(colValue);
          }
        }
        valuesSqlList.push(valueList);
        if (valueIndex < values.length - 1) {
          valuesSqlList.push(sql`, `);
        }
      }
    }

    const withSql = this.buildWithCTE(withList);
    const valuesSql = sql.join(valuesSqlList);
    return sql`${withSql}insert into ${table} ${insertOrder} ${valuesSql}`;
  }

  prepareTyping(encoder: any): string {
    if (is(encoder, DatabendDecimal) || is(encoder, DatabendColumn)) {
      const sqlType = encoder.getSQLType?.();
      if (sqlType) {
        const lower = sqlType.toLowerCase();
        if (
          lower === 'integer' || lower === 'int' ||
          lower === 'smallint' || lower === 'tinyint' ||
          lower === 'bigint' || lower === 'real' ||
          lower === 'double' || lower === 'float' ||
          lower.startsWith('decimal')
        ) {
          return 'decimal';
        }
        if (lower === 'timestamp') {
          return 'timestamp';
        }
        if (lower === 'date') {
          return 'date';
        }
      }
    }
    if (is(encoder, DatabendTimestamp)) {
      return 'timestamp';
    }
    if (is(encoder, DatabendVariant)) {
      return 'none';
    }
    return 'none';
  }

  sqlToQuery(sql2: any, invokeSource?: any) {
    return sql2.toQuery({
      casing: this.casing,
      escapeName: this.escapeName,
      escapeParam: this.escapeParam,
      escapeString: this.escapeString,
      prepareTyping: this.prepareTyping.bind(this),
      invokeSource,
    });
  }

  buildRelationalQueryWithoutPK({
    fullSchema,
    schema,
    tableNamesMap,
    table,
    tableConfig,
    queryConfig: config,
    tableAlias,
    nestedQueryRelation,
    joinOn,
  }: any): any {
    let selection: any[] = [];
    let limit: any, offset: any, orderBy: any[] = [], where: any;
    const joins: any[] = [];

    if (config === true) {
      const selectionEntries = Object.entries(tableConfig.columns);
      selection = selectionEntries.map(([key, value]: [string, any]) => ({
        dbKey: value.name,
        tsKey: key,
        field: aliasedTableColumn(value, tableAlias),
        relationTableTsKey: undefined,
        isJson: false,
        selection: [],
      }));
    } else {
      const aliasedColumns = Object.fromEntries(
        Object.entries(tableConfig.columns).map(([key, value]: [string, any]) => [key, aliasedTableColumn(value, tableAlias)])
      );

      if (config.where) {
        const whereSql = typeof config.where === 'function'
          ? config.where(aliasedColumns, getOperators())
          : config.where;
        where = whereSql && mapColumnsInSQLToAlias(whereSql, tableAlias);
      }

      const fieldsSelection: any[] = [];
      let selectedColumns: string[] = [];

      if (config.columns) {
        let isIncludeMode = false;
        for (const [field, value] of Object.entries(config.columns)) {
          if (value === undefined) continue;
          if (field in tableConfig.columns) {
            if (!isIncludeMode && value === true) {
              isIncludeMode = true;
            }
            selectedColumns.push(field);
          }
        }
        if (selectedColumns.length > 0) {
          selectedColumns = isIncludeMode
            ? selectedColumns.filter((c) => (config.columns as any)?.[c] === true)
            : Object.keys(tableConfig.columns).filter((key) => !selectedColumns.includes(key));
        }
      } else {
        selectedColumns = Object.keys(tableConfig.columns);
      }

      for (const field of selectedColumns) {
        const column = tableConfig.columns[field];
        fieldsSelection.push({ tsKey: field, value: column });
      }

      let selectedRelations: any[] = [];
      if (config.with) {
        selectedRelations = Object.entries(config.with)
          .filter((entry: any) => !!entry[1])
          .map(([tsKey, queryConfig]: [string, any]) => ({
            tsKey,
            queryConfig,
            relation: tableConfig.relations[tsKey],
          }));
      }

      if (config.extras) {
        const extras = typeof config.extras === 'function'
          ? config.extras(aliasedColumns, { sql })
          : config.extras;
        for (const [tsKey, value] of Object.entries(extras)) {
          fieldsSelection.push({
            tsKey,
            value: mapColumnsInAliasedSQLToAlias(value as any, tableAlias),
          });
        }
      }

      for (const { tsKey, value } of fieldsSelection) {
        selection.push({
          dbKey: is(value, SQL.Aliased) ? value.fieldAlias : tableConfig.columns[tsKey].name,
          tsKey,
          field: is(value, Column) ? aliasedTableColumn(value, tableAlias) : value,
          relationTableTsKey: undefined,
          isJson: false,
          selection: [],
        });
      }

      let orderByOrig: any = typeof config.orderBy === 'function'
        ? config.orderBy(aliasedColumns, getOrderByOperators())
        : config.orderBy ?? [];
      if (!Array.isArray(orderByOrig)) {
        orderByOrig = [orderByOrig];
      }
      orderBy = orderByOrig.map((orderByValue: any) => {
        if (is(orderByValue, Column)) {
          return aliasedTableColumn(orderByValue, tableAlias);
        }
        return mapColumnsInSQLToAlias(orderByValue, tableAlias);
      });

      limit = config.limit;
      offset = config.offset;

      for (const {
        tsKey: selectedRelationTsKey,
        queryConfig: selectedRelationConfigValue,
        relation,
      } of selectedRelations) {
        const normalizedRelation = normalizeRelation(schema, tableNamesMap, relation);
        const relationTableName = getTableUniqueName(relation.referencedTable);
        const relationTableTsName = tableNamesMap[relationTableName];
        const relationTableAlias = `${tableAlias}_${selectedRelationTsKey}`;
        const joinOn2 = and(
          ...normalizedRelation.fields.map(
            (field2: any, i: number) =>
              eq(
                aliasedTableColumn(normalizedRelation.references[i], relationTableAlias),
                aliasedTableColumn(field2, tableAlias)
              )
          )
        );
        const builtRelation = this.buildRelationalQueryWithoutPK({
          fullSchema,
          schema,
          tableNamesMap,
          table: fullSchema[relationTableTsName],
          tableConfig: schema[relationTableTsName],
          queryConfig: is(relation, One)
            ? selectedRelationConfigValue === true
              ? { limit: 1 }
              : { ...selectedRelationConfigValue, limit: 1 }
            : selectedRelationConfigValue,
          tableAlias: relationTableAlias,
          joinOn: joinOn2,
          nestedQueryRelation: relation,
        });
        const field = sql`${sql.identifier(relationTableAlias)}.${sql.identifier('data')}`.as(selectedRelationTsKey);
        joins.push({
          on: sql`true`,
          table: new Subquery(builtRelation.sql, {}, relationTableAlias),
          alias: relationTableAlias,
          joinType: 'left',
          lateral: true,
        });
        selection.push({
          dbKey: selectedRelationTsKey,
          tsKey: selectedRelationTsKey,
          field,
          relationTableTsKey: relationTableTsName,
          isJson: true,
          selection: builtRelation.selection,
        });
      }
    }

    if (selection.length === 0) {
      throw new DrizzleError({ message: `No fields selected for table "${tableConfig.tsName}" ("${tableAlias}")` });
    }

    let result: any;
    where = and(joinOn, where);

    if (nestedQueryRelation) {
      let field: any = sql`json_build_array(${sql.join(
        selection.map(
          ({ field: field2, tsKey, isJson }: any) =>
            isJson
              ? sql`${sql.identifier(`${tableAlias}_${tsKey}`)}.${sql.identifier('data')}`
              : is(field2, SQL.Aliased)
                ? field2.sql
                : field2
        ),
        sql`, `
      )})`;
      if (is(nestedQueryRelation, Many)) {
        field = sql`coalesce(json_agg(${field}${orderBy.length > 0 ? sql` order by ${sql.join(orderBy, sql`, `)}` : undefined}), '[]'::json)`;
      }
      const nestedSelection = [{
        dbKey: 'data',
        tsKey: 'data',
        field: field.as('data'),
        isJson: true,
        relationTableTsKey: tableConfig.tsName,
        selection,
      }];
      const needsSubquery = limit !== undefined || offset !== undefined || orderBy.length > 0;
      if (needsSubquery) {
        result = this.buildSelectQuery({
          table: aliasedTable(table, tableAlias),
          fields: {},
          fieldsFlat: [{ path: [], field: sql.raw('*') }],
          where,
          limit,
          offset,
          orderBy,
          setOperators: [],
        });
        where = undefined;
        limit = undefined;
        offset = undefined;
        orderBy = [];
      } else {
        result = aliasedTable(table, tableAlias);
      }
      result = this.buildSelectQuery({
        table: is(result, DatabendTable) ? result : new Subquery(result, {}, tableAlias),
        fields: {},
        fieldsFlat: nestedSelection.map(({ field: field2 }: any) => ({
          path: [],
          field: is(field2, Column) ? aliasedTableColumn(field2, tableAlias) : field2,
        })),
        joins,
        where,
        limit,
        offset,
        orderBy,
        setOperators: [],
      });
    } else {
      result = this.buildSelectQuery({
        table: aliasedTable(table, tableAlias),
        fields: {},
        fieldsFlat: selection.map(({ field }: any) => ({
          path: [],
          field: is(field, Column) ? aliasedTableColumn(field, tableAlias) : field,
        })),
        joins,
        where,
        limit,
        offset,
        orderBy,
        setOperators: [],
      });
    }

    return {
      tableTsKey: tableConfig.tsName,
      sql: result,
      selection,
    };
  }
}
