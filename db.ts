import { DBClient, RawQueryResult } from './queryState';

// ----- Type utility definitions -----

/** Utility to simplify complex types for better editor display on hover. */
type Prettify<T> = { [K in keyof T]: T[K] } & {};

/**
 * Marks a column as generated by the database (e.g., auto-incrementing ID).
 * Generated columns are automatically optional during inserts.
 */
export type Generated<T> = {
  __generated__: T;
};

/**
 * Represents a database column type allowing different types for
 * select, insert, and update operations.
 * @template Select The type when selecting data.
 * @template Insert The type when inserting data (defaults to Select type).
 * @template Update The type when updating data (defaults to Insert type).
 */
export type ColumnType<Select, Insert = Select, Update = Insert> = {
  __select__: Select;
  __insert__: Insert;
  __update__: Update;
};

/**
 * Shorthand for a JSON column type.
 * Often selected as an object (T) but inserted/updated as a string.
 * @template T The type of the parsed JSON object.
 */
export type JSONColumnType<T> = ColumnType<T, string, string>;

// ----- Type extraction helpers -----

// Extracts the select type, unwrapping Generated types.
type SelectType<T> = 
    T extends Generated<infer G> ? G :
    T extends ColumnType<infer S, any, any> ? S : 
    T;

// Extracts the insert type, making Generated types optional.
type InsertType<T> = T extends ColumnType<any, infer I, any> ? I : T extends Generated<infer G> ? G | undefined : T;

// Extracts the update type.
type UpdateType<T> = T extends ColumnType<any, any, infer U> ? U : T extends Generated<infer G> ? G : T;

// ----- Selectable, Insertable, Updateable types -----

/**
 * Extracts the selectable type shape for a table.
 * All columns are included with their `SelectType`.
 */
export type Selectable<T> = Prettify<{ [K in keyof T]: SelectType<T[K]> }>;

/**
 * Extracts the insertable type shape for a table.
 * - Generated columns are optional.
 * - Columns explicitly marked optional for insert (e.g., `ColumnType<Date, string | undefined>`) are optional.
 * - Other non-nullable columns are required.
 */
type RequiredInsertableKeys<T> = {
  [K in keyof T]: T[K] extends Generated<any> ? never :
                 T[K] extends ColumnType<any, infer I, any> ? (undefined extends I ? never : K) :
                 (null extends T[K] ? never : undefined extends T[K] ? never : K);
}[keyof T];

type OptionalInsertableKeys<T> = Exclude<keyof T, RequiredInsertableKeys<T> | { [K in keyof T]: T[K] extends Generated<any> ? K : never }[keyof T]>;

export type Insertable<T> = Prettify<
  & { [K in RequiredInsertableKeys<T>]: InsertType<T[K]> }
  & { [K in OptionalInsertableKeys<T>]?: InsertType<T[K]> }
>;

/**
 * Extracts the updateable type shape for a table.
 * - All columns are optional.
 * - Columns marked with `never` as their update type (e.g., `ColumnType<Date, any, never>`) are excluded.
 */
type OptionalUpdateableKeys<T> = {
    [K in keyof T]: T[K] extends ColumnType<any, any, infer U>
        ? U extends never ? never : K
        : K // Allow updating non-ColumnType and Generated fields by default
}[keyof T];

export type Updateable<T> = Prettify<
    { [K in OptionalUpdateableKeys<T>]?: UpdateType<T[K]> }
>;

// ----- Database model types -----

/** Base constraint for table definitions within the Database interface. */
export interface TableDefinition { 
  [columnName: string]: any;
}
/** Base constraint for the user-defined Database interface. */
export interface Database { 
  [tableName: string]: TableDefinition;
}

// ----- Query builder implementation -----

type WhereOperator = '=' | '!=' | '<' | '<=' | '>' | '>=' | 'like' | 'in' | 'not in' | 'is' | 'is not';
type JoinType = 'inner' | 'left' | 'right' | 'full';

interface WhereCondition { // Internal representation of a WHERE clause
  column: string;
  operator: WhereOperator;
  value: any;
}

interface JoinClause { // Internal representation of a JOIN clause
  type: JoinType;
  table: string;
  on: {
    leftColumn: string;
    operator: WhereOperator;
    rightColumn: string;
  };
}

/**
 * Internal class for building SQL queries fluently.
 * Provides methods for select, insert, update, delete operations with type safety.
 * Users typically interact with this through the `DB` class methods.
 * 
 * @template DB The main Database interface type.
 * @template TB The current table key (string literal type) being queried.
 * @template SelectResult The expected shape of the result for select queries.
 */
class QueryBuilder<DB extends Database, TB extends keyof DB, SelectResult = Selectable<DB[TB]>> {
  private client: DBClient;
  private transactionId: string | null; // ID for ongoing transaction, if any.
  private fromTable: string | null = null; // Table being queried.
  private selectedColumns: string[] = []; // Columns to select.
  private whereConditions: WhereCondition[] = []; // WHERE clauses.
  private limitValue: number | null = null; // LIMIT value.
  private offsetValue: number | null = null; // OFFSET value.
  private orderByColumns: { column: string; direction: 'asc' | 'desc' }[] = []; // ORDER BY clauses.
  private joinClauses: JoinClause[] = []; // JOIN clauses.
  private groupByColumns: string[] = []; // GROUP BY columns.
  private isCountQuery = false; // Flag for COUNT(*) queries.
  private isDistinct = false; // Flag for SELECT DISTINCT.
  private insertValues: Insertable<DB[TB]>[] | null = null; // Data for INSERT.
  private updateValues: Updateable<DB[TB]> | null = null; // Data for UPDATE.
  private isDeleteOperation = false; // Flag for DELETE operations.

  /**
   * Creates a QueryBuilder instance.
   * @param client The DBClient instance for executing queries.
   * @param transactionId Optional transaction ID if operating within a transaction.
   * @param table Optional initial table name (typically set by DB methods).
   */
  constructor(client: DBClient, transactionId: string | null = null, table?: string) {
    this.client = client;
    this.transactionId = transactionId;
    if (table) {
      this.fromTable = table;
    }
  }

  // ----- Select operations -----
  /** Specifies columns to select. */
  select<C extends (keyof Selectable<DB[TB]> & string)[]>(...columns: C): QueryBuilder<DB, TB, Pick<Selectable<DB[TB]>, C[number]>> {
    this.selectedColumns = columns as string[];
    return this as any; // Type casting necessary due to complex generic manipulation.
  }

  /** Selects all columns ('*'). */
  selectAll(): QueryBuilder<DB, TB, Selectable<DB[TB]>> {
    this.selectedColumns = ['*'];
    return this as any; // Cast needed due to SelectResult complexity
  }

  /** Adds DISTINCT to the select query. */
  distinct(): this {
    this.isDistinct = true;
    return this;
  }

  /** Performs a COUNT query. */
  count(column: string = '*'): QueryBuilder<DB, TB, { count: number }> {
    this.selectedColumns = [`COUNT(${column}) as count`];
    this.isCountQuery = true;
    return this as any;
  }

  // ----- Where conditions -----
  /** Adds a WHERE condition. */
  where<C extends keyof Selectable<DB[TB]> & string>(
    column: C, 
    operator: WhereOperator, 
    value: SelectType<DB[TB][C]> | SelectType<DB[TB][C]>[] 
  ): this {
    this.whereConditions.push({ column, operator, value });
    return this;
  }

  /** Shortcut for `where(column, '=', value)`. */
  whereEquals<C extends keyof Selectable<DB[TB]> & string>(column: C, value: SelectType<DB[TB][C]>): this {
    return this.where(column, '=', value);
  }

  /** Shortcut for `where(column, 'in', values)`. */
  whereIn<C extends keyof Selectable<DB[TB]> & string>(column: C, values: SelectType<DB[TB][C]>[]): this {
    this.whereConditions.push({ column, operator: 'in', value: values });
    return this;
  }

  /** Shortcut for `where(column, 'like', pattern)`. */
  whereLike<C extends keyof Selectable<DB[TB]> & string>(column: C, pattern: string): this {
    return this.where(column, 'like', pattern as any);
  }

  // ----- Joins -----
  /** Adds an INNER JOIN clause. */
  // TODO: Improve join result typing to accurately reflect combined shape.
  join<JoinedTable extends keyof DB>(
    table: JoinedTable & string, 
    leftColumn: keyof Selectable<DB[TB]> & string, 
    operator: WhereOperator, 
    rightColumn: keyof Selectable<DB[JoinedTable]> & string
  ): QueryBuilder<DB, TB | JoinedTable, SelectResult & Partial<Selectable<DB[JoinedTable]>>> {
    this.joinClauses.push({
      type: 'inner',
      table,
      on: { leftColumn, operator, rightColumn }
    });
    return this as any;
  }

  /** Adds a LEFT JOIN clause. */
  leftJoin<JoinedTable extends keyof DB>(
    table: JoinedTable & string, 
    leftColumn: keyof Selectable<DB[TB]> & string, 
    operator: WhereOperator, 
    rightColumn: keyof Selectable<DB[JoinedTable]> & string
  ): QueryBuilder<DB, TB | JoinedTable, SelectResult & Partial<Selectable<DB[JoinedTable]>>> {
    this.joinClauses.push({
      type: 'left',
      table,
      on: { leftColumn, operator, rightColumn }
    });
    return this as any;
  }

  // ----- Ordering and grouping -----
  /** Adds an ORDER BY clause. */
  orderBy(column: keyof Selectable<DB[TB]> & string, direction: 'asc' | 'desc' = 'asc'): this {
    this.orderByColumns.push({ column, direction });
    return this;
  }

  /** Adds a GROUP BY clause. */
  groupBy(...columns: (keyof Selectable<DB[TB]> & string)[]): this {
    this.groupByColumns = columns as string[];
    return this;
  }

  // ----- Limiting results -----
  /** Adds a LIMIT clause. */
  limit(limit: number): this {
    this.limitValue = limit;
    return this;
  }

  /** Adds an OFFSET clause. */
  offset(offset: number): this {
    this.offsetValue = offset;
    return this;
  }

  // ----- Execution methods -----
  /** 
   * Executes the built query (SELECT, INSERT, UPDATE, DELETE).
   * Returns results for SELECT, typically row count info for others (depends on client).
   */
  async execute(): Promise<SelectResult[]> {
    console.log("___execute", this.transactionId)
    const { sql, params } = this.buildQuery();
    console.log("___execute", sql, params)
    let result;
    if (this.transactionId) {
      result = await this.client.queryWithTransaction<SelectResult>(this.transactionId, sql, params);
    } else {
      result = await this.client.standardQuery<SelectResult>(sql, params);
    }
    
    if (!result.ok || !result.json) {
      throw new Error(`Query failed: Status ${result.status}, Response: ${JSON.stringify(result.json)}`);
    }
    return result.json;
  }

  /** Executes the query and returns raw results from the client. */
  async executeRaw(): Promise<RawQueryResult> {
    const { sql, params } = this.buildQuery();
    let result;
    if (this.transactionId) {
      const txResult = await this.client.queryWithTransaction<any[]>(this.transactionId, sql, params);
      // Reconstruct RawQueryResult-like structure
      result = {
        ok: txResult.ok,
        status: txResult.status,
        json: txResult.json ? {
           columns: txResult.json.length > 0 ? Object.keys(txResult.json[0]) : [],
           rows: txResult.json.map(row => Object.values(row)),
           // Note: rows_written might be inaccurate in a transaction context
           meta: { rows_read: txResult.json.length, rows_written: 0 } 
         } : null
      };
    } else {
      result = await this.client.rawQuery(sql, params);
    }

    if (!result.ok || !result.json) {
      throw new Error(`Raw query failed: Status ${result.status}, Response: ${JSON.stringify(result.json)}`);
    }
    return result.json;
  }

  /** Executes the query, takes the first result, or returns undefined. */
  async executeTakeFirst(): Promise<SelectResult | undefined> {
    const results = await this.limit(1).execute();
    return results[0];
  }

  /** Executes the query, takes the first result, or throws an error if none found. */
  async executeTakeFirstOrThrow(): Promise<SelectResult> {
    const result = await this.executeTakeFirst();
    if (!result) {
      throw new Error('No results found');
    }
    return result;
  }

  // ----- Insert operations -----
  /** Specifies the record(s) to insert. */
  values(records: Insertable<DB[TB]> | Insertable<DB[TB]>[]): this {
    console.log("___values", records)
    this.insertValues = Array.isArray(records) ? records : [records];
    return this;
  }

  // ----- Update operations -----
  /** Specifies the values to set in an UPDATE statement. */
  set(values: Updateable<DB[TB]>): this {
    this.updateValues = values;
    return this;
  }

  // ----- Query building -----
  /** 
   * Internal method to construct the final SQL string and parameters.
   * Called by execution methods.
   */
  buildQuery(): { sql: string, params: any[] } {
    const params: any[] = [];
    if (!this.fromTable) throw new Error('Table must be specified');

    if (this.insertValues) return this.buildInsertQuery(params);
    if (this.updateValues) return this.buildUpdateQuery(params);
    if (this.isDeleteOperation) return this.buildDeleteQuery(params);
    return this.buildSelectQuery(params);
  }

  // Internal helper to build SELECT queries.
  private buildSelectQuery(params: any[]): { sql: string, params: any[] } {
    let sql = 'SELECT ';
    if (this.isDistinct) sql += 'DISTINCT ';
    sql += this.selectedColumns.length === 0 ? '*' : this.selectedColumns.join(', ');
    sql += ` FROM ${this.fromTable}`;

    for (const join of this.joinClauses) {
      sql += ` ${join.type.toUpperCase()} JOIN ${join.table} ON ${join.on.leftColumn} ${join.on.operator} ${join.on.rightColumn}`;
    }

    if (this.whereConditions.length > 0) {
      sql += ' WHERE ';
      const whereExpressions = this.whereConditions.map(condition => {
        if ((condition.operator === 'in' || condition.operator === 'not in') && Array.isArray(condition.value)) {
          const placeholders = Array(condition.value.length).fill('?').join(', ');
          params.push(...condition.value);
          return `${condition.column} ${condition.operator} (${placeholders})`;
        } else {
          params.push(condition.value);
          return `${condition.column} ${condition.operator} ?`;
        }
      });
      sql += whereExpressions.join(' AND ');
    }

    if (this.groupByColumns.length > 0) sql += ` GROUP BY ${this.groupByColumns.join(', ')}`;
    if (this.orderByColumns.length > 0) sql += ` ORDER BY ${this.orderByColumns.map(o => `${o.column} ${o.direction.toUpperCase()}`).join(', ')}`;
    if (this.limitValue !== null) { sql += ` LIMIT ?`; params.push(this.limitValue); }
    if (this.offsetValue !== null) { sql += ` OFFSET ?`; params.push(this.offsetValue); }

    return { sql, params };
  }

  // Internal helper to build INSERT queries.
  private buildInsertQuery(params: any[]): { sql: string, params: any[] } {
    if (!this.insertValues || this.insertValues.length === 0) throw new Error('Values required for insert');
    
    const firstRow = this.insertValues[0];
    const columns = Object.keys(firstRow); // Assumes all rows have the same keys
    let sql = `INSERT INTO ${this.fromTable} (${columns.join(', ')})`;
    const valuePlaceholders: string[] = [];

    for (const row of this.insertValues) {
      const rowPlaceholders: string[] = [];
      for (const col of columns) {
        rowPlaceholders.push('?');
        params.push(row[col as keyof typeof row]); // Access value safely
      }
      valuePlaceholders.push(`(${rowPlaceholders.join(', ')})`);
    }
    sql += ` VALUES ${valuePlaceholders.join(', ')}`;
    return { sql, params };
  }

  // Internal helper to build UPDATE queries.
  private buildUpdateQuery(params: any[]): { sql: string, params: any[] } {
    if (!this.updateValues || Object.keys(this.updateValues).length === 0) throw new Error('Set values required for update');
    
    const columns = Object.keys(this.updateValues);
    let sql = `UPDATE ${this.fromTable} SET `;
    const setClauses = columns.map(column => {
      params.push(this.updateValues![column as keyof typeof this.updateValues]); // Access value safely
      return `${column} = ?`;
    });
    sql += setClauses.join(', ');

    if (this.whereConditions.length > 0) { // Reuse WHERE logic from select
       sql += ' WHERE ';
      const whereExpressions = this.whereConditions.map(condition => {
        if ((condition.operator === 'in' || condition.operator === 'not in') && Array.isArray(condition.value)) {
          const placeholders = Array(condition.value.length).fill('?').join(', ');
          params.push(...condition.value);
          return `${condition.column} ${condition.operator} (${placeholders})`;
        } else {
          params.push(condition.value);
          return `${condition.column} ${condition.operator} ?`;
        }
      });
      sql += whereExpressions.join(' AND ');
    }
    return { sql, params };
  }

  // Internal helper to build DELETE queries.
  private buildDeleteQuery(params: any[]): { sql: string, params: any[] } {
    let sql = `DELETE FROM ${this.fromTable}`;
    if (this.whereConditions.length > 0) { // Reuse WHERE logic
       sql += ' WHERE ';
      const whereExpressions = this.whereConditions.map(condition => {
        if ((condition.operator === 'in' || condition.operator === 'not in') && Array.isArray(condition.value)) {
          const placeholders = Array(condition.value.length).fill('?').join(', ');
          params.push(...condition.value);
          return `${condition.column} ${condition.operator} (${placeholders})`;
        } else {
          params.push(condition.value);
          return `${condition.column} ${condition.operator} ?`;
        }
      });
      sql += whereExpressions.join(' AND ');
    }
    return { sql, params };
  }
}

// ----- Database class -----

/**
 * Main database interaction class.
 * Provides a fluent API for building and executing SQL queries.
 * 
 * @template DBType The user-defined Database interface, mapping table names to definitions.
 */
export class DB<DBType extends Database = Database> {
  private client: DBClient; // The underlying client (e.g., from queryState) used to execute queries.
  private transactionId: string | null; // Stores the transaction ID if this instance is within a transaction.

  /**
   * Creates a DB instance.
   * @param client The DBClient responsible for communication.
   * @param transactionId Optional transaction ID for transaction-scoped instances.
   */
  constructor(client: DBClient, transactionId: string | null = null) {
    this.client = client;
    this.transactionId = transactionId;
  }

  /** Starts a SELECT query chain. */
  selectFrom<T extends keyof DBType & string>(table: T): QueryBuilder<DBType, T> {
    return new QueryBuilder<DBType, T>(this.client, this.transactionId, table);
  }

  /** Starts an INSERT query chain. Requires `.values().execute()` to complete. */
  insertInto<T extends keyof DBType & string>(table: T): Pick<QueryBuilder<DBType, T>, 'values' | 'execute'> {
    console.log("___insertInto", table)
    const qb = new QueryBuilder<DBType, T>(this.client, this.transactionId, table);
    // Return only the necessary methods for insert flow
    return {
        values: qb.values.bind(qb), // Ensure 'this' context is bound correctly
        execute: qb.execute.bind(qb)
    };
  }

  /** Starts an UPDATE query chain. Requires `.set()` and typically `.where().execute()` to complete. */
  update<T extends keyof DBType & string>(table: T): Pick<QueryBuilder<DBType, T>, 'set' | 'where' | 'whereEquals' | 'whereIn' | 'whereLike' | 'execute'> {
    const qb = new QueryBuilder<DBType, T>(this.client, this.transactionId, table);
    // Return only the necessary methods for update flow
    return {
        set: qb.set.bind(qb),
        where: qb.where.bind(qb),
        whereEquals: qb.whereEquals.bind(qb),
        whereIn: qb.whereIn.bind(qb),
        whereLike: qb.whereLike.bind(qb),
        execute: qb.execute.bind(qb)
    };
  }

  /** Starts a DELETE query chain. Typically requires `.where().execute()` to complete. */
  deleteFrom<T extends keyof DBType & string>(table: T): Pick<QueryBuilder<DBType, T>, 'where' | 'whereEquals' | 'whereIn' | 'whereLike' | 'execute'> {
    const qb = new QueryBuilder<DBType, T>(this.client, this.transactionId, table);
    qb['isDeleteOperation'] = true; // Set internal flag
    // Return only the necessary methods for delete flow
     return {
        where: qb.where.bind(qb),
        whereEquals: qb.whereEquals.bind(qb),
        whereIn: qb.whereIn.bind(qb),
        whereLike: qb.whereLike.bind(qb),
        execute: qb.execute.bind(qb)
    };
  }

  /** Executes a raw SQL query. Use with caution. */
  async raw<T = Record<string, any>>(sql: string, params?: any[]): Promise<T[]> {
    let result;
    if (this.transactionId) {
       result = await this.client.queryWithTransaction<T>(this.transactionId, sql, params);
    } else {
       result = await this.client.standardQuery<T>(sql, params);
    }
    if (!result.ok || !result.json) {
      throw new Error(`Raw query failed: Status ${result.status}, Response: ${JSON.stringify(result.json)}`);
    }
    return result.json;
  }

  /**
   * Executes a callback within a database transaction.
   * The callback receives a transaction-specific DB instance.
   * Automatically commits if the callback succeeds, rolls back on error.
   * @param callback An async function receiving the transaction DB instance (`trx`).
   */
  async transaction<T>(callback: (trx: DB<DBType>) => Promise<T>): Promise<T> {
    if (this.transactionId) {
      throw new Error("Cannot begin nested transaction");
    }
    
    const txResult = await this.client.beginTransaction();
    if (!txResult.ok || !txResult.json) {
      throw new Error(`Failed to start transaction: ${JSON.stringify(txResult.json)}`);
    }
    const txId = txResult.json.transaction_id;
    
    // Create a new DB instance specifically for this transaction
    const trx = new DB<DBType>(this.client, txId);

    try {
      const result = await callback(trx); // Pass the transaction DB to the callback
      await this.client.commitTransaction(txId); // Commit using the original client
      return result;
    } catch (error) {
      console.error("Transaction rolling back due to error:", error);
      await this.client.rollbackTransaction(txId); // Rollback using the original client
      throw error;
    }
  }
}

// ----- Factory function to create DB instance -----

/**
 * Creates a new DB instance, the main entry point for using the query builder.
 * @template DBType The user-defined Database interface.
 * @param client The DBClient instance (e.g., from `createQueryState`).
 * @returns A new DB instance typed with your Database interface.
 */
export function createDB<DBType extends Database = Database>(client: DBClient): DB<DBType> {
  return new DB<DBType>(client);
} 