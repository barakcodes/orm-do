import { CompiledQuery, DatabaseConnection, Dialect, DialectAdapter, Driver, Kysely, QueryCompiler, QueryResult as KyselyQueryResult, SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler, TransactionSettings } from 'kysely';
import { DBClient, createDBClient, DBConfig, RawQueryResult } from './queryState';

/**
 * Configuration for the ORM-DO Kysely dialect.
 * Can be initialized either with an existing DBClient instance
 * or with the parameters needed to create one.
 */
export type ORMDODialectConfig =
  | {
      client: DBClient;
    }
  | {
      doNamespace: DurableObjectNamespace;
      config: DBConfig;
      name?: string;
    };

/**
 * Kysely dialect for ORM-DO.
 * Adapts the DBClient to be used with Kysely.
 */
export class ORMDODialect implements Dialect {
  readonly #config: ORMDODialectConfig;

  constructor(config: ORMDODialectConfig) {
    this.#config = config;
  }

  createAdapter(): DialectAdapter {
    return new SqliteAdapter();
  }

  createDriver(): Driver {
    let client: DBClient;
    let closeClient: boolean = false;

    if ('client' in this.#config) {
      client = this.#config.client;
    } else if (this.#config.doNamespace) {
      client = createDBClient(
        this.#config.doNamespace,
        this.#config.config,
        this.#config.name
      );
    } else {
      throw new Error(
        'Invalid ORMDODialect config: requires either `client` or `doNamespace` and `config`.'
      );
    }

    return new ORMDODriver(client, closeClient);
  }

  createIntrospector(db: Kysely<any>): SqliteIntrospector {
    return new SqliteIntrospector(db);
  }

  createQueryCompiler(): SqliteQueryCompiler {
    return new SqliteQueryCompiler();
  }
}

/**
 * Kysely driver for ORM-DO.
 */
export class ORMDODriver implements Driver {
  readonly #dbClient: DBClient;
  readonly #closeClient: boolean;

  constructor(dbClient: DBClient, closeClient: boolean) {
    this.#dbClient = dbClient;
    this.#closeClient = closeClient;
  }

  async init(): Promise<void> {
    // Initialization is handled by DBClient lazily
  }

  async acquireConnection(): Promise<ORMDOConnection> {
    // Return a new connection wrapper, managing transaction state
    return new ORMDOConnection(this.#dbClient);
  }

  // These methods delegate state management to the ORMDOConnection instance
  async beginTransaction(connection: ORMDOConnection, _settings: TransactionSettings): Promise<void> {
    await connection.beginTransaction();
  }

  async commitTransaction(connection: ORMDOConnection): Promise<void> {
    await connection.commitTransaction();
  }

  async rollbackTransaction(connection: ORMDOConnection): Promise<void> {
    await connection.rollbackTransaction();
  }

  async releaseConnection(_connection: ORMDOConnection): Promise<void> {
    // No-op
  }

  async destroy(): Promise<void> {
    // No-op (DBClient is not closable currently)
  }
}

/**
 * Kysely connection wrapper for ORM-DO DBClient.
 * Uses buffering for transactions because the underlying DO API doesn't support
 * stateful transactions across requests.
 */
export class ORMDOConnection implements DatabaseConnection {
  readonly #dbClient: DBClient;
  #transactionActive = false;
  #transactionStatements: string[] = [];
  #transactionParams: any[][] = [];

  constructor(dbClient: DBClient) {
    this.#dbClient = dbClient;
  }

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<KyselyQueryResult<R>> {
    if (this.#transactionActive) {
      // --- Inside a Kysely Transaction --- 
      // Buffer the query
      this.#transactionStatements.push(compiledQuery.sql);
      // Ensure parameters is mutable for the buffer
      const paramsCopy = Array.isArray(compiledQuery.parameters) ? [...compiledQuery.parameters] : [];
      this.#transactionParams.push(paramsCopy);

      // Return dummy result - actual results are not available within the tx block
      return {
        rows: [] as R[],
        numAffectedRows: BigInt(0),
      };
    } else {
      // --- Single Query Execution --- 
      // Use rawQuery to get metadata including rows_written (affected rows).
      // Ensure parameters is mutable for the client call
      const parameters = Array.isArray(compiledQuery.parameters) ? [...compiledQuery.parameters] : [];
      const result = await this.#dbClient.rawQuery(
        compiledQuery.sql,
        parameters,
      );

      if (!result.ok || !result.json) {
        const statusInfo = result.status ? ` (status: ${result.status})` : '';
        throw new Error(`Query failed${statusInfo}. SQL: ${compiledQuery.sql}`);
      }

      const rawResult = result.json as RawQueryResult;
      const rows = this.#transformRawRows<R>(rawResult);

      return {
        rows: rows,
        numAffectedRows: BigInt(rawResult.meta?.rows_written ?? 0),
      };
    }
  }

  // Helper to convert raw results (columns + rows array) to an array of objects.
  #transformRawRows<R>(rawResult: RawQueryResult): R[] {
    const { columns, rows } = rawResult;
    if (!rows || !columns) {
      return [];
    }
    return rows.map((row) => {
      const obj: Record<string, any> = {};
      columns.forEach((colName, index) => {
        obj[colName] = row[index];
      });
      return obj as R;
    });
  }

  async beginTransaction(): Promise<void> {
    if (this.#transactionActive) {
      throw new Error('Transaction already in progress');
    }
    this.#transactionActive = true;
    this.#transactionStatements = [];
    this.#transactionParams = [];
    console.log("Kysely TX: Began (buffered)");
  }

  async commitTransaction(): Promise<void> {
    if (!this.#transactionActive) {
      throw new Error('Cannot commit: No transaction in progress');
    }

    if (this.#transactionStatements.length === 0) {
        console.log("Kysely TX: Commit (no statements)");
        this.#transactionActive = false; 
        return;
    }

    const transactionPayload = this.#transactionStatements.map((sql, i) => ({
      sql,
      params: this.#transactionParams[i],
    }));
    
    console.log(`Kysely TX: Committing ${transactionPayload.length} statements...`);
    
    // Clear state *before* executing, in case of error
    const statementsToExecute = [...this.#transactionStatements]; // Copy for logging on error
    this.#transactionActive = false;
    this.#transactionStatements = [];
    this.#transactionParams = [];
    
    try {
      // Execute the entire transaction batch using the transactionQuery method
      const result = await this.#dbClient.transactionQuery(transactionPayload);
      
      if (!result.ok) {
        const errorInfo = result.json ? JSON.stringify(result.json) : `status ${result.status}`;
        console.error("Kysely TX: Commit FAILED", errorInfo);
        throw new Error(`Transaction batch failed: ${errorInfo}`);
      }
      console.log("Kysely TX: Committed successfully");
    } catch (error) {
        console.error("Kysely TX: Error during commit execution:", error);
        console.error("Failed batch SQL:", statementsToExecute);
        throw error; // Re-throw the error
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.#transactionActive) {
      console.log("Kysely TX: Rollback called but no active transaction.");
      return;
    }
    console.log(`Kysely TX: Rolling back ${this.#transactionStatements.length} buffered statements.`);
    this.#transactionActive = false;
    this.#transactionStatements = [];
    this.#transactionParams = [];
  }

  async *streamQuery<R>(
    _compiledQuery: CompiledQuery,
    _chunkSize: number
  ): AsyncIterableIterator<KyselyQueryResult<R>> {
    throw new Error('Streaming is not supported by the ORM-DO Kysely dialect');
  }
}
