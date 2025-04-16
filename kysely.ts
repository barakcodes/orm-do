import { CompiledQuery, DatabaseConnection, Dialect, DialectAdapter, Driver, Kysely, QueryCompiler, QueryResult as KyselyQueryResult, SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler, TransactionSettings } from 'kysely';
import { DBClient, createDBClient, DBConfig } from './queryState';

/**
 * Configuration for the ORM-DO Kysely dialect
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
 * Kysely dialect for ORM-DO
 * Adapted from kysely-libsql
 */
export class ORMDODialect implements Dialect {
  #config: ORMDODialectConfig;

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
        'Please specify either `client` or `doNamespace` and `config` in the ORMDODialect config'
      );
    }

    return new ORMDODriver(client, closeClient);
  }

  createIntrospector(db: Kysely<any>): any {
    return new SqliteIntrospector(db);
  }

  createQueryCompiler(): QueryCompiler {
    return new SqliteQueryCompiler();
  }
}

/**
 * Kysely driver for ORM-DO
 */
export class ORMDODriver implements Driver {
  readonly #dbClient: DBClient;
  readonly #closeClient: boolean;

  constructor(dbClient: DBClient, closeClient: boolean) {
    this.#dbClient = dbClient;
    this.#closeClient = closeClient;
  }

  async init(): Promise<void> {
    // No initialization needed
  }

  async acquireConnection(): Promise<DatabaseConnection> {
    return new ORMDOConnection(this.#dbClient);
  }

  async beginTransaction(
    connection: ORMDOConnection,
    _settings: TransactionSettings
  ): Promise<void> {
    await connection.beginTransaction();
  }

  async commitTransaction(connection: ORMDOConnection): Promise<void> {
    await connection.commitTransaction();
  }

  async rollbackTransaction(connection: ORMDOConnection): Promise<void> {
    await connection.rollbackTransaction();
  }

  async releaseConnection(_connection: ORMDOConnection): Promise<void> {
    // No need to release connection
  }

  async destroy(): Promise<void> {
    // No cleanup needed
  }
}

/**
 * Kysely connection for ORM-DO
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
      // Store the query for later execution in the transaction
      this.#transactionStatements.push(compiledQuery.sql);
      this.#transactionParams.push(compiledQuery.parameters as any[]);

      // Return dummy result for now
      return {
        rows: [] as R[],
        numAffectedRows: BigInt(0),
      };
    } else {
      // Execute the query directly
      const result = await this.#dbClient.standardQuery<R>(
        compiledQuery.sql,
        ...compiledQuery.parameters
      );

      if (!result.ok || !result.json) {
        throw new Error(`Query failed with status ${result.status}`);
      }

      // Convert result format to Kysely's expected format
      return {
        rows: result.json,
        // Since we don't have access to lastInsertRowid directly,
        // we'll need to rely on the query design to handle this
        numAffectedRows: BigInt(0), // Default to 0 as we can't reliably determine this
      };
    }
  }

  async beginTransaction(): Promise<void> {
    if (this.#transactionActive) {
      throw new Error('Transaction already in progress');
    }
    
    // Start a transaction by setting the transaction flag
    // and initializing the statements and params arrays
    this.#transactionActive = true;
    this.#transactionStatements = [];
    this.#transactionParams = [];
  }

  async commitTransaction(): Promise<void> {
    if (!this.#transactionActive) {
      throw new Error('No transaction to commit');
    }

    // Create transaction SQL with all stored statements
    let transactionSql = 'BEGIN TRANSACTION;';
    
    for (let i = 0; i < this.#transactionStatements.length; i++) {
      // Execute each statement in the transaction
      transactionSql += `\n${this.#transactionStatements[i]};`;
    }
    
    transactionSql += '\nCOMMIT;';
    
    // Execute the transaction
    const result = await this.#dbClient.standardQuery(transactionSql);
    
    if (!result.ok) {
      throw new Error(`Transaction failed with status ${result.status}`);
    }
    
    // Reset transaction state
    this.#transactionActive = false;
    this.#transactionStatements = [];
    this.#transactionParams = [];
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.#transactionActive) {
      throw new Error('No transaction to rollback');
    }
    
    // Reset transaction state without executing
    this.#transactionActive = false;
    this.#transactionStatements = [];
    this.#transactionParams = [];
  }

  async *streamQuery<R>(
    _compiledQuery: CompiledQuery,
    _chunkSize: number
  ): AsyncIterableIterator<KyselyQueryResult<R>> {
    // Stream queries are not supported
    throw new Error('Stream queries are not supported by ORM-DO Driver');
  }
}
