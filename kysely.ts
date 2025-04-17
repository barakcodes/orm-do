import { CompiledQuery, DatabaseConnection, Dialect, DialectAdapter, Driver, Kysely, QueryCompiler, QueryResult as KyselyQueryResult, SqliteAdapter, SqliteIntrospector, SqliteQueryCompiler, TransactionSettings } from 'kysely';
import { DBClient, createDBClient, DBConfig, RawQueryResult } from './queryState';

// Generic Result type for success/failure responses
export interface Result<T, E = Error> {
  ok: boolean;
  data?: T;
  error?: E;
}

// Result type for executeTransaction method
export interface ExecuteTransactionResult {
  rowCounts: number[];
}

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
 * Uses a preview execution approach to get real results for queries in a transaction.
 */
export class ORMDOConnection implements DatabaseConnection {
  readonly #dbClient: DBClient;
  #transactionActive = false;
  #transactionStatements: Array<{sql: string; params: any[]}> = [];
  #transactionId: string | null = null;
  #queryResults: Array<any> = [];

  constructor(dbClient: DBClient) {
    this.#dbClient = dbClient;
  }

  async executeQuery<R>(compiledQuery: CompiledQuery): Promise<KyselyQueryResult<R>> {
    if (this.#transactionActive) {
      // --- Inside a Kysely Transaction --- 
      // Create a parameter copy to avoid mutation
      const paramsCopy = Array.isArray(compiledQuery.parameters) ? [...compiledQuery.parameters] : [];
      
      // Store the query in our queue
      this.#transactionStatements.push({
        sql: compiledQuery.sql,
        params: paramsCopy
      });
      
      if (this.#transactionId) {
        try {
          // Execute the query in the transaction context immediately to get real results
          console.log(`Executing query in transaction ${this.#transactionId}: ${compiledQuery.sql.substring(0, 80)}...`);
          const result = await this.#dbClient.queryWithTransaction<R>(
            this.#transactionId,
            compiledQuery.sql,
            paramsCopy
          );

          if (!result.ok) {
            // Better error handling - provide detailed error message
            const errorMsg = result.json && typeof result.json === 'object' && 'error' in result.json
              ? result.json.error
              : `Failed with status ${result.status}`;
            
            console.error(`Transaction query execution failed: ${compiledQuery.sql}\nError: ${errorMsg}`);
            
            // Rethrow the error to properly handle transaction failure
            throw new Error(`Query failed in transaction: ${errorMsg}`);
          }

          // Store the result for later reference
          this.#queryResults.push(result.json || []);
          console.log(`Got result from transaction ${this.#transactionId}: ${Array.isArray(result.json) ? result.json.length : 0} rows`);
          
          // Return the actual result we got from the transaction
          return {
            rows: (result.json || []) as R[],
            numAffectedRows: Array.isArray(result.json) ? BigInt(result.json.length) : BigInt(0),
          };
        } catch (error) {
          // Improved error logging with more context
          console.error(`Error executing query in transaction ${this.#transactionId}:`, error);
          console.error(`Failed SQL: ${compiledQuery.sql}`);
          console.error(`Parameters:`, paramsCopy);
          
          // Rethrow to ensure proper transaction handling
          throw error;
        }
      } else {
        // Transaction ID not available yet - queue it but also execute immediately
        try {
          // Execute locally without a transaction ID yet
          const result = await this.#dbClient.rawQuery(
            compiledQuery.sql,
            paramsCopy
          );
          
          if (!result.ok) {
            const errorMsg = result.json && typeof result.json === 'object' && 'error' in result.json
              ? result.json.error
              : `Failed with status ${result.status}`;
              
            throw new Error(`Query failed: ${errorMsg}`);
          }
          
          const rawResult = result.json as RawQueryResult;
          const rows = this.#transformRawRows<R>(rawResult);
          
          // Store the result for later reference
          this.#queryResults.push(rows);
          
          return {
            rows,
            numAffectedRows: BigInt(rawResult.meta?.rows_written ?? 0),
          };
        } catch (error) {
          console.error(`Error executing local query: ${compiledQuery.sql}`, error);
          throw error;
        }
      }
    } else {
      // --- Single Query Execution --- 
      try {
        // Use rawQuery to get metadata including rows_written (affected rows)
        const parameters = Array.isArray(compiledQuery.parameters) ? [...compiledQuery.parameters] : [];
        const result = await this.#dbClient.rawQuery(
          compiledQuery.sql,
          parameters,
        );

        if (!result.ok || !result.json) {
          const errorMsg = result.json && typeof result.json === 'object' && 'error' in result.json
            ? result.json.error
            : `Failed with status ${result.status}`;
          
          throw new Error(`Query failed: ${errorMsg}. SQL: ${compiledQuery.sql}`);
        }

        const rawResult = result.json as RawQueryResult;
        const rows = this.#transformRawRows<R>(rawResult);

        return {
          rows: rows,
          numAffectedRows: BigInt(rawResult.meta?.rows_written ?? 0),
        };
      } catch (error) {
        // Improved error logging
        console.error(`Error executing query: ${compiledQuery.sql}`, error);
        throw error;
      }
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
    this.#queryResults = [];
    
    try {
      // Start a new server-side transaction
      const result = await this.#dbClient.beginTransaction();
      if (!result.ok || !result.json) {
        console.log("Failed to begin interactive transaction, falling back to buffered mode");
        this.#transactionId = null;
      } else {
        this.#transactionId = result.json.transaction_id;
        console.log(`Began interactive transaction ${this.#transactionId}`);
      }
    } catch (error) {
      console.error("Error beginning transaction:", error);
      this.#transactionId = null;
      console.log("Began transaction (buffered mode due to error)");
    }
  }

  async commitTransaction(): Promise<void> {
    if (!this.#transactionActive) {
      console.log("Commit called but no active transaction.");
      return;
    }

    if (this.#transactionStatements.length === 0) {
      console.log("Commit (no statements)");
      this.#transactionActive = false;
      this.#transactionId = null;
      this.#queryResults = [];
      return;
    }

    console.log(`Committing ${this.#transactionStatements.length} statements...`);
    
    const statements = [...this.#transactionStatements]; // Copy for execution
    const transactionId = this.#transactionId;
    
    // Clear state to prevent re-entry issues
    this.#transactionActive = false;
    this.#transactionStatements = [];
    const savedResults = [...this.#queryResults]; // Keep a copy of results
    this.#queryResults = [];
    this.#transactionId = null;
    
    try {
      if (transactionId) {
        // Use the interactive transaction API
        console.log(`Committing interactive transaction ${transactionId} with ${statements.length} operations`);
        
        const result = await this.#dbClient.commitTransaction(transactionId);
        
        if (!result.ok || !result.json || result.json.status !== 'committed') {
          const errorInfo = result.json && typeof result.json === 'object' && 'error' in result.json
            ? result.json.error
            : `Failed with status ${result.status}`;
          
          console.error(`Transaction commit failed: ${errorInfo}`);
          
          // Try rolling back if commit failed
          try {
            console.log(`Attempting rollback after failed commit for ${transactionId}`);
            await this.#dbClient.rollbackTransaction(transactionId);
          } catch (rollbackError) {
            console.error(`Rollback also failed for ${transactionId}:`, rollbackError);
          }
          
          // Throw error to signal failure to Kysely
          throw new Error(`Transaction commit failed: ${errorInfo}`);
        } else {
          console.log(`Committed interactive transaction ${transactionId} successfully`);
          
          // If we have results in the response, keep them
          if (result.json && result.json.results) {
            this.#queryResults = result.json.results;
          } else {
            // Restore our saved results
            this.#queryResults = savedResults;
          }
        }
      } else {
        // Fallback to batch mode
        try {
          const transactionPayload = statements.map((op) => ({
            sql: op.sql,
            params: op.params,
          }));
          
          // Execute the entire transaction batch using the transactionQuery method
          const result = await this.#dbClient.transactionQuery(transactionPayload);
          
          if (!result.ok) {
            const errorInfo = result.json && typeof result.json === 'object' && 'error' in result.json
              ? result.json.error
              : `Failed with status ${result.status}`;
            
            console.error(`Batch commit failed: ${errorInfo}`);
            throw new Error(`Batch transaction commit failed: ${errorInfo}`);
          } else {
            console.log("Committed batch successfully");
            this.#queryResults = savedResults; // Restore our saved results
          }
        } catch (batchError) {
          console.error("Batch mode error:", batchError);
          throw batchError; // Propagate error to Kysely
        }
      }
    } catch (error) {
      console.error("Error during commit execution:", error);
      throw error; // Propagate error to Kysely
    }
  }

  async rollbackTransaction(): Promise<void> {
    if (!this.#transactionActive) {
      console.log("Rollback called but no active transaction.");
      return;
    }

    console.log(`Rolling back ${this.#transactionStatements.length} statements...`);
    const transactionId = this.#transactionId;
    
    // Clear state first to prevent re-entry issues
    this.#transactionActive = false;
    this.#transactionStatements = [];
    this.#transactionId = null;
    this.#queryResults = [];
    
    if (!transactionId) {
      console.log("No transaction ID, rollback is local only");
      return;
    }
    
    try {
      console.log(`Rolling back interactive transaction ${transactionId}`);
      const result = await this.#dbClient.rollbackTransaction(transactionId);
      
      if (!result.ok) {
        const errorInfo = result.json && typeof result.json === 'object' && 'error' in result.json
          ? result.json.error
          : `Failed with status ${result.status}`;
        
        console.error(`Rollback failed for transaction ${transactionId}: ${errorInfo}`);
        // Still continue - the transaction will eventually be cleaned up by the server
      } else {
        console.log(`Rolled back transaction ${transactionId} successfully`);
      }
    } catch (rollbackError) {
      console.error(`Error during rollback of ${transactionId}:`, rollbackError);
      // Don't throw - the transaction will be cleaned up by the server
    }
  }

  async *streamQuery<R>(
    _compiledQuery: CompiledQuery,
    _chunkSize: number
  ): AsyncIterableIterator<KyselyQueryResult<R>> {
    throw new Error('Streaming is not supported by the ORM-DO Kysely dialect');
  }

  async executeTransaction(): Promise<Result<ExecuteTransactionResult, Error>> {
    if (!this.#transactionActive) {
      console.log("Execute called but no active transaction.");
      return { ok: false, error: new Error("No active transaction") };
    }

    if (this.#transactionStatements.length === 0) {
      console.log("No statements to execute, returning success");
      this.#transactionActive = false;
      return { ok: true, data: { rowCounts: [] } };
    }

    const statements = this.#transactionStatements.map(op => op.sql);
    const params = this.#transactionStatements.map(op => op.params);
    const transactionId = this.#transactionId;

    try {
      console.log(`Executing ${statements.length} statements for transaction ${transactionId || 'batch'}`);
      
      let rowCounts: number[] = [];
      
      if (transactionId) {
        try {
          // For each statement in the transaction, execute it through the transaction context
          for (let i = 0; i < statements.length; i++) {
            const sql = statements[i];
            const paramValues = params[i];
            
            const result = await this.#dbClient.queryWithTransaction(
              transactionId,
              sql,
              paramValues
            );
            
            if (!result.ok) {
              console.error(`Failed to execute statement ${i+1} in transaction ${transactionId}`);
              
              // Attempt to rollback
              try {
                await this.rollbackTransaction();
              } catch (rollbackError) {
                console.error(`Rollback after execution failure also failed:`, rollbackError);
              }
              
              return { ok: false, error: new Error(`Transaction execution failed at statement ${i+1}`) };
            }
            
            // Add approximate row count (not available accurately)
            rowCounts.push(Array.isArray(result.json) ? result.json.length : 0);
          }
          
          // Now commit the transaction
          const commitResult = await this.#dbClient.commitTransaction(transactionId);
          if (!commitResult.ok) {
            console.error(`Failed to commit transaction ${transactionId}`);
            return { ok: false, error: new Error("Transaction commit failed") };
          }
          
          console.log(`All operations executed and committed for transaction ${transactionId}`);
        } catch (execError) {
          console.error(`Error executing interactive transaction ${transactionId}:`, execError);
          
          // Attempt to rollback
          try {
            await this.rollbackTransaction();
          } catch (rollbackError) {
            console.error(`Rollback after execution error also failed:`, rollbackError);
          }
          
          return { ok: false, error: new Error(`Transaction execution error: ${execError.message}`) };
        }
      } else {
        // Batch transaction
        try {
          const transactionPayload = this.#transactionStatements.map(op => ({
            sql: op.sql,
            params: op.params,
          }));
          
          const result = await this.#dbClient.transactionQuery(transactionPayload);
          
          if (!result.ok) {
            console.error("Failed to execute batch transaction:", 
              result.json ? JSON.stringify(result.json) : `status ${result.status}`);
            return { ok: false, error: new Error(`Batch transaction execution failed: ${result.status}`) };
          }
          
          // Approximate row counts since we don't have precise information
          rowCounts = new Array(transactionPayload.length).fill(0);
          console.log(`All operations executed successfully for batch transaction`);
        } catch (batchError) {
          console.error("Error executing batch transaction:", batchError);
          return { ok: false, error: new Error(`Batch transaction error: ${batchError.message}`) };
        }
      }
      
      // Clear transaction state after success
      this.#transactionActive = false;
      this.#transactionStatements = [];
      this.#transactionId = null;
      this.#queryResults = [];
      
      return { ok: true, data: { rowCounts } };
    } catch (outerError) {
      console.error("Unexpected error during transaction execution:", outerError);
      
      // Clean up transaction state
      this.#transactionActive = false;
      this.#transactionStatements = [];
      this.#transactionId = null;
      this.#queryResults = [];
      
      return { ok: false, error: new Error(`Transaction outer error: ${outerError.message}`) };
    }
  }
}
