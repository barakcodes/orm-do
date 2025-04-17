/// <reference types="@cloudflare/workers-types" />
//@ts-check

export interface DBConfig {
  version?: string; // Version prefix for DO naming
  schema: string | string[]; // SQL statements to initialize schema
  authSecret?: string; // Optional secret for authenticating requests
}

// Middleware options
export interface MiddlewareOptions {
  secret?: string; // Secret for request authentication
  prefix?: string; // URL path prefix for API endpoints
}

interface QueryOptions {
  isRaw?: boolean;
  isTransaction?: boolean; // Re-added for internal batching
}

// Transaction status type
export type TransactionStatus = 'active' | 'committed' | 'rolledback';

// Transaction request/response types
export interface TransactionRequest {
  operation: 'begin' | 'commit' | 'rollback';
  transaction_id?: string;
}

export interface TransactionResponse {
  transaction_id: string;
  status: TransactionStatus;
  error?: string;
  results?: any[]; // Add results array to return query results
}

// Define specific return types based on raw vs regular format
export type RawQueryResult = {
  columns: string[];
  rows: any[][];
  meta: {
    rows_read: number;
    rows_written: number;
    // last_insert_rowid?: number | bigint; // Potentially add later
  };
};

// Extended QueryRequest to support transaction context
export type QueryRequest = {
  sql: string;
  params?: any[];
  transaction_id?: string; // Optional transaction context
};

type ArrayQueryResult<T = Record<string, any>> = T[];

// Type that depends on isRaw parameter
type QueryResponseType<T extends QueryOptions> = T["isRaw"] extends true
  ? RawQueryResult
  : ArrayQueryResult;

interface QueryResult<T> {
  json: T | null;
  status: number;
  ok: boolean;
}

// Helper to generate DO name with version
function getNameWithVersion(name: string = "root", version?: string): string {
  return version ? `${version}-${name}` : name;
}

// Get CORS headers for responses
function getCorsHeaders() {
  return {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, PATCH, PUT, DELETE, OPTIONS",
    "Access-Control-Allow-Headers":
      "Authorization, Content-Type, X-Starbase-Source, X-Data-Source",
    "Access-Control-Max-Age": "86400",
  };
}

// The main factory function that creates a query function and middleware
export function createDBClient<T extends DBConfig>(
  doNamespace: DurableObjectNamespace,
  config: T,
  name?: string,
): DBClient { // Return the specific DBClient type
  const nameWithVersion = getNameWithVersion(name, config.version);
  const id = doNamespace.idFromName(nameWithVersion);
  const obj = doNamespace.get(id);
  let initialized = false;

  // Convert schema to array if it's a string
  const schemaStatements = Array.isArray(config.schema)
    ? config.schema
    : [config.schema];

  // --- Internal Helper to Send Requests to DO ---
  async function sendToDoRequest<R = any>(path: string, body: any, method: string = 'POST'): Promise<QueryResult<R>> {
      const response = await obj.fetch(`https://dummy-url${path}`, {
        method: method,
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
      });

      let responseJson: R | null = null;
      if (response.ok && response.status !== 204) {
          try {
              responseJson = await response.json() as R;
          } catch (e) {
              console.warn(`Failed to parse JSON response from ${path} (status ${response.status}):`, e);
          }
      } else if (!response.ok) {
           try {
               const errorBody = await response.text();
               console.error(`DO request failed for ${path} (status ${response.status}): ${errorBody}`);
           } catch (e) {
                console.error(`DO request failed for ${path} (status ${response.status}), failed to read error body.`);
           }
      }

      return {
        json: responseJson,
        status: response.status,
        ok: response.ok,
      };
  }

  // --- Initialization Logic ---
  async function ensureInitialized(): Promise<boolean> {
    if (initialized) return true;
    console.log("Attempting DB Initialization...");
    const initResponse = await sendToDoRequest('/init', { schema: schemaStatements });
    if (!initResponse.ok) {
      console.error("Initialization failed:", initResponse.status);
      return false;
    }
    console.log("DB Initialized successfully.");
    initialized = true;
    return true;
  }

  // --- Core Query Function (Handles single queries and transaction batches) ---
  async function query<O extends QueryOptions>(
    options: O,
    // Can be SQL string or the transaction batch array
    sqlOrTransaction: string | { sql: string; params?: any[] }[],
    params: any[] = [], // Only used when sqlOrTransaction is string
  ): Promise<QueryResult<QueryResponseType<O>>> {
    if (!await ensureInitialized()) {
       throw new Error("Database initialization failed. Cannot proceed.");
    }

    let body: any;
    let endpoint: string;

    if (options.isTransaction && Array.isArray(sqlOrTransaction)) {
      // Transaction Batch (from Kysely commit)
      body = { transaction: sqlOrTransaction };
      // Batched transactions MUST go to the raw endpoint
      endpoint = "/query/raw";
    } else if (typeof sqlOrTransaction === 'string') {
      // Single Query
      body = {
        sql: sqlOrTransaction,
        params: params,
      };
      // Determine endpoint based on whether raw results are requested
      endpoint = options.isRaw ? "/query/raw" : "/query";
    } else {
      throw new Error('Invalid arguments provided to internal query function.');
    }

    const result = await sendToDoRequest<QueryResponseType<O>>(endpoint, body);

    if (options.isRaw && result.json && typeof result.json === 'object' && 'result' in result.json) {
        return {
            ...result,
            // @ts-ignore
            json: result.json.result as QueryResponseType<O>
        };
    }

    return result;
  }

  // --- Public Client Methods ---

  async function standardQuery<TRes = Record<string, any>>(
    sql: string,
    params?: any[],
  ): Promise<QueryResult<ArrayQueryResult<TRes>>> {
    return query<QueryOptions & { isRaw: false }>(
      { isRaw: false, isTransaction: false }, sql, params
    ) as Promise<QueryResult<ArrayQueryResult<TRes>>>;
  }

  async function rawQuery(
    sql: string,
    params?: any[],
  ): Promise<QueryResult<RawQueryResult>> {
    return query<QueryOptions & { isRaw: true }>(
      { isRaw: true, isTransaction: false }, sql, params
    );
  }

  // Reintroduce transactionQuery for batching from Kysely commit
  async function transactionQuery<TRes = Record<string, any>>(
      transaction: { sql: string; params?: any[] }[],
  ): Promise<QueryResult<RawQueryResult>> {
      // Send the batch. isRaw needs to be true because the DO endpoint is /query/raw
      // isTransaction needs to be true to trigger the batch format in `query`
      // The actual result type (ArrayQueryResult) depends on the DO's response for batches.
      // Assuming /query/raw with a batch returns results compatible with ArrayQueryResult.
      // If it returns multiple RawQueryResult, this cast needs adjustment.
      return query<QueryOptions & { isRaw: true, isTransaction: true }>(
          { isRaw: true, isTransaction: true },
          transaction // Pass the batch array
      );
  }

  // --- Middleware Function (Largely unchanged) ---
  async function middleware(
    request: Request,
    options: MiddlewareOptions = {},
  ): Promise<Response | undefined> {
    const url = new URL(request.url);
    const prefix = options.prefix || "/db";

    if (!url.pathname.startsWith(prefix)) {
      return undefined;
    }

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: getCorsHeaders() });
    }

    if (options.secret) {
      const authHeader = request.headers.get("Authorization");
      const expectedHeader = `Bearer ${options.secret}`;
      if (!authHeader || authHeader !== expectedHeader) {
        return new Response(JSON.stringify({ error: "Unauthorized" }), {
          status: 401, headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
        });
      }
    }

    const subPath = url.pathname.substring(prefix.length);

    if (subPath === "/init" && request.method === "POST") {
       if (!await ensureInitialized()) {
         return new Response(JSON.stringify({ error: "Initialization failed" }), {
            status: 500, headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
         });
       }
       return new Response(JSON.stringify({ initialized: true }), {
            headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
       });
    }

    if ((subPath === "/query" || subPath === "/query/raw") && request.method === "POST") {
      try {
        const body = await request.json() as { sql: string; params?: any[] };
        if (!body.sql) {
             return new Response(JSON.stringify({ error: "Missing 'sql' property in request body" }), {
                status: 400, headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
             });
        }
        // Middleware executes queries outside transactions
        const queryOptions: QueryOptions = { isRaw: subPath === "/query/raw", isTransaction: false };
        const result = await query(queryOptions, body.sql, body.params);

        return new Response(JSON.stringify(result.json), {
          status: result.status, headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
        });
      } catch (error: any) {
        return new Response(JSON.stringify({ error: error.message || "Query processing error" }), {
          status: 500, headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
        });
      }
    }

    return new Response(JSON.stringify({ error: "Not found" }), {
      status: 404, headers: { ...getCorsHeaders(), "Content-Type": "application/json" },
    });
  }

  // Return the client object conforming to DBClient type
  return {
    standardQuery,
    rawQuery,
    transactionQuery, // Expose batch query method
    middleware,

    // Skeleton implementations for transaction methods
    beginTransaction: async () => {
      return sendToDoRequest<TransactionResponse>('/transaction', { operation: 'begin' });
    },
    commitTransaction: async (transactionId: string) => {
      return sendToDoRequest<TransactionResponse>('/transaction', {
        operation: 'commit',
        transaction_id: transactionId
      });
    },
    rollbackTransaction: async (transactionId: string) => {
      return sendToDoRequest<TransactionResponse>('/transaction', {
        operation: 'rollback',
        transaction_id: transactionId
      });
    },
    queryWithTransaction: async <T = Record<string, any>>(
      transactionId: string,
      sql: string,
      params?: any[],
    ) => {
      return sendToDoRequest<ArrayQueryResult<T>>('/query', {
        sql,
        params,
        transaction_id: transactionId
      });
    }
  };
}

// Updated DBClient type definition
export type DBClient = {
  standardQuery: <T = Record<string, any>>(
    sql: string,
    params?: any[],
  ) => Promise<QueryResult<ArrayQueryResult<T>>>;
  rawQuery: (
    sql: string,
    params?: any[],
  ) => Promise<QueryResult<RawQueryResult>>;
  // Transaction query sends batch and returns RawQueryResult (of last statement)
  transactionQuery: (
      transaction: { sql: string; params?: any[] }[],
  ) => Promise<QueryResult<RawQueryResult>>;
  middleware: (
    request: Request,
    options?: MiddlewareOptions,
  ) => Promise<Response | undefined>;

  // Interactive transaction methods
  beginTransaction: () => Promise<QueryResult<TransactionResponse>>;
  commitTransaction: (transactionId: string) => Promise<QueryResult<TransactionResponse>>;
  rollbackTransaction: (transactionId: string) => Promise<QueryResult<TransactionResponse>>;
  queryWithTransaction: <T = Record<string, any>>(
    transactionId: string,
    sql: string,
    params?: any[],
  ) => Promise<QueryResult<ArrayQueryResult<T>>>;
};

// Durable Object implementation
export class ORMDO {
  private state: DurableObjectState;
  public sql: SqlStorage;
  private initialized: boolean = false;

  private corsHeaders = getCorsHeaders();

  // Transaction context management
  private transactionContexts: Map<string, {
    id: string;
    bookmark: any;
    operations: { sql: string; params?: any[] }[];
    readTables: Set<string>;
    writeTables: Set<string>;
    startTime: number;
    lastActivity: number;
    status: TransactionStatus;
    results: any[]; // Add array to track individual query results
  }> = new Map();

  constructor(state: DurableObjectState) {
    this.state = state;
    this.sql = state.storage.sql;
    this.state.storage.get("initialized_at").then(val => { this.initialized = !!val; });

    // Load transaction contexts from storage
    this.loadTransactionContexts();

    // Set up periodic cleanup of stale transactions
    this.setupTransactionCleaner();
  }

  // Load transaction contexts from persistent storage
  private async loadTransactionContexts() {
    try {
      const storedContexts = await this.state.storage.get("transaction_contexts");
      if (storedContexts) {
        console.log(`Loading ${Object.keys(storedContexts).length} persisted transaction contexts`);
        // Convert stored contexts back to Map with proper Set objects
        for (const [txId, ctx] of Object.entries(storedContexts)) {
          this.transactionContexts.set(txId, {
            ...ctx,
            readTables: new Set(ctx.readTables || []),
            writeTables: new Set(ctx.writeTables || []),
            results: ctx.results || []
          });
        }
      }
    } catch (error) {
      console.error("Failed to load transaction contexts:", error);
    }
  }

  // Save transaction contexts to persistent storage
  private async saveTransactionContext(txId: string, context: any) {
    try {
      // Convert Set objects to arrays for storage
      const storableContext = {
        ...context,
        readTables: Array.from(context.readTables || []),
        writeTables: Array.from(context.writeTables || []),
        results: context.results || []
      };

      // Get current contexts
      const storedContexts = await this.state.storage.get("transaction_contexts") || {};

      // Update with the new/modified context
      storedContexts[txId] = storableContext;

      // Save back to storage
      await this.state.storage.put("transaction_contexts", storedContexts);
      console.log(`Saved transaction context for ${txId}`);
    } catch (error) {
      console.error(`Failed to save transaction context for ${txId}:`, error);
    }
  }

  // Remove a transaction context from persistent storage
  private async removeTransactionContext(txId: string) {
    try {
      const storedContexts = await this.state.storage.get("transaction_contexts") || {};
      if (storedContexts && typeof storedContexts === 'object' && txId in storedContexts) {
        delete storedContexts[txId];
        await this.state.storage.put("transaction_contexts", storedContexts);
        console.log(`Removed transaction context for ${txId} from storage`);
      }
    } catch (error) {
      console.error(`Failed to remove transaction context for ${txId}:`, error);
    }
  }

  // Set up periodic transaction cleanup
  private setupTransactionCleaner() {
    const CLEANUP_INTERVAL = 60000; // 1 minute

    const cleanup = () => {
      this.cleanupStaleTransactions();
      setTimeout(cleanup, CLEANUP_INTERVAL);
    };

    setTimeout(cleanup, CLEANUP_INTERVAL);
  }

  // Clean up stale transactions
  private async cleanupStaleTransactions() {
    const now = Date.now();
    const MAX_TRANSACTION_LIFETIME = 10 * 60 * 1000; // 10 minutes
    const MAX_IDLE_TIME = 5 * 60 * 1000; // 5 minutes

    // Track which transactions need cleanup in persistent storage
    const txIdsToRemove: string[] = [];

    for (const [txId, ctx] of this.transactionContexts.entries()) {
      // Skip non-active transactions
      if (ctx.status !== 'active') continue;

      const txAge = now - ctx.startTime;
      const idleTime = now - ctx.lastActivity;

      if (txAge > MAX_TRANSACTION_LIFETIME || idleTime > MAX_IDLE_TIME) {
        console.log(`Auto-rolling back stale transaction ${txId} (age: ${txAge}ms, idle: ${idleTime}ms)`);

        // Auto-rollback stale transaction
        ctx.status = 'rolledback';
        ctx.lastActivity = now;

        // Update in persistent storage
        await this.saveTransactionContext(txId, ctx);

        // If transaction had any operations, restore from bookmark
        if (ctx.operations.length > 0) {
          try {
            await this.state.storage.onNextSessionRestoreBookmark(ctx.bookmark);
            this.state.abort();
          } catch (e) {
            console.error(`Error rolling back stale transaction ${txId}:`, e);
          }
        }
      }
    }

    // Clean up old non-active transactions
    for (const [txId, ctx] of this.transactionContexts.entries()) {
      if (ctx.status !== 'active' && now - ctx.lastActivity > 10 * 60 * 1000) {
        this.transactionContexts.delete(txId);
        txIdsToRemove.push(txId);
      }
    }

    // Remove from persistent storage
    for (const txId of txIdsToRemove) {
      await this.removeTransactionContext(txId);
    }
  }

  // Helper method to extract table names from SQL queries
  private extractAffectedTables(sql: string): string[] {
    // Simplified implementation - in production, use a proper SQL parser
    const tables: string[] = [];

    // Basic regex to extract table names
    // This is just a simple example - real implementation needs a proper SQL parser
    const fromMatch = sql.match(/FROM\s+([a-zA-Z0-9_]+)/i);
    if (fromMatch && fromMatch[1]) tables.push(fromMatch[1]);

    const joinMatch = sql.match(/JOIN\s+([a-zA-Z0-9_]+)/gi);
    if (joinMatch) {
      joinMatch.forEach(match => {
        const table = match.replace(/JOIN\s+/i, '');
        tables.push(table);
      });
    }

    const intoMatch = sql.match(/INTO\s+([a-zA-Z0-9_]+)/i);
    if (intoMatch && intoMatch[1]) tables.push(intoMatch[1]);

    const updateMatch = sql.match(/UPDATE\s+([a-zA-Z0-9_]+)/i);
    if (updateMatch && updateMatch[1]) tables.push(updateMatch[1]);

    return tables;
  }

  // Check for transaction conflicts
  private hasConflictingTransaction(currentTxId: string, tables: string[], isWrite: boolean): string | null {
    for (const [txId, ctx] of this.transactionContexts.entries()) {
      if (txId === currentTxId || ctx.status !== 'active') continue;

      // Check for write-write conflicts (most serious)
      if (isWrite) {
        for (const table of tables) {
          // If another transaction is writing to the same table
          if (ctx.writeTables.has(table)) {
            return txId; // Conflict detected
          }
        }
      }

      // Check for read-write conflicts
      // If this is a read operation but another transaction is writing to the table
      if (!isWrite) {
        for (const table of tables) {
          if (ctx.writeTables.has(table)) {
            return txId; // Conflict detected
          }
        }
      }

      // Check for write-read conflicts
      // If this is a write operation but another transaction has read from the table
      if (isWrite) {
        for (const table of tables) {
          if (ctx.readTables.has(table)) {
            return txId; // Conflict detected
          }
        }
      }
    }

    return null; // No conflicts
  }

  // Safely clean up a transaction after a delay
  private async cleanupTransactionAfterDelay(transactionId: string, delayMs: number): Promise<void> {
    // Use a Promise with a timeout instead of setTimeout
    await new Promise(resolve => setTimeout(resolve, delayMs));

    // Now perform the cleanup
    try {
      // Check if transaction still exists and needs cleanup
      const txContext = this.transactionContexts.get(transactionId);
      if (txContext && txContext.status !== 'active') {
        console.log(`Cleaning up transaction ${transactionId} after delay`);
        this.transactionContexts.delete(transactionId);
        await this.removeTransactionContext(transactionId);
      }
    } catch (error) {
      // Log but don't throw to prevent DO crashes
      console.error(`Error during delayed cleanup of transaction ${transactionId}:`, error);
    }
  }

  // Helper method to initialize the database
  private async initializeDatabase(schema?: string[]): Promise<boolean> {
    if (this.initialized) return true;

    try {
      // Use the same initialization logic as the /init endpoint
      const schemaToUse = schema || [
        `CREATE TABLE IF NOT EXISTS schema_info (
          key TEXT PRIMARY KEY,
          value TEXT
        )`
      ];

      // Use storage.transaction for initialization to ensure atomicity
      await this.state.storage.transaction(async () => {
        this.sql.exec(`
          CREATE TABLE IF NOT EXISTS schema_info (
            key TEXT PRIMARY KEY,
            value TEXT
          )
        `);

        for (const statement of schemaToUse) {
          this.sql.exec(statement);
        }

        const timestamp = new Date().toISOString();
        this.sql.exec(
          "INSERT OR REPLACE INTO schema_info (key, value) VALUES ('initialized_at', ?)",
          [timestamp],
        );

        this.initialized = true;
        console.log("Database initialized at:", timestamp);
      });

      return true;
    } catch (error) {
      console.error("Failed to initialize database:", error);
      return false;
    }
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: this.corsHeaders });
    }

    try {
      // Initialization endpoint
      if (path === "/init" && request.method === "POST") {
        const { schema } = (await request.json()) as { schema: string[] };

        // Use our common initialization method
        const success = await this.initializeDatabase(schema);

        if (!success) {
          return new Response(JSON.stringify({ error: "Database initialization failed" }), {
            status: 500,
            headers: { ...this.corsHeaders, "Content-Type": "application/json" }
          });
        }

        return new Response(JSON.stringify({ initialized_at: new Date().toISOString() }), {
          headers: { ...this.corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Standard query endpoint
      if (path === "/query" && request.method === "POST") {
        const {
          sql,
          params = [],
          transaction_id
        } = (await request.json()) as QueryRequest;

        // For non-transaction queries, ensure database is initialized
        if (!transaction_id && !this.initialized) {
          console.log("Query requested but database not initialized, initializing...");
          const success = await this.initializeDatabase();
          if (!success) {
            return new Response(JSON.stringify({
              error: "Failed to initialize database."
            }), {
              status: 500,
              headers: { ...this.corsHeaders, "Content-Type": "application/json" }
            });
          }
        }

        // Handle query within transaction context
        if (transaction_id) {
          const txContext = this.transactionContexts.get(transaction_id);
          if (!txContext || txContext.status !== 'active') {
            // Try to recover from storage if not in memory
            if (!txContext) {
              try {
                console.log(`Transaction ${transaction_id} not found in memory for query, attempting to recover from storage...`);
                const storedContexts = await this.state.storage.get("transaction_contexts") || {};
                if (storedContexts && typeof storedContexts === 'object' && transaction_id in storedContexts) {
                  const storedCtx = storedContexts[transaction_id];
                  if (storedCtx.status === 'active') {
                    // Restore transaction from storage
                    const restoredCtx = {
                      ...storedCtx,
                      readTables: new Set<string>(storedCtx.readTables || []),
                      writeTables: new Set<string>(storedCtx.writeTables || []),
                      results: storedCtx.results || []
                    };
                    this.transactionContexts.set(transaction_id, restoredCtx);
                    console.log(`Successfully recovered transaction ${transaction_id} from storage for query`);

                    // Process the request again with the recovered context
                    return this.fetch(request);
                  }
                }
              } catch (error) {
                console.error(`Failed to recover transaction ${transaction_id} from storage for query:`, error);
              }
            }

            return new Response(JSON.stringify({
              error: `Transaction not found or inactive: ${transaction_id}`,
              status: txContext?.status || 'unknown'
            }), {
              status: 404,
              headers: { ...this.corsHeaders, "Content-Type": "application/json" }
            });
          }

          // Update activity timestamp
          txContext.lastActivity = Date.now();

          try {
            // Parse SQL to identify affected tables
            const affectedTables = this.extractAffectedTables(sql);

            // Check if read-only or write operation
            const isWrite = /INSERT|UPDATE|DELETE|CREATE|DROP|ALTER/i.test(sql);

            // For write operations, track tables being written to
            if (isWrite) {
              affectedTables.forEach(table => txContext.writeTables.add(table));
            } else {
              // For read operations, track tables being read from
              affectedTables.forEach(table => txContext.readTables.add(table));
            }

            // Look for conflicts with other active transactions
            const conflictingTxId = this.hasConflictingTransaction(transaction_id, affectedTables, isWrite);
            if (conflictingTxId) {
              return new Response(JSON.stringify({
                error: `Transaction conflict detected with transaction ${conflictingTxId}`
              }), {
                status: 409, // Conflict
                headers: { ...this.corsHeaders, "Content-Type": "application/json" }
              });
            }

            // Store operation for later execution at commit time
            txContext.operations.push({ sql, params });

            // SIMPLIFIED APPROACH: Execute the query directly without preview mode
            // Just execute the query normally and get the result
            let result;
            const cursor = this.sql.exec(sql, ...params);
            result = cursor.toArray();

            // Store result for later reference
            if (!txContext.results) {
              txContext.results = [];
            }
            txContext.results.push(result);

            // Update transaction context in storage
            txContext.lastActivity = Date.now();
            await this.saveTransactionContext(transaction_id, txContext);

            console.log(`TX ${transaction_id}: Executed operation #${txContext.operations.length}: ${sql.substring(0, 80)}...`);

            return new Response(JSON.stringify(result), {
              headers: { ...this.corsHeaders, "Content-Type": "application/json" },
            });
          } catch (error: any) {
            console.error("Transaction query error:", error);
            return new Response(JSON.stringify({ error: error.message }), {
              status: 400,
              headers: { ...this.corsHeaders, "Content-Type": "application/json" },
            });
          }
        } else {
          // Regular non-transactional query handling
          const cursor = this.sql.exec(sql, ...params);
          const result = cursor.toArray();

          return new Response(JSON.stringify(result), {
            headers: { ...this.corsHeaders, "Content-Type": "application/json" },
          });
        }
      }

      // Raw query endpoint (Handles single raw queries AND transaction batches)
      if (path === "/query/raw" && request.method === "POST") {
        const data = (await request.json()) as {
          sql?: string;
          params?: any[];
          transaction?: { sql: string; params?: any[] }[];
        };

        // Ensure database is initialized before proceeding
        if (!this.initialized) {
          console.log("Raw query requested but database not initialized, initializing...");
          const success = await this.initializeDatabase();
          if (!success) {
            return new Response(JSON.stringify({
              error: "Failed to initialize database."
            }), {
              status: 500,
              headers: { ...this.corsHeaders, "Content-Type": "application/json" }
            });
          }
        }

        let finalResult: RawQueryResult;

        if (data.transaction) {
          // Handle transaction batch using storage.transaction()
          const results: RawQueryResult[] = [];
          await this.state.storage.transaction(async () => {
              for (const query of data.transaction!) {
                  const cursor = this.sql.exec(query.sql, ...(query.params || []));
                  results.push({
                      columns: cursor.columnNames,
                      rows: Array.from(cursor.raw()), // Convert Iterator to Array
                      meta: {
                          rows_read: cursor.rowsRead,
                          rows_written: cursor.rowsWritten,
                      },
                  });
              }
          });
          // Return the result of the last statement in the batch
          finalResult = results.length > 0 ? results[results.length - 1] : { columns: [], rows: [], meta: { rows_read: 0, rows_written: 0 } };
          finalResult.rows = []; // Kysely expects empty rows from buffered transaction executeQuery

        } else if (data.sql) {
          // Handle single raw query
          const cursor = this.sql.exec(data.sql, ...(data.params || []));
          finalResult = {
            columns: cursor.columnNames,
            rows: Array.from(cursor.raw()), // Convert Iterator to Array
            meta: {
              rows_read: cursor.rowsRead,
              rows_written: cursor.rowsWritten,
            },
          };
        } else {
            throw new Error("Invalid request to /query/raw: missing 'sql' or 'transaction'");
        }

       return new Response(JSON.stringify(finalResult), {
         headers: { ...this.corsHeaders, "Content-Type": "application/json" },
       });
      }

      // Transaction management endpoint
      if (path === "/transaction" && request.method === "POST") {
        const { operation, transaction_id } = await request.json() as TransactionRequest;

        // Instead of failing, ensure database is initialized on demand
        if (!this.initialized) {
          console.log("Transaction requested but database not initialized, initializing on demand...");
          const success = await this.initializeDatabase();
          if (!success) {
            return new Response(JSON.stringify({
              error: "Failed to initialize database on demand."
            }), {
              status: 500, // Internal Server Error
              headers: { ...this.corsHeaders, "Content-Type": "application/json" }
            });
          }
        }

        if (operation === "begin") {
          const txId = crypto.randomUUID();
          const bookmark = await this.state.storage.getCurrentBookmark();

          const txContext = {
            id: txId,
            bookmark,
            operations: [] as { sql: string; params?: any[] }[],
            readTables: new Set<string>(),
            writeTables: new Set<string>(),
            startTime: Date.now(),
            lastActivity: Date.now(),
            status: 'active' as TransactionStatus,
            results: [] // Initialize results array
          };

          // Store in memory
          this.transactionContexts.set(txId, txContext);

          // Persist to storage
          await this.saveTransactionContext(txId, txContext);

          return new Response(JSON.stringify({
            transaction_id: txId,
            status: 'active'
          } as TransactionResponse), {
            headers: { ...this.corsHeaders, "Content-Type": "application/json" }
          });
        }

        if (operation === "commit" || operation === "rollback") {
          if (!transaction_id) {
            return new Response(JSON.stringify({ error: "Missing transaction_id" }), {
              status: 400,
              headers: { ...this.corsHeaders, "Content-Type": "application/json" }
            });
          }

          const txContext = this.transactionContexts.get(transaction_id);
          if (!txContext || txContext.status !== 'active') {
            // Try to recover from storage if not in memory
            if (!txContext) {
              try {
                console.log(`Transaction ${transaction_id} not found in memory, attempting to recover from storage...`);
                const storedContexts = await this.state.storage.get("transaction_contexts") || {};
                if (storedContexts && typeof storedContexts === 'object' && transaction_id in storedContexts) {
                  const storedCtx = storedContexts[transaction_id];
                  if (storedCtx.status === 'active') {
                    // Restore transaction from storage
                    const restoredCtx = {
                      ...storedCtx,
                      readTables: new Set<string>(storedCtx.readTables || []),
                      writeTables: new Set<string>(storedCtx.writeTables || []),
                      results: storedCtx.results || []
                    };
                    this.transactionContexts.set(transaction_id, restoredCtx);
                    console.log(`Successfully recovered transaction ${transaction_id} from storage`);

                    // Process the request again with the recovered context
                    return this.fetch(request);
                  }
                }
              } catch (error) {
                console.error(`Failed to recover transaction ${transaction_id} from storage:`, error);
              }
            }

            // If we get here, the transaction couldn't be recovered
            return new Response(JSON.stringify({
              error: "Transaction not found or inactive",
              transaction_id,
              status: txContext?.status || 'unknown'
            } as TransactionResponse), {
              status: 404,
              headers: { ...this.corsHeaders, "Content-Type": "application/json" }
            });
          }

          if (operation === "commit") {
            try {
              console.log(`Committing transaction ${transaction_id} with ${txContext.operations.length} operations`);

              // Mark transaction as committed - no need to re-execute anything
              txContext.status = 'committed';
              txContext.lastActivity = Date.now();

              // Update the transaction context in persistent storage
              await this.saveTransactionContext(transaction_id, txContext);

              // Return successful response with the results we already have
              const response = new Response(JSON.stringify({
                transaction_id,
                status: 'committed',
                results: txContext.results // We already have the real results
              }), {
                headers: { ...this.corsHeaders, "Content-Type": "application/json" }
              });

              // Schedule cleanup for later without blocking the response
              this.cleanupTransactionAfterDelay(transaction_id, 60000).catch(e => {
                console.error(`Error committing transaction ${transaction_id}:`, e);
              });

              return response;
            } catch (error: any) {
              console.error("Transaction commit error:", error);
              return new Response(JSON.stringify({ error: error.message }), {
                status: 500,
                headers: { ...this.corsHeaders, "Content-Type": "application/json" },
              });
            }
          }

          if (operation === "rollback") {
            try {
              console.log(`Rolling back transaction ${transaction_id} with ${txContext.operations.length} operations`);

              // Mark transaction as rolled back - no need to re-execute anything
              txContext.status = 'rolledback';
              txContext.lastActivity = Date.now();

              // Update the transaction context in persistent storage
              await this.saveTransactionContext(transaction_id, txContext);

              // Return successful response with the results we already have
              const response = new Response(JSON.stringify({
                transaction_id,
                status: 'rolledback',
                results: txContext.results // We already have the real results
              }), {
                headers: { ...this.corsHeaders, "Content-Type": "application/json" }
              });

              // Schedule cleanup for later without blocking the response
              this.cleanupTransactionAfterDelay(transaction_id, 60000).catch(e => {
                console.error(`Error rolling back transaction ${transaction_id}:`, e);
              });

              return response;
            } catch (error: any) {
              console.error("Transaction rollback error:", error);
              return new Response(JSON.stringify({ error: error.message }), {
                status: 500,
                headers: { ...this.corsHeaders, "Content-Type": "application/json" },
              });
            }
          }
        }
      }
    } catch (error: any) {
      console.error("Error processing request:", error);
      return new Response(JSON.stringify({ error: error.message }), {
        status: 500,
        headers: { ...this.corsHeaders, "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ error: "Not found" }), {
      status: 404, headers: { ...this.corsHeaders, "Content-Type": "application/json" },
    });
  }
}
