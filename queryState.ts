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

// WebSocket connection URL for clients
export interface WebSocketConfig {
  baseUrl: string; // Base URL for WebSocket connection
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
    "Authorization, Content-Type",
    "Access-Control-Max-Age": "86400",
  };
}

// Utility function for returning consistent JSON responses
function json(data: any, status: number = 200, init: ResponseInit = {}): Response {
  const mergedHeaders = {
    "Content-Type": "application/json",
    ...getCorsHeaders(),
    ...init.headers,
  };
  
  // Create a new ResponseInit without headers
  const { headers, ...restInit } = init;
  
  return new Response(
    JSON.stringify(data), 
    { 
      status, 
      headers: mergedHeaders,
      ...restInit
    }
  );
}

// Utility function for OPTIONS requests and other empty responses
function emptyResponse(status: number = 204, headers = getCorsHeaders()): Response {
  return new Response(null, { status, headers });
}

/**
 * Creates a database client with HTTP and WebSocket support
 * 
 * Example usage with WebSockets:
 * ```typescript
 * // Create the client
 * const dbClient = createDBClient(env.ORMDO, { schema: SCHEMA });
 * 
 * // From your frontend JavaScript/TypeScript:
 * const wsUrl = dbClient.getWebSocketUrl();
 * const socket = new WebSocket(wsUrl);
 * 
 * // Set up event handlers
 * socket.onopen = () => {
 *   console.log('Connected to database WebSocket');
 * };
 * 
 * socket.onmessage = (event) => {
 *   const response = JSON.parse(event.data);
 *   console.log('Received:', response);
 *   
 *   // Handle different response types
 *   switch (response.type) {
 *     case 'connection_established':
 *       // Connection established successfully
 *       console.log('Connected with session ID:', response.wsSessionId);
 *       break;
 *     case 'query_result':
 *       // Process query results
 *       console.log('Query result for request:', response.requestId);
 *       processResults(response.result);
 *       break;
 *     case 'transaction_result':
 *       // Handle transaction results
 *       console.log('Transaction:', response.transaction_id, response.status);
 *       break;
 *     case 'database_change':
 *       // Change Data Capture (CDC) event
 *       console.log('Database change in tables:', response.tables);
 *       console.log('Operation:', response.operation);
 *       console.log('Rows affected:', response.rowsAffected);
 *       
 *       // Refresh UI or data based on changes
 *       if (response.tables.includes('users')) {
 *         refreshUsersList();
 *       }
 *       break;
 *     case 'transaction_committed':
 *       // Transaction commit CDC event
 *       console.log('Transaction committed affecting tables:', response.tables);
 *       // Refresh affected data
 *       break;
 *   }
 * };
 * 
 * // Execute a query
 * socket.send(JSON.stringify({
 *   action: 'query',
 *   requestId: 'unique-query-id',
 *   sql: 'SELECT * FROM users LIMIT 5',
 *   params: []
 * }));
 * 
 * // Start a transaction
 * socket.send(JSON.stringify({
 *   action: 'transaction',
 *   operation: 'begin',
 *   requestId: 'tx-request-1'
 * }));
 * 
 * // Use a transaction
 * socket.send(JSON.stringify({
 *   action: 'query',
 *   requestId: 'tx-query-1',
 *   sql: 'INSERT INTO users (name) VALUES (?)',
 *   params: ['User 1'],
 *   transaction_id: 'received-transaction-id' // From the 'begin' response
 * }));
 * 
 * // Commit a transaction
 * socket.send(JSON.stringify({
 *   action: 'transaction',
 *   operation: 'commit',
 *   requestId: 'tx-commit-1',
 *   transaction_id: 'received-transaction-id'
 * }));
 * ```
 * 
 * Change Data Capture (CDC):
 * WebSocket connections automatically receive change notifications when data is
 * modified in the database. This enables real-time updates in your application
 * without polling. The server broadcasts events to all connected clients when:
 * - INSERT, UPDATE, or DELETE operations are executed
 * - Transactions with write operations are committed
 * 
 * CDC events include:
 * - The affected tables
 * - The type of operation
 * - Number of rows affected
 * - Timestamp of the change
 */
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

  // Generate WebSocket connection URL
  function getWebSocketUrl(options: { secure?: boolean, host?: string } = {}): string {
    const protocol = options.secure !== false ? 'wss' : 'ws';
    const host = options.host || (typeof location !== 'undefined' ? location.host : '');
    return `${protocol}://${host}/socket`;
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
      return emptyResponse();
    }

    if (options.secret) {
      const authHeader = request.headers.get("Authorization");
      const expectedHeader = `Bearer ${options.secret}`;
      if (!authHeader || authHeader !== expectedHeader) {
        return json({ error: "Unauthorized" }, 401);
      }
    }

    const subPath = url.pathname.substring(prefix.length);

    if (subPath === "/init" && request.method === "POST") {
       if (!await ensureInitialized()) {
         return json({ error: "Initialization failed" }, 500);
       }
       return json({ initialized: true });
    }

    if ((subPath === "/query" || subPath === "/query/raw") && request.method === "POST") {
      try {
        const body = await request.json() as { sql: string; params?: any[] };
        if (!body.sql) {
             return json({ error: "Missing 'sql' property in request body" }, 400);
        }
        // Middleware executes queries outside transactions
        const queryOptions: QueryOptions = { isRaw: subPath === "/query/raw", isTransaction: false };
        const result = await query(queryOptions, body.sql, body.params);

        return json(result.json, result.status);
      } catch (error: any) {
        return json({ error: error.message || "Query processing error" }, 500);
      }
    }

    return json({ error: "Not found" }, 404);
  }

  // Return the client object conforming to DBClient type
  return {
    standardQuery,
    rawQuery,
    transactionQuery, // Expose batch query method
    middleware,
    
    // WebSocket connection helper
    getWebSocketUrl,

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
  
  // WebSocket utilities
  getWebSocketUrl: (options?: { secure?: boolean, host?: string }) => string;

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

  // WebSocket connection management
  private connections = new Map<string, WebSocket>();

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
    
    // Initialize database in constructor
    this.state.storage.get("initialized_at").then(val => { 
      this.initialized = !!val;
      if (!this.initialized) {
        this.initializeDatabase().catch(err => 
          console.error("Failed to initialize database:", err));
      }
    });

    // Load transaction contexts from storage
    this.loadTransactionContexts();

    // Set up periodic cleanup of stale transactions
    this.setupTransactionCleaner();
    
    // Set up WebSocket event handlers for any existing connections
    this.state.getWebSockets().forEach(ws => {
      this.setupWebSocketHandlers(ws);
    });
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

  // Clean up a transaction after a delay
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

  // Broadcast an event to all connected WebSocket clients
  private broadcastEvent(event: { type: string; [key: string]: any }) {
    const message = JSON.stringify(event);
    console.log(`Broadcasting event ${event.type} to ${this.connections.size} clients`);
    
    // Send to all connected clients
    for (const [sessionId, ws] of this.connections.entries()) {
      try {
        ws.send(message);
      } catch (err) {
        console.error(`Failed to send to client ${sessionId}:`, err);
        // Remove broken connections
        this.connections.delete(sessionId);
      }
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

  // Create a method to handle new WebSocket connections
  private handleWebSocketConnection(): Response {
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);
    const wsSessionId = crypto.randomUUID();

    // Accept the WebSocket connection
    this.state.acceptWebSocket(server);
    
    // Store the connection with its ID
    this.connections.set(wsSessionId, server);
    
    // Set up event handlers
    this.setupWebSocketHandlers(server, wsSessionId);
    
    return new Response(null, { 
      status: 101, 
      webSocket: client 
    });
  }

  // Set up handlers for WebSocket events
  private setupWebSocketHandlers(ws: WebSocket, wsSessionId?: string) {
    ws.addEventListener('message', async (event) => {
      try {
        const message = JSON.parse(event.data);
        await this.handleWebSocketMessage(ws, message);
      } catch (error) {
        console.error("Error processing WebSocket message:", error);
        ws.send(JSON.stringify({
          error: "Failed to process message"
        }));
      }
    });
    
    ws.addEventListener('close', (event) => {
      // Clean up connection
      if (wsSessionId) {
        this.connections.delete(wsSessionId);
      }
      console.log(`WebSocket connection closed: ${event.code}`);
    });
    
    // Send initial connection confirmation
    ws.send(JSON.stringify({
      type: 'connection_established',
      wsSessionId
    }));
  }

  // Handle different types of WebSocket messages
  private async handleWebSocketMessage(ws: WebSocket, message: any) {
    // Ensure DB is initialized
    if (!this.initialized) {
      await this.initializeDatabase();
    }
    
    try {
      // Route to appropriate handler based on action type
      switch (message.action) {
        case 'query':
          await this.handleQueryMessage(ws, message);
          break;
        case 'transaction':
          await this.handleTransactionMessage(ws, message);
          break;
        default:
          ws.send(JSON.stringify({
            type: 'error',
            requestId: message.requestId,
            error: `Unknown action type: ${message.action}`
          }));
      }
    } catch (error: any) {
      console.error("Error processing WebSocket message:", error);
      ws.send(JSON.stringify({
        type: 'error',
        requestId: message.requestId,
        error: error.message || "An unknown error occurred"
      }));
    }
  }
  
  // Handle query messages from WebSockets
  private async handleQueryMessage(ws: WebSocket, message: any) {
    const { sql, params = [], transaction_id, requestId } = message;
    
    // Handle query within transaction context
    if (transaction_id) {
      await this.executeTransactionQuery(ws, sql, params, transaction_id, requestId);
    } else {
      // Regular non-transactional query
      await this.executeStandardQuery(ws, sql, params, requestId);
    }
  }
  
  // Execute a query within transaction context for WebSockets
  private async executeTransactionQuery(
    ws: WebSocket, 
    sql: string, 
    params: any[] = [], 
    transaction_id: string, 
    requestId: string
  ) {
    try {
      // Check if transaction needs to be recovered
      const txContext = this.transactionContexts.get(transaction_id);
      if (!txContext || txContext.status !== 'active') {
        const recovered = await this.recoverTransactionFromStorage(transaction_id);
        if (!recovered) {
          throw new Error(`Transaction not found or inactive: ${transaction_id}`);
        }
      }
      
      // Use common implementation to execute the query
      const result = await this.executeQueryInTransaction(sql, params, transaction_id);
      
      // Send response back to client
      ws.send(JSON.stringify({
        type: 'query_result',
        requestId,
        result,
        transaction_id
      }));
    } catch (error: any) {
      ws.send(JSON.stringify({
        type: 'error',
        requestId,
        error: error.message
      }));
    }
  }
  
  // Handle transaction operation messages from WebSockets
  private async handleTransactionMessage(ws: WebSocket, message: any) {
    const { operation, transaction_id, requestId } = message;
    
    switch (operation) {
      case 'begin':
        await this.handleBeginTransaction(ws, requestId);
        break;
      case 'commit':
      case 'rollback':
        if (!transaction_id) {
          throw new Error('Missing transaction_id for commit/rollback operation');
        }
        await this.handleEndTransaction(ws, operation, transaction_id, requestId);
        break;
      default:
        throw new Error(`Unknown transaction operation: ${operation}`);
    }
  }
  
  // Handle beginning a new transaction via WebSocket
  private async handleBeginTransaction(ws: WebSocket, requestId: string) {
    try {
      // Use common implementation
      const result = await this.createTransaction();
      
      ws.send(JSON.stringify({
        type: 'transaction_result',
        requestId,
        transaction_id: result.transaction_id,
        status: result.status
      }));
    } catch (error: any) {
      ws.send(JSON.stringify({
        type: 'error',
        requestId,
        error: error.message
      }));
    }
  }
  
  // Handle committing or rolling back a transaction via WebSocket
  private async handleEndTransaction(
    ws: WebSocket, 
    operation: 'commit' | 'rollback', 
    transaction_id: string, 
    requestId: string
  ) {
    try {
      // Check if transaction needs to be recovered
      const txContext = this.transactionContexts.get(transaction_id);
      if (!txContext || txContext.status !== 'active') {
        const recovered = await this.recoverTransactionFromStorage(transaction_id);
        if (!recovered) {
          throw new Error(`Transaction not found or inactive: ${transaction_id}`);
        }
      }
      
      // Use common implementation to finalize the transaction
      const result = await this.finalizeTransaction(transaction_id, operation);
      
      if (!result.success) {
        throw new Error(result.error || `Failed to ${operation} transaction`);
      }
      
      ws.send(JSON.stringify({
        type: 'transaction_result',
        requestId,
        transaction_id: result.transaction_id,
        status: result.status,
        results: result.results
      }));
    } catch (error: any) {
      ws.send(JSON.stringify({
        type: 'error',
        requestId,
        error: error.message
      }));
    }
  }

  // Common function to execute a query within a transaction context
  private async executeQueryInTransaction(
    sql: string,
    params: any[] = [],
    transaction_id: string
  ): Promise<any> {
    console.log("___query", sql, params)
    const txContext = this.transactionContexts.get(transaction_id);
    if (!txContext || txContext.status !== 'active') {
      throw new Error(`Transaction not found or inactive: ${transaction_id}`);
    }
    
    // Update activity timestamp
    txContext.lastActivity = Date.now();
    
    // Check for table conflicts
    const tablesInfo = this.analyzeQueryTables(sql);
    
    // Update tables being read/written
    if (tablesInfo.isWrite) {
      tablesInfo.tables.forEach(table => txContext.writeTables.add(table));
    } else {
      tablesInfo.tables.forEach(table => txContext.readTables.add(table));
    }
    
    // Check for conflicts with other transactions
    const conflictingTxId = this.hasConflictingTransaction(transaction_id, tablesInfo.tables, tablesInfo.isWrite);
    if (conflictingTxId) {
      throw new Error(`Transaction conflict detected with transaction ${conflictingTxId}`);
    }
    
    // Store operation for later
    txContext.operations.push({ sql, params });
    
    // Execute the query
    const cursor = this.sql.exec(sql, ...params);
    const result = cursor.toArray();
    
    // Store result for reference
    txContext.results.push(result);
    
    // Update storage
    await this.saveTransactionContext(transaction_id, txContext);
    
    // Check if we should broadcast CDC events
    if (tablesInfo.isWrite) {
      this.broadcastChangeDataEvent(sql, cursor.rowsWritten, tablesInfo.tables);
    }
    
    console.log(`TX ${transaction_id}: Executed operation #${txContext.operations.length}: ${sql.substring(0, 80)}...`);
    
    return result;
  }

  // Execute a standard (non-transaction) query
  private async executeStandardQuery(
    ws: WebSocket, 
    sql: string, 
    params: any[] = [], 
    requestId: string
  ) {
    console.log("___query", sql, params)
    try {
      const cursor = this.sql.exec(sql, ...params);
      const result = cursor.toArray();
      
      // Check if we should broadcast CDC events
      const tablesInfo = this.analyzeQueryTables(sql);
      if (tablesInfo.isWrite) {
        this.broadcastChangeDataEvent(sql, cursor.rowsWritten, tablesInfo.tables);
      }
      
      ws.send(JSON.stringify({
        type: 'query_result',
        requestId,
        result
      }));
    } catch (error: any) {
      ws.send(JSON.stringify({
        type: 'error',
        requestId,
        error: error.message
      }));
    }
  }
  
  // Helper to analyze tables and write status from a query
  private analyzeQueryTables(sql: string): { tables: string[], isWrite: boolean } {
    const tables = this.extractAffectedTables(sql);
    const isWrite = /INSERT|UPDATE|DELETE|CREATE|DROP|ALTER/i.test(sql);
    return { tables, isWrite };
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const path = url.pathname;

    if (request.method === "OPTIONS") {
      return emptyResponse();
    }

    // Handle WebSocket connections
    if (path === "/socket") {
      return this.handleWebSocketConnection();
    }

    try {
      // Route to appropriate endpoint handler
      if (path === "/init" && request.method === "POST") {
        return this.handleInitRequest(request);
      }
      
      if (path === "/query" && request.method === "POST") {
        return this.handleQueryRequest(request);
      }
      
      if (path === "/query/raw" && request.method === "POST") {
        return this.handleRawQueryRequest(request);
      }
      
      if (path === "/transaction" && request.method === "POST") {
        return this.handleTransactionRequest(request);
      }

      // No matching handler found
      return json({ error: "Not found" }, 404);
    } catch (error: any) {
      console.error("Error processing request:", error);
      return json({ error: error.message }, 500);
    }
  }
  
  // Handle database initialization requests
  private async handleInitRequest(request: Request): Promise<Response> {
    const { schema } = (await request.json()) as { schema: string[] };

    // Use our common initialization method
    const success = await this.initializeDatabase(schema);

    if (!success) {
      return json({ error: "Database initialization failed" }, 500);
    }

    return json({ initialized_at: new Date().toISOString() });
  }
  
  // Handle standard query requests
  private async handleQueryRequest(request: Request): Promise<Response> {
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
        return json({ error: "Failed to initialize database." }, 500);
      }
    }

    // Handle query within transaction context
    if (transaction_id) {
      return this.executeHttpTransactionQuery(sql, params, transaction_id);
    } else {
      // Regular non-transactional query
      const cursor = this.sql.exec(sql, ...params);
      const result = cursor.toArray();
      
      // Check if we should broadcast CDC events
      const tablesInfo = this.analyzeQueryTables(sql);
      if (tablesInfo.isWrite) {
        this.broadcastChangeDataEvent(sql, cursor.rowsWritten, tablesInfo.tables);
      }

      return json(result);
    }
  }
  
  // Execute a query within transaction context for HTTP requests
  private async executeHttpTransactionQuery(
    sql: string, 
    params: any[] = [], 
    transaction_id: string
  ): Promise<Response> {
    // First check if transaction exists in memory
    const txContext = this.transactionContexts.get(transaction_id);
    if (!txContext || txContext.status !== 'active') {
      // Try to recover from storage if not in memory
      const recovered = await this.recoverTransactionFromStorage(transaction_id);
      if (recovered) {
        // Try again with the recovered transaction
        return this.executeHttpTransactionQuery(sql, params, transaction_id);
      }
      
      // Could not recover transaction
      return json({
        error: `Transaction not found or inactive: ${transaction_id}`,
        status: txContext?.status || 'unknown'
      }, 404);
    }

    // Continue with existing implementation
    try {
      const result = await this.executeQueryInTransaction(sql, params, transaction_id);
      return json(result);
    } catch (error: any) {
      console.error("Transaction query error:", error);
      return json({ error: error.message }, 400);
    }
  }
  
  // Handle raw query and transaction batch requests
  private async handleRawQueryRequest(request: Request): Promise<Response> {
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
        return json({ error: "Failed to initialize database." }, 500);
      }
    }

    let finalResult: RawQueryResult;

    if (data.transaction) {
      finalResult = await this.executeRawBatchQuery(data.transaction);
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
      
      // Check if we should broadcast CDC events
      const tablesInfo = this.analyzeQueryTables(data.sql);
      if (tablesInfo.isWrite) {
        this.broadcastChangeDataEvent(data.sql, cursor.rowsWritten, tablesInfo.tables);
      }
    } else {
      return json({ 
        error: "Invalid request to /query/raw: missing 'sql' or 'transaction'" 
      }, 400);
    }

    return json(finalResult);
  }
  
  // Execute a batch of queries in a transaction
  private async executeRawBatchQuery(
    queries: { sql: string; params?: any[] }[]
  ): Promise<RawQueryResult> {
    // Handle transaction batch using storage.transaction()
    const results: RawQueryResult[] = [];
    
    await this.state.storage.transaction(async () => {
      for (const query of queries) {
        const cursor = this.sql.exec(query.sql, ...(query.params || []));
        
        const result = {
          columns: cursor.columnNames,
          rows: Array.from(cursor.raw()), // Convert Iterator to Array
          meta: {
            rows_read: cursor.rowsRead,
            rows_written: cursor.rowsWritten,
          },
        };
        
        results.push(result);
        
        // Check if we should broadcast CDC events for each statement in the batch
        const tablesInfo = this.analyzeQueryTables(query.sql);
        if (tablesInfo.isWrite) {
          this.broadcastChangeDataEvent(query.sql, cursor.rowsWritten, tablesInfo.tables);
        }
      }
    });
    
    // Return the result of the last statement in the batch
    const finalResult = results.length > 0 
      ? results[results.length - 1] 
      : { columns: [], rows: [], meta: { rows_read: 0, rows_written: 0 } };
      
    // Kysely expects empty rows from buffered transaction executeQuery
    finalResult.rows = []; 
    
    return finalResult;
  }
  
  // Handle transaction management requests
  private async handleTransactionRequest(request: Request): Promise<Response> {
    const { operation, transaction_id } = await request.json() as TransactionRequest;

    // Instead of failing, ensure database is initialized on demand
    if (!this.initialized) {
      console.log("Transaction requested but database not initialized, initializing on demand...");
      const success = await this.initializeDatabase();
      if (!success) {
        return json({ error: "Failed to initialize database on demand." }, 500);
      }
    }

    if (operation === "begin") {
      return this.handleHttpBeginTransaction();
    }

    if (operation === "commit" || operation === "rollback") {
      if (!transaction_id) {
        return json({ error: "Missing transaction_id" }, 400);
      }
      
      return this.handleHttpEndTransaction(operation, transaction_id);
    }
    
    return json({ error: "Invalid transaction operation" }, 400);
  }
  
  // Try to recover a transaction from storage if not found in memory
  private async recoverTransactionFromStorage(transaction_id: string): Promise<boolean> {
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
          return true;
        }
      }
      return false;
    } catch (error) {
      console.error(`Failed to recover transaction ${transaction_id} from storage:`, error);
      return false;
    }
  }
  
  // Create a new transaction and return its ID
  private async createTransaction(): Promise<{ 
    transaction_id: string; 
    status: TransactionStatus; 
  }> {
    const txId = crypto.randomUUID();
    const bookmark = await this.state.storage.getCurrentBookmark();

    const txContext = {
      id: txId,
      bookmark,
      operations: [],
      readTables: new Set<string>(),
      writeTables: new Set<string>(),
      startTime: Date.now(),
      lastActivity: Date.now(),
      status: 'active' as TransactionStatus,
      results: []
    };

    this.transactionContexts.set(txId, txContext);
    await this.saveTransactionContext(txId, txContext);
    
    return {
      transaction_id: txId,
      status: 'active'
    };
  }
  
  // Change transaction status to committed or rolledback
  private async finalizeTransaction(
    transaction_id: string,
    operation: 'commit' | 'rollback'
  ): Promise<{
    success: boolean;
    transaction_id: string;
    status: TransactionStatus;
    results: any[];
    error?: string;
  }> {
    const txContext = this.transactionContexts.get(transaction_id);
    if (!txContext || txContext.status !== 'active') {
      return {
        success: false,
        transaction_id,
        status: txContext?.status || 'unknown' as TransactionStatus,
        results: [],
        error: `Transaction not found or inactive: ${transaction_id}`
      };
    }

    try {
      // Update transaction status
      txContext.status = operation === 'commit' ? 'committed' : 'rolledback';
      txContext.lastActivity = Date.now();
      
      // Update storage
      await this.saveTransactionContext(transaction_id, txContext);
      
      // Schedule cleanup
      this.cleanupTransactionAfterDelay(transaction_id, 60000).catch(e => {
        console.error(`Error in ${operation} transaction ${transaction_id}:`, e);
      });

      // For commit operations, broadcast CDC events
      if (operation === 'commit' && this.connections.size > 1) {
        this.broadcastTransactionCommitEvent(txContext, transaction_id);
      }
      
      return {
        success: true,
        transaction_id,
        status: txContext.status,
        results: txContext.results || []
      };
    } catch (error: any) {
      console.error(`Error finalizing transaction ${transaction_id}:`, error);
      return {
        success: false,
        transaction_id,
        status: txContext.status,
        results: [],
        error: error.message || 'Unknown error finalizing transaction'
      };
    }
  }

  // Handle beginning a new transaction via HTTP
  private async handleHttpBeginTransaction(): Promise<Response> {
    const result = await this.createTransaction();
    
    return json({
      transaction_id: result.transaction_id,
      status: result.status
    } as TransactionResponse);
  }
  
  // Handle committing or rolling back a transaction via HTTP
  private async handleHttpEndTransaction(
    operation: 'commit' | 'rollback',
    transaction_id: string
  ): Promise<Response> {
    // First check if transaction exists in memory
    const txContext = this.transactionContexts.get(transaction_id);
    if (!txContext || txContext.status !== 'active') {
      // Try to recover from storage if not in memory
      const recovered = await this.recoverTransactionFromStorage(transaction_id);
      if (recovered) {
        // Try again with the recovered transaction
        return this.handleHttpEndTransaction(operation, transaction_id);
      }
      
      // Could not recover transaction
      return json({
        error: "Transaction not found or inactive",
        transaction_id,
        status: txContext?.status || 'unknown'
      } as TransactionResponse, 404);
    }

    // Finalize transaction
    const result = await this.finalizeTransaction(transaction_id, operation);
    
    if (!result.success) {
      return json({ error: result.error }, 500);
    }
    
    // Return successful response
    return json({
      transaction_id: result.transaction_id,
      status: result.status,
      results: result.results
    } as TransactionResponse);
  }
  
  // Helper to broadcast CDC events for data changes
  private broadcastChangeDataEvent(sql: string, rowsAffected: number, tables: string[]) {
    if (this.connections.size <= 1) return; // Don't broadcast if no other clients
    
    const operationType = sql.match(/INSERT|UPDATE|DELETE/i)?.[0].toLowerCase();
    if (!operationType || tables.length === 0) return;
    
    this.broadcastEvent({
      type: 'database_change',
      operation: operationType,
      tables,
      rowsAffected,
      timestamp: Date.now()
    });
  }
  
  // Helper to broadcast transaction commit events
  private broadcastTransactionCommitEvent(txContext: any, transaction_id: string) {
    try {
      const writeOperations = txContext.operations.filter(op => 
        /INSERT|UPDATE|DELETE/i.test(op.sql)
      );
      
      if (writeOperations.length > 0) {
        const affectedTables = Array.from(txContext.writeTables);
        
        this.broadcastEvent({
          type: 'transaction_committed',
          transaction_id,
          tables: affectedTables,
          operationCount: writeOperations.length,
          timestamp: Date.now()
        });
      }
    } catch (error) {
      console.error('Error broadcasting transaction event:', error);
    }
  }
}
