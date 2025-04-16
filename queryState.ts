/// <reference types="@cloudflare/workers-types" />
//@ts-check

// Remove direct import as Transaction should be global via reference
// import type { SqlStorage, SqlStorageTransaction, SqlStorageCursor } from "@cloudflare/workers-types";

// Basic configuration for the factory
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

// Query options for individual queries
interface QueryOptions {
  isRaw?: boolean;
  isTransaction?: boolean; // Re-added for internal batching
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

    // If the DO returns results within a { result: ... } structure (like /query/raw did),
    // extract the actual result. Otherwise, return the direct JSON.
    // Adjust this based on the actual DO response format for consistency.
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
};

// Durable Object implementation 
export class ORMDO {
  private state: DurableObjectState;
  public sql: SqlStorage; 
  private initialized: boolean = false;

  private corsHeaders = getCorsHeaders();

  constructor(state: DurableObjectState) {
    this.state = state;
    this.sql = state.storage.sql;
    this.state.storage.get("initialized_at").then(val => { this.initialized = !!val; });
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
        
        // Use storage.transaction for initialization to ensure atomicity
        await this.state.storage.transaction(async () => { 
            await this.sql.exec(`
              CREATE TABLE IF NOT EXISTS schema_info (
                key TEXT PRIMARY KEY,
                value TEXT
              )
            `); 
            for (const statement of schema) {
              await this.sql.exec(statement);
            }
            const timestamp = new Date().toISOString();
            await this.sql.exec(
              "INSERT OR REPLACE INTO schema_info (key, value) VALUES ('initialized_at', ?)",
              [timestamp],
            );
            this.initialized = true;
            console.log("Schema initialized via transaction at:", timestamp);
        });

        return new Response(JSON.stringify({ initialized_at: new Date().toISOString() }), { 
          headers: { ...this.corsHeaders, "Content-Type": "application/json" },
        });
      }
      
      // Standard query endpoint
      if (path === "/query" && request.method === "POST") {
        const {
          sql,
          params = [],
        } = (await request.json()) as { sql: string; params?: any[]; };
        
        const cursor = await this.sql.exec(sql, ...params);
        const result = await cursor.toArray(); 

        return new Response(JSON.stringify(result), {
          headers: { ...this.corsHeaders, "Content-Type": "application/json" },
        });
      }

      // Raw query endpoint (Handles single raw queries AND transaction batches)
      if (path === "/query/raw" && request.method === "POST") {
        const data = (await request.json()) as {
          sql?: string;
          params?: any[];
          transaction?: { sql: string; params?: any[] }[];
        };

        let finalResult: RawQueryResult;

        if (data.transaction) {
          // Handle transaction batch using storage.transaction()
          const results: RawQueryResult[] = [];
          await this.state.storage.transaction(async () => { 
              for (const query of data.transaction!) { 
                  const cursor = await this.sql.exec(query.sql, ...(query.params || []));
                  results.push({
                      columns: cursor.columnNames,
                      rows: Array.from(await cursor.raw()), // Convert Iterator to Array
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
          const cursor = await this.sql.exec(data.sql, ...(data.params || []));
          finalResult = {
            columns: cursor.columnNames,
            rows: Array.from(await cursor.raw()), // Convert Iterator to Array
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
      
      // Not found
      return new Response(JSON.stringify({ error: "Not found" }), { status: 404, headers: this.corsHeaders });
      
    } catch (error: any) {
      console.error("Error in DO fetch:", error);
      return new Response(JSON.stringify({ error: error.message || "Internal Server Error" }), {
        status: 500,
        headers: { ...this.corsHeaders, "Content-Type": "application/json" },
      });
    }
  }
}
