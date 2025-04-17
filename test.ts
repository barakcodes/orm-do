import { ORMDO, DBConfig, createDBClient } from "./queryState";
import { adminHtml } from "./adminHtml";
import { ORMDODialect } from './kysely';
import { Kysely } from 'kysely';

export { ORMDO };

// Define database schema for TypeScript
interface Database {
  users: {
    id: string;
    name: string;
    email: string | null;
    created_at: string;
  };
}

const dbConfig: DBConfig = {
  /** Put your CREATE TABLE queries here */
  schema: [
    `
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    `,
  ],
  /** Updating this if you have breaking schema changes. */
  version: "v1",
  authSecret: "my-secret-key", // Optional: used for authenticating requests
};

type Env = {
  MY_EXAMPLE_DO: DurableObjectNamespace;
};

// CORS headers for responses
const corsHeaders = {
  "Content-Type": "application/json",
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

export default {
  fetch: async (request: Request, env: Env, ctx: ExecutionContext) => {
    // First create a DB client for middleware access
    const client = createDBClient(env.MY_EXAMPLE_DO, dbConfig);
    
    // Create Kysely instance
    const db = new Kysely<Database>({
      dialect: new ORMDODialect({
        doNamespace: env.MY_EXAMPLE_DO,
        config: dbConfig,
      }),
    });
    
    // Handle CORS preflight requests
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders,
      });
    }

    // First try to handle the request with the DB middleware
    const middlewareResponse = await client.middleware(request, {
      prefix: "/api/db",
      secret: dbConfig.authSecret,
    });

    // If middleware handled the request, return its response
    if (middlewareResponse) {
      return middlewareResponse;
    }

    // Get URL and method for routing
    const url = new URL(request.url);
    const method = request.method;

    // Admin UI route
    if (url.pathname === "/" || url.pathname === "/admin") {
      return new Response(adminHtml, {
        headers: { "Content-Type": "text/html" },
      });
    }

    // Handle API routes based on method
    if (url.pathname.startsWith("/api/")) {
      try {
        if (method === "GET") {
          return await handleGet(request, db);
        } else if (method === "POST") {
          return await handlePost(request, db);
        } else if (method === "PUT") {
          return await handlePut(request, db);
        } else if (method === "DELETE") {
          return await handleDelete(request, db);
        }
      } catch (error) {
        return new Response(
          JSON.stringify({ error: (error as Error).message || "An error occurred" }),
          {
            status: 500,
            headers: corsHeaders,
          }
        );
      }
    }

    // Default 404 response
    return new Response("Not found", { status: 404 });
  },
};

// Handle GET requests
export const handleGet = async (
  request: Request,
  db: Kysely<Database>,
) => {
  const url = new URL(request.url);

  // Get all users
  if (url.pathname === "/api/users") {
    try {
      const users = await db
        .selectFrom("users")
        .selectAll()
        .orderBy("created_at", "desc")
        .execute();

      return new Response(JSON.stringify(users), {
        headers: corsHeaders,
      });
    } catch (error) {
      return new Response(
        JSON.stringify({ error: "Failed to fetch users" }),
        {
          status: 500,
          headers: corsHeaders,
        }
      );
    }
  }

  // Get user by ID
  if (url.pathname.startsWith("/api/users/") && url.pathname.length > 11) {
    const userId = url.pathname.substring(11);

    try {
      const user = await db
        .selectFrom("users")
        .selectAll()
        .where("id", "=", userId)
        .executeTakeFirst();

      if (!user) {
        return new Response(
          JSON.stringify({ error: "User not found" }),
          {
            status: 404,
            headers: corsHeaders,
          }
        );
      }

      return new Response(JSON.stringify(user), {
        headers: corsHeaders,
      });
    } catch (error) {
      return new Response(
        JSON.stringify({ error: "Failed to fetch user" }),
        {
          status: 500,
          headers: corsHeaders,
        }
      );
    }
  }

  // Default 404 response
  return new Response("Not found", { status: 404 });
};

// Handle POST requests
export const handlePost = async (
  request: Request,
  db: Kysely<Database>,
) => {
  const url = new URL(request.url);

  // Create a new user
  if (url.pathname === "/api/users") {
    try {
      const body = (await request.json()) as {
        id?: string;
        name: string;
        email: string;
      };

      if (!body.name || !body.email) {
        return new Response(
          JSON.stringify({ error: "Name and email are required" }),
          {
            status: 400,
            headers: corsHeaders,
          }
        );
      }

      // Generate random ID if not provided
      const userId = body.id || crypto.randomUUID();
      const createdAt = new Date().toISOString();

     const user = await db.transaction().execute(async (trx) => {
      // Insert the user
      const user = await trx
        .insertInto("users")
        .values({
          id: userId,
          name: body.name,
          email: body.email,
          created_at: createdAt,
        })
        .returning(["id", "name", "email", "created_at"])
        .executeTakeFirst();
        console.log("__user", user)
        const inserted = await trx.selectFrom("users").selectAll().where("id", "=", userId).executeTakeFirst();
        console.log("__inserted", inserted)

        await trx.updateTable("users")
        .set({
          name: `${user?.name}-123`
        })
        .where("id", "=", user?.id|| userId)
        .execute();

        return user
      });

    // const user = await db
    // .insertInto("users")
    // .values({
    //   id: userId,
    //   name: body.name,
    //   email: body.email,
    //   created_at: createdAt,
    // })
    // .returning(["id", "name", "email", "created_at"])
    // .executeTakeFirst();
    console.log("__user", user)
      return new Response(JSON.stringify(user), {
        status: 201,
        headers: corsHeaders,
      });
    
    } catch (error) {
      // Check for unique constraint violation (email already exists)
      const errorMsg = (error as Error).message || "";
      if (errorMsg.includes("UNIQUE constraint failed: users.email")) {
        return new Response(
          JSON.stringify({ error: "Email already exists" }),
          {
            status: 409, // Conflict
            headers: corsHeaders,
          }
        );
      }

      return new Response(
        JSON.stringify({ error: "Failed to create user" }),
        {
          status: 500,
          headers: corsHeaders,
        }
      );
    }
  }

  // Default 404 response
  return new Response("Not found", { status: 404 });
};

// Handle PUT requests
export const handlePut = async (
  request: Request,
  db: Kysely<Database>,
) => {
  const url = new URL(request.url);

  // Update a user
  if (url.pathname.startsWith("/api/users/") && url.pathname.length > 11) {
    try {
      const userId = url.pathname.substring(11);
      const body = (await request.json()) as { name?: string; email?: string };

      if (!body.name && !body.email) {
        return new Response(
          JSON.stringify({
            error: "At least one field (name or email) is required",
          }),
          {
            status: 400,
            headers: corsHeaders,
          }
        );
      }

      // Check if user exists
      const existingUser = await db
        .selectFrom("users")
        .selectAll()
        .where("id", "=", userId)
        .executeTakeFirst();

      if (!existingUser) {
        return new Response(
          JSON.stringify({ error: "User not found" }),
          {
            status: 404,
            headers: corsHeaders,
          }
        );
      }

      // Build update query based on provided fields
      const updateQuery = db
        .updateTable("users")
        .where("id", "=", userId);

      // Add fields to update
      if (body.name) {
        updateQuery.set("name", body.name);
      }
      if (body.email) {
        updateQuery.set("email", body.email);
      }

      // Execute update and return the updated user
      const updatedUser = await updateQuery
        .returning(["id", "name", "email", "created_at"])
        .executeTakeFirstOrThrow();

      return new Response(JSON.stringify(updatedUser), {
        headers: corsHeaders,
      });
    } catch (error) {
      // Check for unique constraint violation (email already exists)
      const errorMsg = (error as Error).message || "";
      if (errorMsg.includes("UNIQUE constraint failed: users.email")) {
        return new Response(
          JSON.stringify({ error: "Email already exists" }),
          {
            status: 409, // Conflict
            headers: corsHeaders,
          }
        );
      }

      return new Response(
        JSON.stringify({ error: "Failed to update user" }),
        {
          status: 500,
          headers: corsHeaders,
        }
      );
    }
  }

  // Default 404 response
  return new Response("Not found", { status: 404 });
};

// Handle DELETE requests
export const handleDelete = async (
  request: Request,
  db: Kysely<Database>,
) => {
  const url = new URL(request.url);

  // Delete a user
  if (url.pathname.startsWith("/api/users/") && url.pathname.length > 11) {
    const userId = url.pathname.substring(11);

    try {
      // Check if user exists
      const existingUser = await db
        .selectFrom("users")
        .selectAll()
        .where("id", "=", userId)
        .executeTakeFirst();

      if (!existingUser) {
        return new Response(
          JSON.stringify({ error: "User not found" }),
          {
            status: 404,
            headers: corsHeaders,
          }
        );
      }

      // Delete the user and return the deleted user data
      const deletedUser = await db
        .deleteFrom("users")
        .where("id", "=", userId)
        .returning(["id", "name", "email", "created_at"])
        .executeTakeFirstOrThrow();

      return new Response(
        JSON.stringify({
          message: "User deleted successfully",
          user: deletedUser,
        }),
        {
          headers: corsHeaders,
        }
      );
    } catch (error) {
      return new Response(
        JSON.stringify({ error: "Failed to delete user" }),
        {
          status: 500,
          headers: corsHeaders,
        }
      );
    }
  }

  // Default 404 response
  return new Response("Not found", { status: 404 });
};

// Example of other operations using Kysely
export async function kyselyExamples(doNamespace: DurableObjectNamespace) {
  // Initialize Kysely with the dialect
  const db = new Kysely<Database>({
    dialect: new ORMDODialect({
      doNamespace,
      config: dbConfig,
    }),
  });
  
  // 1. Transaction example - Create multiple users in a transaction
  await db.transaction().execute(async (trx) => {
    // Insert first user
    await trx
      .insertInto('users')
      .values({
        id: crypto.randomUUID(),
        name: 'Alice Brown',
        email: 'alice@example.com',
        created_at: new Date().toISOString(),
      })
      .execute();
    
    // Insert second user
    await trx
      .insertInto('users')
      .values({
        id: crypto.randomUUID(),
        name: 'Bob Green',
        email: 'bob@example.com',
        created_at: new Date().toISOString(),
      })
      .execute();
  });
  
  // 2. Count users
  const userCount = await db
    .selectFrom('users')
    .select(({ fn }) => [
      fn.count('id').as('count')
    ])
    .executeTakeFirstOrThrow();
  
  // 3. Find users with pagination
  const page = 1;
  const pageSize = 10;
  
  const paginatedUsers = await db
    .selectFrom('users')
    .selectAll()
    .orderBy('created_at', 'desc')
    .limit(pageSize)
    .offset((page - 1) * pageSize)
    .execute();
  
  // 4. Search users by name or email
  const searchTerm = 'john';
  
  const searchResults = await db
    .selectFrom('users')
    .selectAll()
    .where((eb) => 
      eb.or([
        eb('name', 'like', `%${searchTerm}%`),
        eb('email', 'like', `%${searchTerm}%`),
      ])
    )
    .execute();
  
  // Return results
  return {
    userCount: userCount.count,
    paginatedUsers,
    searchResults,
  };
} 