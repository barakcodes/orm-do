import { ORMDO, DBConfig, createDBClient } from "./queryState";
import { adminHtml } from "./adminHtml";

// Import the new DB client and types
import { createDB, DB, ColumnType, Database as BaseDatabase } from './db'; // Import BaseDatabase

export { ORMDO };

// Define database schema using new types
// Extend BaseDatabase to satisfy constraint
interface Database extends BaseDatabase {
  users: UserTable;
  profiles: ProfileTable;
}

interface UserTable {
  // Remove Generated wrapper since ID is client-provided
  id: string; 
  name: string;
  email: ColumnType<string | null, string, string | null>; // Allow null select/update, require string on insert
  created_at: ColumnType<Date, string | undefined, never>; // Select as Date, insert optional string, never update
}

interface ProfileTable {
  id: string;
  userId: string;
  username: string;
  created_at: ColumnType<Date, string | undefined, never>; // Select as Date, insert optional string, never update
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
    `
    CREATE TABLE IF NOT EXISTS profiles (
      id TEXT PRIMARY KEY,
      userId TEXT NOT NULL,
      username TEXT NOT NULL,
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
    // Create DB client (used for middleware and the main DB instance)
    const client = createDBClient(env.MY_EXAMPLE_DO, dbConfig);
    
    // Create the new DB instance with types
    const db = createDB<Database>(client);
    
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
        // Pass the new db instance to handlers
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
        console.error("API Error:", error); // Log the error
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

// Handle GET requests - Update db parameter type
const handleGet = async (
  request: Request,
  db: DB<Database>, // Use the new DB type
) => {
  const url = new URL(request.url);

  // Get all users
  if (url.pathname === "/api/users") {
    try {
      const users = await db
        .selectFrom("users")
        .selectAll()
        .orderBy("created_at", "desc") // Assumes created_at can be ordered directly
        .execute();

        // Use the new leftJoin syntax (table, leftCol, rightCol)
        // Use qualified column names ("tableName.columnName") in subsequent methods
        const usersWithProfiles = await db
          .selectFrom("users")
          // Use reversed join syntax: leftJoin(JoinTable, colFromJoinTable, colFromExisting)
          .leftJoin("profiles", "userId", "id") // "userId" is from profiles, "id" is from users
          // Where clause now uses qualified names
          .where("users.id", "=", "test-id") // Example where condition using qualified name
          // OrderBy now uses qualified names
          .orderBy("users.created_at", "desc")
          // Select now uses qualified names
          .select(["users.id", "users.name", "users.email", "profiles.username"]) 
          .execute();

      return new Response(JSON.stringify(usersWithProfiles), {
        headers: corsHeaders,
      });
    } catch (error) {
       console.error("GET /api/users Error:", error);
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
        .select("*")
        .where("id", "=", userId) // Use the base type for comparison
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
       // Manually parse date if needed, assuming DB returns string/number
      if (user.created_at && typeof user.created_at !== 'object') {
         user.created_at = new Date(user.created_at);
      }


      return new Response(JSON.stringify(user), {
        headers: corsHeaders,
      });
    } catch (error) {
      console.error(`GET /api/users/${userId} Error:`, error);
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

// Handle POST requests - Update db parameter type
const handlePost = async (
  request: Request,
  db: DB<Database>,
) => {
  const url = new URL(request.url);

  // Create a new user
  if (url.pathname === "/api/users") {

    try {
      // id is now required in the body because it's not Generated
      const body = (await request.json()) as {
        id: string; // Make id required in body
        name: string;
        email: string; 
      };

      // Validate required fields including id
      if ( !body.name || !body.email) {
        return new Response(
          JSON.stringify({ error: "ID, Name and email are required" }),
          {
            status: 400,
            headers: corsHeaders,
          }
        );
      }
      const userId = crypto.randomUUID();
      // Use db.transaction for atomicity
      const user = await db.transaction(async (trx) => {
        // Insert user
        await trx
          .insertInto("users")
          .values({
            id: userId, // Pass the required id
            name: body.name,
            email: body.email,
          })
       
          .execute();
        
        // Select the inserted user (since returning is not built-in)
        const insertedUser = await trx.selectFrom("users")
          .selectAll()
          .where("id", "=", userId)
          .executeTakeFirstOrThrow(); // Throw if not found (shouldn't happen)

          
         // Update example within transaction
        const updatedUser = await trx.update("users")
           .set({
             name: `${insertedUser.name}-Updated`
           })
           .where("id", "=", userId)
           .returning("*")
           .executeTakeFirst();

        // Select again to get the final state after update
         const finalUser = await trx.selectFrom("users")
          .selectAll()
          .where("id", "=", userId)
          .executeTakeFirstOrThrow();

        // Manually parse date if needed
        if (finalUser.created_at && typeof finalUser.created_at !== 'object') {
           finalUser.created_at = new Date(finalUser.created_at);
        }

        return finalUser; // Return the final state
      });
      
      return new Response(JSON.stringify(user), {
        status: 201, // Created
        headers: corsHeaders,
      });
    
    } catch (error) {
       console.error("POST /api/users Error:", error);
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

// Handle PUT requests - Update db parameter type
const handlePut = async (
  request: Request,
  db: DB<Database>, // Use the new DB type
) => {
  const url = new URL(request.url);

  // Update a user
  if (url.pathname.startsWith("/api/users/") && url.pathname.length > 11) {
    const userId = url.pathname.substring(11);
    try {
      // Body type matches Updateable<UserTable>
      const body = (await request.json()) as { name?: string; email?: string | null };

      if (!body.name && body.email === undefined) { // Check if email is explicitly provided (even null)
        return new Response(
          JSON.stringify({
            error: "At least one field (name or email) must be provided for update",
          }),
          {
            status: 400,
            headers: corsHeaders,
          }
        );
      }

      // Check if user exists before update
      const existingUser = await db
        .selectFrom("users")
        .select(['id']) // Select minimal data
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

      // Build update object matching Updateable<UserTable>
      const updateData: { name?: string; email?: string | null } = {};
      if (body.name !== undefined) {
        updateData.name = body.name;
      }
      if (body.email !== undefined) { // Allow setting email to null
        updateData.email = body.email;
      }
      
      // Perform update
      await db
        .update("users")
        .set(updateData) // Pass the typed update object
        .where("id", "=", userId)
        .execute();

      // Fetch the updated user data
      const updatedUser = await db
        .selectFrom("users")
        .selectAll()
        .where("id", "=", userId)
        .executeTakeFirstOrThrow(); // Throw if somehow deleted

       // Manually parse date if needed
       if (updatedUser.created_at && typeof updatedUser.created_at !== 'object') {
          updatedUser.created_at = new Date(updatedUser.created_at);
       }

      return new Response(JSON.stringify(updatedUser), {
        headers: corsHeaders,
      });
    } catch (error) {
      console.error(`PUT /api/users/${userId} Error:`, error);
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

// Handle DELETE requests - Update db parameter type
const handleDelete = async (
  request: Request,
  db: DB<Database>, // Use the new DB type
) => {
  const url = new URL(request.url);

  // Delete a user
  if (url.pathname.startsWith("/api/users/") && url.pathname.length > 11) {
    const userId = url.pathname.substring(11);

    try {
      // Fetch user data before deleting (to return it)
      const userToDelete = await db
        .selectFrom("users")
        .selectAll()
        .where("id", "=", userId)
        .executeTakeFirst();

      if (!userToDelete) {
        return new Response(
          JSON.stringify({ error: "User not found" }),
          {
            status: 404,
            headers: corsHeaders,
          }
        );
      }

      // Perform delete operation
      await db
        .deleteFrom("users")
        .where("id", "=", userId)
        .execute();

      // Manually parse date if needed
      if (userToDelete.created_at && typeof userToDelete.created_at !== 'object') {
         userToDelete.created_at = new Date(userToDelete.created_at);
      }

      return new Response(
        JSON.stringify({
          message: "User deleted successfully",
          user: userToDelete, // Return the data fetched before delete
        }),
        {
          headers: corsHeaders,
        }
      );
    } catch (error) {
      console.error(`DELETE /api/users/${userId} Error:`, error);
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

// Example of other operations using the new client
// Rename function to reflect the new client
export async function dbClientExamples(env: Env) {

   // Create DB client and typed DB instance
    const client = createDBClient(env.MY_EXAMPLE_DO, dbConfig);
    const db = createDB<Database>(client);
  
  // 1. Transaction example - Create multiple users
  await db.transaction(async (trx) => {
     const user1Id = crypto.randomUUID();
     const user2Id = crypto.randomUUID();
    // Insert first user
    await trx
      .insertInto('users')
      .values({
        id: user1Id, // Provide required ID
        name: 'Alice Brown Transaction',
        email: 'alice.tx@example.com',
        // created_at uses default
      })
      .execute();
    
    // Insert second user
    await trx
      .insertInto('users')
      .values({
         id: user2Id, // Provide required ID
        name: 'Bob Green Transaction',
        email: 'bob.tx@example.com',
         // created_at uses default
      })
      .execute();
      
     console.log(`Inserted users in transaction: ${user1Id}, ${user2Id}`);
  });
  
  // 2. Count users
  const userCountResult = await db
    .selectFrom('users')
    .count() // Use the simpler count method
    .executeTakeFirstOrThrow();
    
   console.log("User count:", userCountResult.count);
  
  // 3. Find users with pagination
  const page = 1;
  const pageSize = 5; // Smaller size for example
  
  const paginatedUsers = await db
    .selectFrom('users')
    .selectAll()
    .orderBy('created_at', 'desc')
    .limit(pageSize)
    .offset((page - 1) * pageSize)
    .execute();
    
   console.log("Paginated users (page 1):", paginatedUsers);
  
  // 4. Search users by name or email (using raw for OR condition)
  const searchTerm = 'alice';
  
  // Raw query is better for complex OR conditions across columns with LIKE
  const searchResults = await db.raw<UserTable>(
     `SELECT * FROM users WHERE name LIKE ? OR email LIKE ?`,
     [`%${searchTerm}%`, `%${searchTerm}%`]
  );
  
   console.log(`Search results for "${searchTerm}":`, searchResults);
  
  // Return results
  return {
    userCount: userCountResult.count,
    paginatedUsers,
    searchResults,
  };
} 