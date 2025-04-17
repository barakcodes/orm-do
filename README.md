# ORM DO - Unlimited SQLite DBs Directly In Your Worker

Functionality

- 🔥 Abstracts away from the DO so you can just perform SQL queries to state from unlimited SQLite DBs, directly from your workers.
- 🔥 Compatible and linked with @outerbase to easily explore the state of the DO or DOs
- 🔥 query fn promises json/ok directly from the worker. This makes working with it a lot simpler.

> [!IMPORTANT]
> New (more opinionated) version at [janwilmake/dorm](https://github.com/janwilmake/dorm)

# Demo

See https://ormdo.wilmake.com for the `example.ts` example, which demonstrates it works using a users management API and HTML for that.

X Post: https://x.com/janwilmake/status/1912146275597721959

# Contribute

(still testing this button! lmk if it worked)

[![Deploy to Cloudflare](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/janwilmake/orm-do)

# Base Usage (queryState.ts)

This section describes the low-level client provided by `queryState.ts`.

In your `wrangler.toml`

```toml
[[durable_objects.bindings]]
name = "MY_EXAMPLE_DO"
class_name = "ORMDO"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["ORMDO"]
```

In your worker:

```ts
import { ORMDO, createDBClient, DBConfig, DBClient } from "./queryState";
import { adminHtml } from "./adminHtml";
export { ORMDO };

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
  // Optional: used for authenticating requests
  authSecret: "my-secret-key",
};

type Env = {
  MY_EXAMPLE_DO: DurableObjectNamespace;
};

export default {
  fetch: async (request: Request, env: Env, ctx: any) => {
    // Create the low-level client
    const client = createDBClient(env.MY_EXAMPLE_DO, dbConfig);

    // First try to handle the request with the middleware
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

    ///... YOUR ENDPOINTS HERE USING DB CLIENT or the Type-Safe Query Builder
  },
};
```

# Why?

I'm looking for a simpler way to create stateful workers with multiple DBs. One of the issues I have with DOs is that they're hard to work with and your code becomes verbose quite easily. Also it's not yet easy to explore multiple databases. This is an abstraction that ensures you can perform state queries directly from your worker, queue, schedule, etc, more easily.

My ultimate goal would be to be able to hook it up to github oauth and possibly [sponsorflare](https://sponsorflare.com) and have anyone explore their own data.

I'm still experimenting. Hit me up if you've got ideas!

Made by [janwilmake](https://x.com/janwilmake).

# Using the Type-Safe Query Builder (`db.ts`)

This library includes a type-safe query builder (`db.ts`) inspired by Kysely, designed to work on top of the `DBClient` from `queryState.ts`. It provides better type inference and safety for your database interactions.

## Setup

1.  **Define Your Database Interface:** Create a TypeScript interface that describes your database schema.

    ```typescript
    import { Generated, ColumnType } from './db';

    interface UserTable {
      id: Generated<string>; // Auto-generated primary key
      name: string;
      email: string | null; // Nullable email
      created_at: ColumnType<Date, string | undefined, never>; // Selects as Date, inserts as optional string, never updated
    }

    interface PostTable {
      id: Generated<number>;
      title: string;
      content: string;
      author_id: string; // Foreign key to UserTable.id
      published_at: Date | null;
    }

    // Your main database interface
    export interface Database {
      users: UserTable;
      posts: PostTable;
      // Add other tables here
    }
    ```

2.  **Create the Query Builder Instance:** Use the `createDB` factory function, passing your low-level `DBClient`.

    ```typescript
    import { createDB } from './db';
    import { createDBClient } from './queryState';
    import type { Database } from './your-database-interface-file'; // Import your DB interface

    // Assuming 'client' is your DBClient instance from createDBClient()
    const client = createDBClient(env.MY_EXAMPLE_DO, dbConfig);

    // Create the type-safe DB instance
    const db = createDB<Database>(client);
    ```

## Query Examples

Now you can use the `db` instance to build queries:

**Select:**

```typescript
// Select specific columns
const users = await db.selectFrom('users')
  .select(['id', 'name'])
  .where('name', 'like', 'J%')
  .orderBy('created_at', 'desc')
  .limit(10)
  .execute();
// users will be: { id: string; name: string; }[]

// Select all columns
const posts = await db.selectFrom('posts')
  .selectAll()
  .where('author_id', '=', userId)
  .executeTakeFirst(); // Get single result or undefined
// posts will be: Selectable<PostTable> | undefined

// Select with count
const { count } = await db.selectFrom('users')
  .count()
  .executeTakeFirstOrThrow(); // Get count or throw error
```

**Insert:**

```typescript
const newUser = {
  id: crypto.randomUUID(), // Or let the DB generate if `Generated` is used correctly with DB defaults
  name: 'Alice',
  // email is optional (nullable)
  // created_at is optional (InsertType allows undefined)
};

await db.insertInto('users')
  .values(newUser)
  .execute();

// Batch insert
await db.insertInto('posts')
  .values([
    { title: 'Post 1', content: '...', author_id: userId },
    { title: 'Post 2', content: '...', author_id: userId },
  ])
  .execute();
```

**Update:**

```typescript
const userIdToUpdate = 'some-user-id';

await db.update('users')
  .set({ name: 'Alice Updated', email: 'alice.updated@example.com' })
  .where('id', '=', userIdToUpdate)
  .execute();
```

**Delete:**

```typescript
const postIdToDelete = 123;

await db.deleteFrom('posts')
  .where('id', '=', postIdToDelete)
  .execute();
```

**Transactions:**

Use the `transaction` method. The callback receives a transaction-specific `db` instance (`trx`).

```typescript
await db.transaction(async (trx) => {
  // Operations within this callback run in a transaction

  const newUser = await trx.insertInto('users')
    .values({ name: 'Bob', id: crypto.randomUUID() })
    // .returning(['id']) // Note: returning() is not currently implemented
    .executeTakeFirst(); // Assuming execute on insert returns the inserted row or relevant info

  // Use the result from the first insert (if needed and returned)
  // const bobUserId = newUser.id; 

  await trx.insertInto('posts')
    .values({
      title: 'Bobs First Post',
      content: 'Hello world!',
      author_id: 'some-id' // Replace with actual ID if returned
    })
    .execute();

  // If any operation fails, the transaction is automatically rolled back.
  // If all succeed, it's committed.
});
```

**Raw SQL:**

```typescript
// Use raw SQL when needed, but lose type safety
const results = await db.raw<{
  total_users: number
}>('SELECT COUNT(*) as total_users FROM users WHERE name LIKE ?', ['A%']);

const total = results[0].total_users;
```

This query builder provides a more structured and type-safe way to interact with your Durable Object database compared to raw SQL strings alone.
