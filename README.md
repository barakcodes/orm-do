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

# Usage

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

    ///... YOUR ENDPOINTS HERE USING DB CLIENT
  },
};
```

# Why?

I'm looking for a simpler way to create stateful workers with multiple DBs. One of the issues I have with DOs is that they're hard to work with and your code becomes verbose quite easily. Also it's not yet easy to explore multiple databases. This is an abstraction that ensures you can perform state queries directly from your worker, queue, schedule, etc, more easily.

My ultimate goal would be to be able to hook it up to github oauth and possibly [sponsorflare](https://sponsorflare.com) and have anyone explore their own data.

I'm still experimenting. Hit me up if you've got ideas!

Made by [janwilmake](https://x.com/janwilmake).

# ORM-DO with Kysely Integration

This library provides a Cloudflare Durable Objects-based SQLite database with a Kysely ORM integration.

## Features

- Easy-to-use Durable Object-based SQLite database
- Kysely ORM integration for type-safe queries
- Supports transactions
- Simple middleware for HTTP API access

## Installation

```bash
npm install kysely
```

## Basic Usage

### Create a DB Client

```typescript
import { createDBClient } from './queryState';

// Define schema
const schema = `
  CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
  );
`;

// Create DB client
const dbClient = createDBClient(env.DB_NAMESPACE, { schema });
```

### Use Raw Queries

```typescript
// Standard query (returns objects)
const users = await dbClient.standardQuery('SELECT * FROM users');

// Raw query (returns columns and rows)
const results = await dbClient.rawQuery('SELECT * FROM users');

// Transaction
await dbClient.transactionQuery(`
  INSERT INTO users (id, name, email) VALUES ('1', 'John', 'john@example.com');
  INSERT INTO users (id, name, email) VALUES ('2', 'Jane', 'jane@example.com');
`);
```

### Use with Kysely ORM

You can use Kysely with ORM-DO in two ways:

#### Option 1: Using an existing DBClient

```typescript
import { createDBClient } from './queryState';
import { createKysely } from './kysely';

// Define your database schema for TypeScript
interface Database {
  users: {
    id: string;
    name: string;
    email: string;
    created_at: string;
  }
}

// First create a DB client
const dbClient = createDBClient(env.DB_NAMESPACE, { schema });

// Then create a Kysely instance from the client
const db = createKysely<Database>(dbClient);
```

#### Option 2: Direct Kysely instantiation

```typescript
import { ORMDODialect } from './kysely';
import { Kysely } from 'kysely';

// Define your database schema for TypeScript
interface Database {
  users: {
    id: string;
    name: string;
    email: string;
    created_at: string;
  }
}

// Create a Kysely instance directly
const db = new Kysely<Database>({
  dialect: new ORMDODialect({
    doNamespace: env.DB_NAMESPACE,
    config: { 
      schema: `CREATE TABLE IF NOT EXISTS users (...);`,
      version: 'v1', // optional
    }
  }),
});
```

#### Using the Kysely instance

```typescript
// Insert a user
await db
  .insertInto('users')
  .values({
    id: '1',
    name: 'John Doe',
    email: 'john@example.com',
    created_at: new Date().toISOString(),
  })
  .execute();

// Query data
const user = await db
  .selectFrom('users')
  .select(['id', 'name', 'email'])
  .where('id', '=', '1')
  .executeTakeFirst();
```

## Transactions with Kysely

```typescript
await db.transaction().execute(async (trx) => {
  await trx
    .insertInto('users')
    .values({
      id: '1',
      name: 'John Doe',
      email: 'john@example.com',
      created_at: new Date().toISOString(),
    })
    .execute();
    
  await trx
    .insertInto('users')
    .values({
      id: '2', 
      name: 'Jane Doe',
      email: 'jane@example.com',
      created_at: new Date().toISOString(),
    })
    .execute();
});
```

## HTTP API Middleware

```typescript
// In your worker
export default {
  async fetch(request, env) {
    // Try the DB middleware
    const response = await dbClient.middleware(request, {
      prefix: '/api/db',
      secret: 'your-auth-secret', // Optional authentication
    });
    
    if (response) return response;
    
    // Handle other routes...
    return new Response('Not Found', { status: 404 });
  }
}
```

## HTTP API Endpoints

- `POST /api/db/init` - Initialize the database schema
- `POST /api/db/query` - Execute a standard SQL query
- `POST /api/db/query/raw` - Execute a raw SQL query

## License

MIT
