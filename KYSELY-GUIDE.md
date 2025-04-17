# Using Kysely ORM with orm-do

This guide explains how to use the [Kysely](https://kysely.dev/) type-safe SQL query builder with orm-do for Cloudflare Workers.

## Getting Started

### 1. Install Dependencies

```bash
npm install kysely
```

### 2. Initialize Kysely with orm-do

You can initialize Kysely in two ways:

#### Option A: Direct initialization with ORMDODialect

```typescript
import { Kysely } from 'kysely';
import { ORMDODialect } from './kysely';

// Define your database schema for type safety
interface Database {
  users: {
    id: string;
    name: string;
    email: string;
    created_at: string;
  };
}

// Initialize Kysely
const db = new Kysely<Database>({
  dialect: new ORMDODialect({
    doNamespace: env.MY_DATABASE_DO,
    config: { 
      schema: `
        CREATE TABLE IF NOT EXISTS users (
          id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          email TEXT UNIQUE NOT NULL,
          created_at TEXT NOT NULL
        )
      `,
      version: 'v1', // Optional version for DO naming
    }
  }),
});
```

#### Option B: Using an existing DBClient

```typescript
import { createDBClient } from './queryState';
import { createKysely } from './kysely';

// Create a DBClient first
const dbClient = createDBClient(env.MY_DATABASE_DO, {
  schema: `
    CREATE TABLE IF NOT EXISTS users (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT UNIQUE NOT NULL,
      created_at TEXT NOT NULL
    )
  `
});

// Then create a Kysely instance
const db = createKysely<Database>(dbClient);
```

## Basic Operations

### Inserting Data

```typescript
// Insert a single record
await db
  .insertInto('users')
  .values({
    id: crypto.randomUUID(),
    name: 'John Doe',
    email: 'john@example.com',
    created_at: new Date().toISOString(),
  })
  .execute();

// Insert multiple records
await db
  .insertInto('users')
  .values([
    {
      id: crypto.randomUUID(),
      name: 'Jane Smith',
      email: 'jane@example.com',
      created_at: new Date().toISOString(),
    },
    {
      id: crypto.randomUUID(),
      name: 'Bob Johnson',
      email: 'bob@example.com',
      created_at: new Date().toISOString(),
    },
  ])
  .execute();
```

### Querying Data

```typescript
// Select all records
const allUsers = await db
  .selectFrom('users')
  .selectAll()
  .execute();

// Select specific columns
const userEmails = await db
  .selectFrom('users')
  .select(['id', 'email'])
  .execute();

// Query with conditions
const specificUser = await db
  .selectFrom('users')
  .selectAll()
  .where('email', '=', 'john@example.com')
  .executeTakeFirst(); // Get only the first result or undefined
```

### Updating Data

```typescript
// Update a record
await db
  .updateTable('users')
  .set({
    name: 'John Smith',
  })
  .where('id', '=', userId)
  .execute();
```

### Deleting Data

```typescript
// Delete a record
await db
  .deleteFrom('users')
  .where('id', '=', userId)
  .execute();
```

## Advanced Usage

### Transactions

```typescript
await db.transaction().execute(async (trx) => {
  // Create a user
  const userId = crypto.randomUUID();
  await trx
    .insertInto('users')
    .values({
      id: userId,
      name: 'Transaction User',
      email: 'transaction@example.com',
      created_at: new Date().toISOString(),
    })
    .execute();
  
  // Create a related record
  await trx
    .insertInto('posts')
    .values({
      id: crypto.randomUUID(),
      user_id: userId,
      title: 'My First Post',
      content: 'Hello, world!',
      created_at: new Date().toISOString(),
    })
    .execute();
});
```

### Joins

```typescript
// Join tables
const postsWithAuthors = await db
  .selectFrom('posts')
  .innerJoin('users', 'users.id', 'posts.user_id')
  .select([
    'posts.id',
    'posts.title',
    'users.name as author',
    'posts.created_at',
  ])
  .execute();
```

### Aggregations

```typescript
// Count, group, and aggregate
const stats = await db
  .selectFrom('posts')
  .select(({ fn }) => [
    'user_id',
    fn.count('id').as('post_count'),
  ])
  .groupBy('user_id')
  .execute();
```

### Complex Filtering

```typescript
// Complex WHERE clauses
const filteredUsers = await db
  .selectFrom('users')
  .selectAll()
  .where((eb) => 
    eb.or([
      eb('name', 'like', '%John%'),
      eb('email', 'like', '%john%'),
    ])
  )
  .where('created_at', '>=', lastWeek.toISOString())
  .execute();
```

### Working with JSON

Since SQLite stores JSON as text, you'll need to handle serialization and parsing:

```typescript
// Define your schema with TEXT columns for JSON
interface Database {
  users: {
    id: string;
    name: string;
    metadata: string; // JSON stored as TEXT
  };
}

// Helper for parsing JSON
function parseJsonColumn<T>(json: string | null): T {
  if (!json) return {} as T;
  try {
    return JSON.parse(json) as T;
  } catch (e) {
    console.error('Error parsing JSON:', e);
    return {} as T;
  }
}

// Insert with JSON
await db
  .insertInto('users')
  .values({
    id: crypto.randomUUID(),
    name: 'John Doe',
    metadata: JSON.stringify({ 
      preferences: { theme: 'dark' },
      lastLogin: new Date().toISOString()
    }),
  })
  .execute();

// Query and parse JSON
const users = await db
  .selectFrom('users')
  .selectAll()
  .execute()
  .then(users => users.map(user => ({
    ...user,
    metadata: parseJsonColumn(user.metadata),
  })));
```

## Tips for SQLite in Cloudflare Workers

1. **SQLite JSON support**: While SQLite has JSON functions, stick to basic operations and use client-side parsing when possible.

2. **Dates**: Store dates as ISO strings (`new Date().toISOString()`) for consistency.

3. **Pagination**: Implement pagination to avoid large result sets.

   ```typescript
   const pageSize = 10;
   const page = 1;
   
   const pagedResults = await db
     .selectFrom('users')
     .selectAll()
     .limit(pageSize)
     .offset((page - 1) * pageSize)
     .execute();
   ```

4. **Complex Queries**: Use Kysely's query builder for complex queries instead of raw SQL when possible.

## Performance Considerations

1. **Indexes**: Create indexes for frequently queried columns:

   ```sql
   CREATE INDEX idx_users_email ON users(email);
   ```

2. **Limit Result Sets**: Always limit the number of rows returned.

3. **Use Transactions**: For multiple related operations, use transactions to ensure consistency.

## Resources

- [Kysely Documentation](https://kysely.dev/)
- [Kysely GitHub Repository](https://github.com/kysely-org/kysely)
- [orm-do Documentation](https://github.com/yourusername/orm-do) 