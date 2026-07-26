# `@lucid-softworks/queue-store-sqlite`

A durable synchronous `QueueStore` using Node's built-in `node:sqlite` driver.
Node 22.5 or newer is required.

```ts
import { DatabaseSync } from "node:sqlite";
import { SqliteQueueStore } from "@lucid-softworks/queue-store-sqlite";

const database = new DatabaseSync("jobs.sqlite");
database.exec("PRAGMA journal_mode = WAL");

const store = new SqliteQueueStore(database);
```

Construction creates the table and indexes by default. Pass `{ migrate: false
}` when migrations are managed separately, then invoke `store.migrate()` at the
appropriate deployment boundary.

Claims use `BEGIN IMMEDIATE`, recover expired leases, select by priority and
age, update the lease, and commit as one transaction. A partial unique index
enforces one non-terminal job per deduplication key. The supplied table name
must be a plain SQL identifier.
