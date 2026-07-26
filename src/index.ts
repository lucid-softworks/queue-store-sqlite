import { randomUUID } from "node:crypto";
import { type DatabaseSync, type SQLOutputValue } from "node:sqlite";

import {
  QueueJobNotFoundError,
  type QueueClaimOptions,
  type QueueJob,
  type QueueStore,
} from "@lucid-softworks/queue-core";
import {
  queueJobFromRecord,
  queueJobToRecord,
  type QueueJobRecord,
} from "@lucid-softworks/queue-store-codec";

type SqliteRow = Record<string, SQLOutputValue>;

function validateTableName(tableName: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(tableName))
    throw new TypeError("SQLite queue table name is invalid");
  return tableName;
}

function nullableString(value: SQLOutputValue | undefined): string | null {
  return value === null || value === undefined ? null : String(value);
}

function nullableNumber(value: SQLOutputValue | undefined): number | null {
  return value === null || value === undefined ? null : Number(value);
}

function rowToRecord(row: SqliteRow): QueueJobRecord {
  return {
    attempt: Number(row.attempt),
    availableAt: Number(row.available_at),
    createdAt: Number(row.created_at),
    dataJson: String(row.data_json),
    deduplicationKey: nullableString(row.deduplication_key),
    errorJson: nullableString(row.error_json),
    id: String(row.id),
    leaseExpiresAt: nullableNumber(row.lease_expires_at),
    leaseToken: nullableString(row.lease_token),
    leaseWorkerId: nullableString(row.lease_worker_id),
    maxAttempts: Number(row.max_attempts),
    name: String(row.name),
    priority: Number(row.priority),
    resultJson: nullableString(row.result_json),
    state: String(row.state) as QueueJobRecord["state"],
    updatedAt: Number(row.updated_at),
  };
}

function values(record: QueueJobRecord): readonly (number | string | null)[] {
  return [
    record.id,
    record.name,
    record.state,
    record.priority,
    record.attempt,
    record.maxAttempts,
    record.availableAt,
    record.createdAt,
    record.updatedAt,
    record.dataJson,
    record.deduplicationKey,
    record.leaseToken,
    record.leaseWorkerId,
    record.leaseExpiresAt,
    record.resultJson,
    record.errorJson,
  ];
}

const columns =
  "id, name, state, priority, attempt, max_attempts, available_at, created_at, updated_at, data_json, deduplication_key, lease_token, lease_worker_id, lease_expires_at, result_json, error_json";

/** Durable synchronous QueueStore backed by Node's built-in SQLite driver. */
export class SqliteQueueStore implements QueueStore {
  readonly #table: string;

  constructor(
    readonly database: DatabaseSync,
    options: Readonly<{ tableName?: string; migrate?: boolean }> = {},
  ) {
    this.#table = validateTableName(options.tableName ?? "queue_jobs");
    if (options.migrate ?? true) this.migrate();
  }

  migrate(): void {
    this.database.exec(`
      CREATE TABLE IF NOT EXISTS "${this.#table}" (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        state TEXT NOT NULL,
        priority REAL NOT NULL,
        attempt INTEGER NOT NULL,
        max_attempts INTEGER NOT NULL,
        available_at REAL NOT NULL,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        data_json TEXT NOT NULL,
        deduplication_key TEXT,
        lease_token TEXT,
        lease_worker_id TEXT,
        lease_expires_at REAL,
        result_json TEXT,
        error_json TEXT
      );
      CREATE INDEX IF NOT EXISTS "${this.#table}_ready"
        ON "${this.#table}" (state, available_at, priority DESC, created_at, id);
      CREATE UNIQUE INDEX IF NOT EXISTS "${this.#table}_dedupe"
        ON "${this.#table}" (deduplication_key)
        WHERE deduplication_key IS NOT NULL
          AND state IN ('waiting', 'scheduled', 'active');
    `);
  }

  add(job: QueueJob): boolean {
    const placeholders = Array.from({ length: 16 }, () => "?").join(", ");
    const result = this.database
      .prepare(
        `INSERT OR IGNORE INTO "${this.#table}" (${columns}) VALUES (${placeholders})`,
      )
      .run(...values(queueJobToRecord(job)));
    return result.changes === 1;
  }

  get(id: string): QueueJob | undefined {
    const row = this.database
      .prepare(`SELECT ${columns} FROM "${this.#table}" WHERE id = ?`)
      .get(id);
    return row === undefined ? undefined : queueJobFromRecord(rowToRecord(row));
  }

  list(): readonly QueueJob[] {
    return this.database
      .prepare(
        `SELECT ${columns} FROM "${this.#table}" ORDER BY created_at, id`,
      )
      .all()
      .map((row) => queueJobFromRecord(rowToRecord(row)));
  }

  claim(options: QueueClaimOptions): QueueJob | undefined {
    if (options.workerId.length === 0)
      throw new TypeError("workerId cannot be empty");
    if (!Number.isFinite(options.leaseDuration) || options.leaseDuration <= 0)
      throw new RangeError("leaseDuration must be positive and finite");
    this.database.exec("BEGIN IMMEDIATE");
    try {
      this.database
        .prepare(
          `UPDATE "${this.#table}"
           SET state = 'waiting', available_at = ?, updated_at = ?,
               lease_token = NULL, lease_worker_id = NULL, lease_expires_at = NULL
           WHERE state = 'active' AND lease_expires_at <= ?`,
        )
        .run(options.now, options.now, options.now);
      const names = options.names;
      const nameClause =
        names === undefined
          ? ""
          : names.length === 0
            ? " AND 0"
            : ` AND name IN (${names.map(() => "?").join(", ")})`;
      const candidate = this.database
        .prepare(
          `SELECT id FROM "${this.#table}"
           WHERE state IN ('waiting', 'scheduled') AND available_at <= ?${nameClause}
           ORDER BY priority DESC, created_at, id LIMIT 1`,
        )
        .get(options.now, ...(names ?? []));
      if (candidate === undefined) {
        this.database.exec("COMMIT");
        return undefined;
      }
      const id = String(candidate.id);
      const token = randomUUID();
      this.database
        .prepare(
          `UPDATE "${this.#table}"
           SET state = 'active', attempt = attempt + 1, updated_at = ?,
               lease_token = ?, lease_worker_id = ?, lease_expires_at = ?
           WHERE id = ?`,
        )
        .run(
          options.now,
          token,
          options.workerId,
          options.now + options.leaseDuration,
          id,
        );
      const claimed = this.get(id) as QueueJob;
      this.database.exec("COMMIT");
      return claimed;
    } catch (error) {
      this.database.exec("ROLLBACK");
      throw error;
    }
  }

  save(job: QueueJob): void {
    const record = queueJobToRecord(job);
    const result = this.database
      .prepare(
        `UPDATE "${this.#table}" SET
          name = ?, state = ?, priority = ?, attempt = ?, max_attempts = ?,
          available_at = ?, created_at = ?, updated_at = ?, data_json = ?,
          deduplication_key = ?, lease_token = ?, lease_worker_id = ?,
          lease_expires_at = ?, result_json = ?, error_json = ?
         WHERE id = ?`,
      )
      .run(...values(record).slice(1), record.id);
    if (result.changes === 0) throw new QueueJobNotFoundError(job.id);
  }

  heartbeat(
    id: string,
    leaseToken: string,
    now: number,
    leaseDuration: number,
  ): QueueJob | undefined {
    if (!Number.isFinite(leaseDuration) || leaseDuration <= 0)
      throw new RangeError("leaseDuration must be positive and finite");
    const result = this.database
      .prepare(
        `UPDATE "${this.#table}"
         SET lease_expires_at = ?, updated_at = ?
         WHERE id = ? AND state = 'active' AND lease_token = ?`,
      )
      .run(now + leaseDuration, now, id, leaseToken);
    return result.changes === 0 ? undefined : this.get(id);
  }

  delete(id: string): boolean {
    return (
      this.database.prepare(`DELETE FROM "${this.#table}" WHERE id = ?`).run(id)
        .changes === 1
    );
  }

  findByDeduplicationKey(key: string): QueueJob | undefined {
    const row = this.database
      .prepare(
        `SELECT ${columns} FROM "${this.#table}"
         WHERE deduplication_key = ?
           AND state IN ('waiting', 'scheduled', 'active')
         ORDER BY created_at, id LIMIT 1`,
      )
      .get(key);
    return row === undefined ? undefined : queueJobFromRecord(rowToRecord(row));
  }
}
