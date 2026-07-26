import { DatabaseSync } from "node:sqlite";

import {
  QueueJobNotFoundError,
  type QueueJob,
} from "@lucid-softworks/queue-core";
import { describe, expect, it } from "vitest";

import { SqliteQueueStore } from "../src/index.js";

const job = (id: string, overrides: Partial<QueueJob> = {}): QueueJob => ({
  attempt: 0,
  availableAt: 0,
  createdAt: 0,
  data: { id },
  id,
  maxAttempts: 2,
  name: "work",
  priority: 0,
  state: "waiting",
  updatedAt: 0,
  ...overrides,
});

describe("SqliteQueueStore", () => {
  it("migrates, stores, updates, queries, deduplicates, and deletes", () => {
    const database = new DatabaseSync(":memory:");
    const store = new SqliteQueueStore(database);
    expect(store.add(job("a", { deduplicationKey: "key" }))).toBe(true);
    expect(store.add(job("a"))).toBe(false);
    expect(store.add(job("duplicate-key", { deduplicationKey: "key" }))).toBe(
      false,
    );
    expect(store.get("missing")).toBeUndefined();
    expect(store.get("a")?.data).toEqual({ id: "a" });
    expect(store.findByDeduplicationKey("key")?.id).toBe("a");
    expect(store.list()).toHaveLength(1);
    const updated = job("a", {
      deduplicationKey: "key",
      error: new Error("old"),
      result: undefined,
      state: "completed",
      updatedAt: 2,
    });
    store.save(updated);
    expect(store.get("a")).toEqual(updated);
    expect(store.findByDeduplicationKey("key")).toBeUndefined();
    expect(() => store.save(job("missing"))).toThrow(QueueJobNotFoundError);
    expect(store.delete("a")).toBe(true);
    expect(store.delete("a")).toBe(false);
    database.close();
  });

  it("claims by name, heartbeats leases, and recovers expiration", () => {
    const database = new DatabaseSync(":memory:");
    const store = new SqliteQueueStore(database);
    store.add(job("named", { name: "named", priority: 1 }));
    store.add(job("other", { name: "other", priority: 10 }));
    store.add(job("future", { availableAt: 50, state: "scheduled" }));
    expect(() =>
      store.claim({ leaseDuration: 10, now: 0, workerId: "" }),
    ).toThrow(TypeError);
    expect(() =>
      store.claim({ leaseDuration: 0, now: 0, workerId: "worker" }),
    ).toThrow(RangeError);
    expect(() =>
      store.claim({
        leaseDuration: Number.NaN,
        now: 0,
        workerId: "worker",
      }),
    ).toThrow(RangeError);
    const named = store.claim({
      leaseDuration: 10,
      names: ["named"],
      now: 0,
      workerId: "worker",
    });
    expect(named?.id).toBe("named");
    expect(named?.attempt).toBe(1);
    expect(store.findByDeduplicationKey("missing")).toBeUndefined();
    expect(() => store.heartbeat("named", "x", 1, 0)).toThrow(RangeError);
    expect(store.heartbeat("named", "wrong", 1, 10)).toBeUndefined();
    expect(
      store.heartbeat("named", named?.lease?.token as string, 1, 10)?.lease
        ?.expiresAt,
    ).toBe(11);
    expect(
      store.claim({
        leaseDuration: 10,
        names: [],
        now: 0,
        workerId: "worker",
      }),
    ).toBeUndefined();
    const other = store.claim({
      leaseDuration: 10,
      now: 0,
      workerId: "worker",
    });
    expect(other?.id).toBe("other");
    const reclaimed = store.claim({
      leaseDuration: 10,
      now: 10,
      workerId: "recovery",
    });
    expect(reclaimed?.id).toBe("other");
    expect(reclaimed?.attempt).toBe(2);
    database.close();
  });

  it("supports explicit migration and rolls back failed claims", () => {
    const database = new DatabaseSync(":memory:");
    expect(
      () => new SqliteQueueStore(database, { tableName: "bad-name" }),
    ).toThrow(TypeError);
    const store = new SqliteQueueStore(database, {
      migrate: false,
      tableName: "custom_jobs",
    });
    store.migrate();
    store.add(
      job("bad", {
        lease: { expiresAt: 0, token: "token", workerId: "old" },
        state: "active",
      }),
    );
    database.exec(`
      CREATE TRIGGER fail_recovery BEFORE UPDATE ON custom_jobs
      WHEN OLD.id = 'bad'
      BEGIN
        SELECT RAISE(ABORT, 'forced rollback');
      END;
    `);
    expect(() =>
      store.claim({ leaseDuration: 10, now: 0, workerId: "worker" }),
    ).toThrow("forced rollback");
    expect(database.isTransaction).toBe(false);
    database.close();
  });
});
