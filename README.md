# CacheCraft

Portfolio reference implementation of a 2024 project, built to show a production caching layer for read-heavy commerce APIs (product detail, price, availability). It's a two-tier cache, an in-process LRU in front of Redis, with the protections a naive `get → miss → load → set` lacks.

**Stack:** Node.js, Redis (ioredis adapter), `node:test`.

## Features
- **Stampede protection**, at two levels:
  - Concurrent misses inside one process share one load (single-flight).
  - Across processes, a short `SET NX PX` lock lets one instance load while the others poll for the value. The lock is released with a compare-and-delete Lua script, so an instance never frees a lock it no longer owns. If the lock holder dies, waiters eventually load anyway.
- **Stale-while-revalidate:** entries have a fresh TTL and a longer stale window. Stale hits return immediately and refresh in the background, and a failed refresh keeps serving stale data and emits `refresh-error`.
- **Jittered TTLs** (±10%), so keys written together don't expire together.
- **Negative caching** of "not found", with its own short TTL. This protects the database from scans of nonexistent SKUs.
- **Tag invalidation** (`invalidateTag('product:42')`) plus **Redis pub/sub**: every process drops its L1 copy, so a price change is visible everywhere within one round trip, not after the L1 TTL.
- **Write-through `set()`** after updates, which avoids a miss storm right after a write.
- **Stats:** L1, L2, stale and negative hits, misses, loads, lock waits.

## Usage
```js
const Redis = require('ioredis');
const { Cache } = require('cachecraft');
const { ioredisAdapter } = require('cachecraft/src/redis');

const cache = new Cache({ redis: ioredisAdapter(new Redis(), new Redis()), namespace: 'catalog' });
const product = await cache.get(`product:${id}:detail`, () => db.product(id),
  { ttlMs: 60_000, staleMs: 10 * 60_000, tags: [`product:${id}`] });
await cache.invalidateTag(`product:${id}`); // after an edit
```

## Test and benchmark
```bash
npm test      # 11 tests: tiers, single-flight, cross-process lock, SWR, stale-on-error, negative cache, jitter, tags + pub/sub, write-through
npm run bench
# 1000 concurrent requests on an expired hot key: naive cache-aside → 1000 DB queries, CacheCraft → 1
```
