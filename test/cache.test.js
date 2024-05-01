'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { Cache, LRU } = require('../src/cache');
const { memoryRedis, sharedMemoryRedis } = require('../src/redis');

function clock(start = 1_000_000) {
  let t = start;
  return { now: () => t, advance: (ms) => (t += ms) };
}

const opts = { ttlMs: 60_000, staleMs: 300_000, tags: ['product:42'] };

test('cache-aside: miss, L1 hit, L2 hit after L1 expiry', async () => {
  const c = clock();
  const cache = new Cache({ redis: memoryRedis(c.now), now: c.now, random: () => 0.5 });
  let loads = 0;
  const loader = async () => ({ id: 42, price: 1999, n: ++loads });
  assert.equal((await cache.get('p42', loader, opts)).n, 1);
  assert.equal((await cache.get('p42', loader, opts)).n, 1);
  c.advance(6000); // past L1 TTL (5s), still fresh in Redis
  assert.equal((await cache.get('p42', loader, opts)).n, 1);
  assert.deepEqual([cache.stats.miss, cache.stats.l1Hit, cache.stats.l2Hit, loads], [1, 1, 1, 1]);
});

test('single-flight: 100 concurrent misses load once', async () => {
  const c = clock();
  const cache = new Cache({ redis: memoryRedis(c.now), now: c.now });
  let loads = 0;
  const loader = () => new Promise((r) => setTimeout(() => r({ n: ++loads }), 20));
  const results = await Promise.all(Array.from({ length: 100 }, () => cache.get('hot', loader, opts)));
  assert.equal(loads, 1);
  assert.ok(results.every((r) => r.n === 1));
});

test('cross-process stampede: second instance waits for the lock holder instead of loading', async () => {
  const c = clock();
  const shared = sharedMemoryRedis(c.now);
  const a = new Cache({ redis: shared(), now: c.now, waitMs: 5 });
  const b = new Cache({ redis: shared(), now: c.now, waitMs: 5 });
  let loads = 0;
  const loader = () => new Promise((r) => setTimeout(() => r({ n: ++loads }), 40));
  const [ra, rb] = await Promise.all([a.get('k', loader, opts), b.get('k', loader, opts)]);
  assert.equal(loads, 1);
  assert.deepEqual([ra.n, rb.n], [1, 1]);
  assert.ok(b.stats.lockWaits > 0);
});

test('stale-while-revalidate returns instantly and refreshes in the background', async () => {
  const c = clock();
  const cache = new Cache({ redis: memoryRedis(c.now), now: c.now, random: () => 0.5, l1TtlMs: 1 });
  let version = 1;
  const loader = async () => ({ v: version });
  await cache.get('k', loader, opts);
  version = 2;
  c.advance(61_000); // fresh expired, stale window open
  const stale = await cache.get('k', loader, opts);
  assert.equal(stale.v, 1);
  assert.equal(cache.stats.staleHit, 1);
  await new Promise((r) => setImmediate(r));
  await new Promise((r) => setTimeout(r, 5));
  c.advance(10);
  assert.equal((await cache.get('k', loader, opts)).v, 2);
});

test('refresh failures keep serving stale data', async () => {
  const c = clock();
  const cache = new Cache({ redis: memoryRedis(c.now), now: c.now, random: () => 0.5, l1TtlMs: 1 });
  await cache.get('k', async () => ({ v: 1 }), opts);
  c.advance(61_000);
  const errors = [];
  cache.on('refresh-error', (k, e) => errors.push(e.message));
  const v = await cache.get('k', async () => { throw new Error('db down'); }, opts);
  await new Promise((r) => setTimeout(r, 5));
  assert.equal(v.v, 1);
  assert.deepEqual(errors, ['db down']);
});

test('negative caching stops repeated lookups for missing keys, briefly', async () => {
  const c = clock();
  const cache = new Cache({ redis: memoryRedis(c.now), now: c.now, random: () => 0.5 });
  let loads = 0;
  const loader = async () => { loads++; return null; };
  assert.equal(await cache.get('sku-x', loader, { ttlMs: 60_000, negativeTtlMs: 10_000 }), undefined);
  assert.equal(await cache.get('sku-x', loader, { ttlMs: 60_000, negativeTtlMs: 10_000 }), undefined);
  assert.equal(loads, 1);
  c.advance(11_000);
  await cache.get('sku-x', loader, { ttlMs: 60_000, negativeTtlMs: 10_000 });
  assert.equal(loads, 2);
});

test('TTL jitter spreads expiries ±10%', async () => {
  const c = clock();
  const r = memoryRedis(c.now);
  const lo = new Cache({ redis: r, now: c.now, random: () => 0 });
  const hi = new Cache({ redis: r, now: c.now, random: () => 0.999 });
  assert.equal(lo.jitter(60_000), 54_000);
  assert.equal(hi.jitter(60_000), 65_988);
});

test('tag invalidation clears every key for a product, in every process', async () => {
  const c = clock();
  const shared = sharedMemoryRedis(c.now);
  const a = new Cache({ redis: shared(), now: c.now });
  const b = new Cache({ redis: shared(), now: c.now });
  let n = 0;
  const loader = async () => ({ n: ++n });
  await a.get('product:42:detail', loader, opts);
  await a.get('product:42:price', loader, opts);
  await b.get('product:42:detail', loader, opts); // b now has it in L1
  assert.equal(b.l1.size, 1);
  assert.equal(await a.invalidateTag('product:42'), 2);
  assert.equal(b.l1.size, 0, 'pub/sub dropped b\'s L1 copy');
  assert.equal((await b.get('product:42:detail', loader, opts)).n, 3);
});

test('write-through set updates all processes', async () => {
  const c = clock();
  const shared = sharedMemoryRedis(c.now);
  const a = new Cache({ redis: shared(), now: c.now });
  const b = new Cache({ redis: shared(), now: c.now });
  await b.get('k', async () => ({ price: 1 }), opts);
  await a.set('k', { price: 2 }, opts);
  assert.equal((await b.get('k', async () => ({ price: 999 }), opts)).price, 2);
});

test('lock is only released by its owner', async () => {
  const r = memoryRedis();
  await r.set('lock', 'mine', { px: 1000, nx: true });
  assert.equal(await r.delIfEquals('lock', 'theirs'), 0);
  assert.equal(await r.delIfEquals('lock', 'mine'), 1);
});

test('LRU evicts least recently used', () => {
  const l = new LRU(2);
  l.set('a', 1); l.set('b', 2); l.get('a'); l.set('c', 3);
  assert.equal(l.get('b'), undefined);
  assert.equal(l.get('a'), 1);
});
