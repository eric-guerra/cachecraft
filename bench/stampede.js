'use strict';
// Simulates a cache-key expiry under load: 4 app instances × 250 concurrent requests for one hot key.
// Compares database queries with naive cache-aside vs CacheCraft (single-flight + cross-process lock).
const { Cache } = require('../src/cache');
const { sharedMemoryRedis } = require('../src/redis');

async function naive(instances, perInstance) {
  const store = new Map();
  let dbCalls = 0;
  const load = () => new Promise((r) => setTimeout(() => { dbCalls++; r({ price: 1999 }); }, 30));
  const get = async () => store.get('hot') ?? (async () => { const v = await load(); store.set('hot', v); return v; })();
  await Promise.all(Array.from({ length: instances * perInstance }, get));
  return dbCalls;
}

async function crafted(instances, perInstance) {
  const shared = sharedMemoryRedis();
  const caches = Array.from({ length: instances }, () => new Cache({ redis: shared(), waitMs: 5 }));
  let dbCalls = 0;
  const load = () => new Promise((r) => setTimeout(() => { dbCalls++; r({ price: 1999 }); }, 30));
  await Promise.all(caches.flatMap((c) => Array.from({ length: perInstance }, () => c.get('hot', load, { ttlMs: 60_000 }))));
  return dbCalls;
}

(async () => {
  const n = await naive(4, 250);
  const c = await crafted(4, 250);
  console.log(`1000 concurrent requests on an expired hot key: naive cache-aside → ${n} DB queries, CacheCraft → ${c}`);
})();
