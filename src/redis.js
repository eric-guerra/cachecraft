'use strict';
// The Redis surface the cache needs, with an ioredis adapter and an in-memory twin for tests.

/** ioredis adapter. Pass two connections: subscribers can't run normal commands. */
function ioredisAdapter(client, subscriber) {
  return {
    get: (k) => client.get(k),
    async set(k, v, { px, nx } = {}) {
      const args = [k, v];
      if (px) args.push('PX', px);
      if (nx) args.push('NX');
      return (await client.set(...args)) === 'OK';
    },
    del: (k) => client.del(k),
    // Compare-and-delete in one round trip so we only release a lock we still own.
    delIfEquals: (k, v) => client.eval("if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end", 1, k, v),
    sadd: (k, m) => client.sadd(k, m),
    smembers: (k) => client.smembers(k),
    publish: (ch, msg) => client.publish(ch, msg),
    subscribe(ch, fn) {
      subscriber.subscribe(ch);
      subscriber.on('message', (c, m) => c === ch && fn(m));
    },
  };
}

/** In-memory Redis with PX expiry against an injectable clock, and shared pub/sub between instances. */
function memoryRedis(now = Date.now, bus = { subs: new Map() }) {
  const kv = new Map();
  const sets = new Map();
  const live = (k) => {
    const e = kv.get(k);
    if (!e) return undefined;
    if (e.exp && e.exp <= now()) { kv.delete(k); return undefined; }
    return e;
  };
  const r = {
    calls: { get: 0, set: 0 },
    async get(k) { r.calls.get++; return live(k)?.v ?? null; },
    async set(k, v, { px, nx } = {}) {
      r.calls.set++;
      if (nx && live(k)) return false;
      kv.set(k, { v, exp: px ? now() + px : 0 });
      return true;
    },
    async del(k) { kv.delete(k); sets.delete(k); },
    async delIfEquals(k, v) { if (live(k)?.v === v) { kv.delete(k); return 1; } return 0; },
    async sadd(k, m) { if (!sets.has(k)) sets.set(k, new Set()); sets.get(k).add(m); },
    async smembers(k) { return [...(sets.get(k) ?? [])]; },
    async publish(ch, msg) { for (const fn of bus.subs.get(ch) ?? []) fn(msg); },
    subscribe(ch, fn) { if (!bus.subs.has(ch)) bus.subs.set(ch, []); bus.subs.get(ch).push(fn); },
    _kv: kv,
  };
  return r;
}

/** Two "processes" sharing one Redis: same keyspace, same bus. */
function sharedMemoryRedis(now = Date.now) {
  const bus = { subs: new Map() };
  const base = memoryRedis(now, bus);
  return () => ({ ...base, subscribe: base.subscribe });
}

module.exports = { ioredisAdapter, memoryRedis, sharedMemoryRedis };
