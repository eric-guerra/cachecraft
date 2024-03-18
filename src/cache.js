'use strict';
const { EventEmitter } = require('node:events');

/**
 * Two-tier cache-aside: an in-process LRU in front of Redis.
 *
 *  - stampede protection: concurrent misses in one process share one load (single-flight), and across
 *    processes a short Redis lock lets one instance load while the others wait briefly for the value
 *  - stale-while-revalidate: entries have a fresh TTL and a longer stale TTL; stale hits return
 *    immediately and refresh in the background
 *  - jittered TTLs so keys written together don't expire together
 *  - negative caching for "not found" (bounded short TTL)
 *  - tag invalidation (e.g. every key for product 42) + pub/sub so every process drops its L1 copy
 *
 * `redis` is a small interface (see redis.js) so tests run against an in-memory implementation.
 */

const NOT_FOUND = Symbol('not-found');

class LRU {
  constructor(max) { this.max = max; this.map = new Map(); }
  get(k) {
    const v = this.map.get(k);
    if (v === undefined) return undefined;
    this.map.delete(k); this.map.set(k, v); // refresh recency
    return v;
  }
  set(k, v) {
    this.map.delete(k);
    this.map.set(k, v);
    if (this.map.size >= this.max) this.map.delete(this.map.keys().next().value);
  }
  delete(k) { this.map.delete(k); }
  clear() { this.map.clear(); }
  get size() { return this.map.size; }
}

class Cache extends EventEmitter {
  constructor({ redis, namespace = 'cc', l1Size = 5000, l1TtlMs = 5000, now = Date.now, random = Math.random, lockMs = 3000, waitMs = 50, maxWaits = 40 }) {
    super();
    Object.assign(this, { redis, namespace, l1TtlMs, now, random, lockMs, waitMs, maxWaits });
    this.l1 = new LRU(l1Size);
    this.inflight = new Map();
    this.stats = { l1Hit: 0, l2Hit: 0, staleHit: 0, miss: 0, loads: 0, negativeHit: 0, lockWaits: 0 };
    this.channel = `${namespace}:invalidate`;
    if (redis.subscribe) {
      redis.subscribe(this.channel, (msg) => {
        const { keys, origin } = JSON.parse(msg);
        if (origin === this.id) return;
        keys.forEach((k) => this.l1.delete(k));
        this.emit('remote-invalidate', keys);
      });
    }
    this.id = Math.random().toString(36).slice(2);
  }

  key(k) { return `${this.namespace}:v:${k}`; }

  jitter(ms) { return Math.round(ms * (0.9 + this.random() * 0.2)); } // ±10%

  /**
   * @param {string} key
   * @param {() => Promise<any>} loader  returns the value, or undefined/null for "not found"
   * @param {{ttlMs:number, staleMs?:number, negativeTtlMs?:number, tags?:string[]}} opts
   */
  async get(key, loader, { ttlMs, staleMs = 0, negativeTtlMs = Math.min(30_000, ttlMs), tags = [] }) {
    const t = this.now();
    const l1 = this.l1.get(key);
    if (l1 && l1.l1Until > t) {
      this.stats.l1Hit++;
      if (l1.freshUntil <= t) this._refresh(key, loader, { ttlMs, staleMs, negativeTtlMs, tags });
      return this._unwrap(l1);
    }

    const raw = await this.redis.get(this.key(key));
    if (raw) {
      const e = JSON.parse(raw);
      if (e.staleUntil > t) {
        this._l1set(key, e);
        if (e.freshUntil > t) {
          this.stats.l2Hit++;
        } else {
          this.stats.staleHit++;
          this._refresh(key, loader, { ttlMs, staleMs, negativeTtlMs, tags });
        }
        return this._unwrap(e);
      }
    }
    this.stats.miss++;
    return this._unwrap(await this._load(key, loader, { ttlMs, staleMs, negativeTtlMs, tags }));
  }

  _unwrap(e) {
    if (e.nf) { this.stats.negativeHit++; return undefined; }
    return e.v;
  }

  _l1set(key, e) {
    this.l1.set(key, { ...e, l1Until: Math.min(e.staleUntil, this.now() + this.l1TtlMs) });
  }

  /** Single-flight within the process + a Redis lock across processes. */
  _load(key, loader, opts) {
    const existing = this.inflight.get(key);
    if (existing) return existing;
    const p = (async () => {
      const lockKey = `${this.namespace}:lock:${key}`;
      const token = Math.random().toString(36).slice(2);
      const got = await this.redis.set(lockKey, token, { px: this.lockMs, nx: true });
      if (!got) {
        // Another instance is loading: wait for its value rather than hammering the database.
        for (let i = 0; i < this.maxWaits; i++) {
          this.stats.lockWaits++;
          await new Promise((r) => setTimeout(r, this.waitMs));
          const raw = await this.redis.get(this.key(key));
          if (raw) { const e = JSON.parse(raw); this._l1set(key, e); return e; }
        }
        // Lock holder probably died; load anyway (correctness over efficiency).
      }
      try {
        this.stats.loads++;
        const v = await loader();
        return await this._write(key, v, opts);
      } finally {
        if (got) await this.redis.delIfEquals(lockKey, token); // never release someone else's lock
      }
    })().finally(() => this.inflight.delete(key));
    this.inflight.set(key, p);
    return p;
  }

  async _write(key, v, { ttlMs, staleMs, negativeTtlMs, tags }) {
    const t = this.now();
    const nf = v === undefined || v === null;
    const fresh = this.jitter(nf ? negativeTtlMs : ttlMs);
    const e = nf ? { nf: true, freshUntil: t + fresh, staleUntil: t + fresh } : { v, freshUntil: t + fresh, staleUntil: t + fresh + staleMs };
    await this.redis.set(this.key(key), JSON.stringify(e), { px: e.staleUntil - t });
    for (const tag of tags) await this.redis.sadd(`${this.namespace}:tag:${tag}`, key);
    this._l1set(key, e);
    return e;
  }

  _refresh(key, loader, opts) {
    if (this.inflight.has(key)) return;
    this._load(key, loader, opts).catch((err) => this.emit('refresh-error', key, err)); // keep serving stale on failure
  }

  /** Write-through after an update (avoids a miss storm right after the write). */
  async set(key, value, opts) {
    await this._write(key, value, opts);
    await this._broadcast([key]);
  }

  async invalidate(...keys) {
    for (const k of keys) { await this.redis.del(this.key(k)); this.l1.delete(k); }
    await this._broadcast(keys);
  }

  async invalidateTag(tag) {
    const tagKey = `${this.namespace}:tag:${tag}`;
    const keys = await this.redis.smembers(tagKey);
    await this.redis.del(tagKey);
    if (keys.length) await this.invalidate(...keys);
    return keys.length;
  }

  async _broadcast(keys) {
    if (this.redis.publish) await this.redis.publish(this.channel, JSON.stringify({ keys, origin: this.id }));
  }
}

module.exports = { Cache, LRU, NOT_FOUND };
