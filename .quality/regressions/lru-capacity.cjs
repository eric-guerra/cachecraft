const assert = require('node:assert/strict');
const {LRU} = require('../../src/cache.js');
const c = new LRU(2); c.set('a', 1); c.set('b', 2);
assert.equal(c.size, 2); assert.equal(c.get('a'), 1);
c.set('c', 3); assert.equal(c.get('b'), undefined); assert.equal(c.get('a'), 1);
console.log('LRU capacity boundary passed');
