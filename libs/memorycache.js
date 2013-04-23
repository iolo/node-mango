'use strict';


var
  Q = require('q'),
  storage = {},
  stats = {
    get: 0,
    hit: 0,
    miss: 0,
    set: 0,
    update: 0,
    insert: 0,
    del: 0
  },
  DEBUG = !!process.env['MANGO_DEBUG'];

/**
 * mango cache using heap memory as a POC / testing.
 *
 * NOTE: do **NOT** use this in production environment.
 *
 * @param {String} [prefix]
 * @param {MemoryCache} [parent]
 * @constructor
 */
function MemoryCache(prefix, parent) {
  this.prefix = prefix || 'mango_';
  this.parent = parent;
  DEBUG && console.log('create MemoryCache', prefix);
}

MemoryCache.prototype.derive = function (prefix) {
  return new MemoryCache(this.prefix + prefix + '_', this);
};

MemoryCache.prototype.get = function (key) {
  var value = storage[this.prefix + key];
  stats.get += 1;
  if (value) {
    stats.hit += 1;
  } else {
    stats.miss += 1;
  }
  return Q(value);
};

MemoryCache.prototype.set = function (key, value) {
  stats.set += 1;
  if (storage[this.prefix + key]) {
    stats.update += 1;
  } else {
    stats.insert += 1;
  }
  storage[this.prefix + key] = value;
};

MemoryCache.prototype.del = function (key) {
  stats.del += 1;
  delete storage[this.prefix + key];
};

MemoryCache.prototype.close = function () {
  if (!this.parent) {
    DEBUG && console.log('MemoryCache stats:', stats);
    storage = {};
    stats = {
      get: 0,
      hit: 0,
      miss: 0,
      set: 0,
      update: 0,
      insert: 0,
      del: 0
    };
  }
};

module.exports.MemoryCache = MemoryCache;
