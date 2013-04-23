'use strict';

var
  Q = require('q'),
  DEBUG = !!process.env['MANGO_DEBUG'];

/**
 * mango cache using redis.
 *
 * @param {RedisClient} [redisClient] underlying redis client.
 * @param {String} [prefix]
 * @param {RedisCache} [parent]
 * @constructor
 */
function RedisCache(redisClient, prefix, parent) {
  this.redisClient = redisClient || require('redis').createClient();
  this.prefix = prefix || 'mango_';
  this.parent = parent;
  this.getPromise = Q.nbind(this.redisClient, 'get');
  DEBUG && console.log('create RedisCache', prefix);
}

RedisCache.prototype.derive = function (prefix) {
  return new RedisCache(this.redisClient, this.prefix + prefix + '_', this);
};

RedisCache.prototype.get = function (key) {
  return this.getPromise(this.prefix + key);
};

RedisCache.prototype.set = function (key, value) {
  this.redisClient.set(this.prefix + key, value);//no callback!
};

RedisCache.prototype.del = function (key) {
  this.redisClient.del(this.prefix + key);//no callback!
};

RedisCache.prototype.close = function () {
  // TODO: delete all entries added by me?
  if (!this.parent) {
    this.redisClient.close();
  }
  this.redisClient = null;
};

module.exports.RedisCache = RedisCache;
