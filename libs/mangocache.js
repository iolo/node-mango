'use strict';

var
  Q = require('q'),
  DEBUG = !!process.env['MANGO_DEBUG'];

/**
 * **dummy** mango cache.
 *
 * @param {String} [prefix]
 * @param {MangoCache} [parent]
 * @constructor
 */
function MangoCache(prefix, parent) {
  this.prefix = prefix || 'mango_';
  this.parent = parent;
  DEBUG && console.log('create MangoCache', parent);
}

MangoCache.prototype.derive = function (prefix) {
  return new MangoCache(this.prefix + prefix + '_', this);
};

MangoCache.prototype.get = function (key) {
  DEBUG && console.log('cache get', key);
  //always miss!
  return Q.resolve(null);
};

MangoCache.prototype.set = function (key, value) {
  DEBUG && console.log('cache set', key, value);
  // do nothing
};

MangoCache.prototype.del = function (key) {
  DEBUG && console.log('cache del', key);
  // do nothing
};

MangoCache.prototype.close = function () {
  DEBUG && console.log('cache close');
  // do nothing
};

module.exports.MangoCache = MangoCache;
