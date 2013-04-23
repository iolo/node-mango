'use strict';

var
  _ = require('underscore'),
  Q = require('q'),
  mongodb = require('mongodb'),
  MangoCollection = require('./mangocollection').MangoCollection,
  MangoDb = require('./mangodb').MangoDb,
  DEBUG = !!process.env['MANGO_DEBUG'];

/**
 * mongodb data source manager.
 *
 * @constructor
 */
function Mango() {
  this.mangoDbs = {};
  this.cache = null;

  process.on('exit', this.close.bind(this));
};

/**
 * close all underlying db connections.
 */
Mango.prototype.close = function () {
  if (this.cache) {
    this.cache.close();
    this.cache = null;
  }

  _.each(this.mangoDbs, function (mangoDb) {
    mangoDb.close();
  });
  this.mangoDbs = {};
};

/**
 *
 * @param {String} url MongoClient.connect url
 * @param {Object} [options] MongoClient.connect options
 * @param {Object} [mangoOptions] mango specific options
 * @param {MangoCache} [cache] mango cache
 * @returns {Promise} MangoDb instance with mongodb connection
 */
Mango.prototype.connect = function (url, options, mangoOptions, cache) {
  DEBUG && console.log('create mongodb connection to', url);
  var self = this;
  return Q.ninvoke(mongodb.MongoClient, 'connect', url, options).then(function (db) {
    var mangoDb = new MangoDb(this, db, mangoOptions, cache);

    self.mangoDbs[db.databaseName] = mangoDb;
    self.__defineGetter__(db.databaseName, function () {
      return mangoDb;
    });

    return mangoDb.collectionNamesWithoutSystemCollections().then(function (names) {
      names.forEach(function (name) {
        mangoDb.__defineGetter__(name, mangoDb.collection.bind(mangoDb, name));
      });
      return mangoDb;
    });
  });
};

/**
 * get mango db by name.
 *
 * @param {String} name db name
 * @returns {Promise} MangoDb instance
 */
Mango.prototype.db = function (name) {
  var mangoDb = this.mangoDbs[name];
  if (mangoDb) {
    return Q(mangoDb);
  }
  var config = this.config[name];
  var url = (config.mongo && config.mongo.url) || config.url || ''; //''=error prone
  var options = (config.mongo && config.mongo.options) || config.options || {};
  var mangoOptions = config.mango || config || {};
  var cache = this.cache && this.cache.derive(name);
  return this.connect(url, options, mangoOptions, cache);
};

/**
 * configure mango with full configuration.
 *
 * @param {Object|String} config mango configuration
 * @param {MangoCache} [cache] mango cache
 * @param {function} [callback] called when all connections are established
 * @returns {Mango} Mango instance(itself)
 */
Mango.prototype.configure = function (config, cache, callback) {
  DEBUG && console.log('configure mango...');

  this.config = _.extend({}, config);
  this.cache = cache;

  var promises = Object.keys(this.config).map(function (name) {
    return this.db(name);
  }, this);

  Q.all(promises).done(function () {
    DEBUG && console.log('mango ready!');
    callback && callback();
  });

  // FIXME: sync way! how to wait until connection is established?
  return this;
};

module.exports = new Mango();

module.exports.Mango = Mango;
module.exports.MangoCollection = MangoCollection;
module.exports.MangoDb = MangoDb;

// for lazy loading,
module.exports.__defineGetter__('MangoCache', function () {
  return require('./mangocache').MangoCache;
});
module.exports.__defineGetter__('MemoryCache', function () {
  return require('./memorycache').MemoryCache;
});
module.exports.__defineGetter__('RedisCache', function () {
  return require('./rediscache').RedisCache;
});

// for convenience,
module.exports.ObjectID = mongodb.ObjectID;
module.exports.newObjectID = function (str) {
  return new mongodb.ObjectID(str)
};
