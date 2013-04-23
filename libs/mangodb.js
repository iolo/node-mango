'use strict';

var
  _ = require('underscore'),
  Q = require('q'),
  MangoCollection = require('./mangocollection').MangoCollection,
  DEBUG = !!process.env['MANGO_DEBUG'];

/**
 * Mango style wrapper for mongodb.Db.
 *
 * @param {Mango} mango container
 * @param {monodb.Db} db underlying mongodb.Db instance
 * @param {Object} [options]
 * @param {MangoCache} [cache]
 * @constructor
 */
function MangoDb(mango, db, options, cache) {
  DEBUG && console.log('create MangoDb for', db.databaseName);

  this.mango = mango;
  this.db = db;
  this.options = _.extend({collections: {}}, options);
  this.cache = cache;

  this.mangoCollections = {};

  Object.keys(this.options.collections).forEach(function (name) {
    this.__defineGetter__(name, this.collection.bind(this, name));
  }, this);
}

/**
 * close the underlying db connection.
 */
MangoDb.prototype.close = function () {
  _.each(this.mangoCollections, function (mangoCollection) {
    mangoCollection.close();
  });
  this.mangoCollections = {};

  if (this.cache) {
    this.cache.close();
    this.cache = null;
  }

  if (this.db) {
    this.db.close();
    this.db = null;
  }
};

/**
 * get mango collection by name.
 *
 * @param {String} name
 * @returns {MangoCollection}
 */
MangoDb.prototype.collection = function (name) {
  var mongoCollection = this.mangoCollections[name];
  if (mongoCollection) {
    DEBUG && console.log('use existing MangoCollection for', name);
    return mongoCollection;
  }
  var collection = this.db.collection(name);
  var options = this.options.collections[name];
  var cache = this.cache && this.cache.derive(name);
  return this.mangoCollections[name] = new MangoCollection(this, collection, options, cache);
};

MangoDb.prototype.collectionNamesWithoutSystemCollections = function () {
  return Q.ninvoke(this.db, 'collectionNames', {namesOnly: 1}).then(function (names) {
    // for convenience,
    // setup getters for all collections
    return names.reduce(function (result, name) {
      // excludes system collections:
      // ex. test.system.indexes
      if (name.indexOf('.system.') < 0) {
        // remove leading namespace:
        // ex. test.users -> users
        result.push(name.substring(name.indexOf('.') + 1));
      }
      return result;
    }, []);
  });
};

module.exports.MangoDb = MangoDb;
