'use strict';

var
  _ = require('underscore'),
  Q = require('q'),
  mongodb = require('mongodb'),
  MongoClient = mongodb.MongoClient,
  ObjectID = mongodb.ObjectID,
  MONGODB_COLLECTION_FUNCTIONS = [
    'insert', 'remove', 'rename', 'save', 'update', 'distinct', 'count',
    'findAndModify', 'findAndRemove', 'find', 'findOne',
    'createIndex', 'ensureIndex', 'indexInformation', 'dropIndex', 'dropAllIndexes', 'reIndex',
    'mapReduce', 'group', 'options', 'isCapped', 'indexExists',
    'geoNear', 'geoHaystackSearch', 'indexes', 'aggregate', 'stats'
  ],
  mango = {},// singleton instance of Mango. this will be exported ;)
  DEBUG = true;//!!process.env['MONGODAO_DEBUG'];

/**
 *
 * @param {String} [str]
 * @returns {ObjectID}
 */
function newObjectID(str) {
  return new ObjectID(str);
}

/**
 *
 * @param {String} key
 * @param {*} value
 * @param {String} [prefix]
 * @returns {Object}
 */
function tuple(key, value, prefix) {
  var tuple = {};
  if (prefix) {
    key = prefix + '.' + key;
  }
  tuple[key] = value;
  return tuple;
}

/**
 *
 * @param target {Object}
 * @param context {Object}
 * @param funcNames {Array}
 * @param [prefix] {String}
 * @param [suffix] {String}
 * @returns {Object} the given target with promise-returning functions
 */
function denodeifyAll(target, context, funcNames, prefix, suffix) {
  target = target || {};
  prefix = prefix || '';
  suffix = suffix || '';
  funcNames.forEach(function (funcName) {
    var func = context[funcName];
    if (_.isFunction(func)) {
      target[prefix + funcName + suffix] = Q.nbind(func, context);
    }
  });
  return target;
}

//
//
//

function NoCache() {
  DEBUG && console.log('create NoCache');
}

NoCache.prototype.get = function (key) {
  DEBUG && console.log('cache get', key);
  return Q.resolve(null);//always miss!
};

NoCache.prototype.set = function (key, value) {
  DEBUG && console.log('cache set', key, value);
  // do nothing
};

NoCache.prototype.del = function (key) {
  DEBUG && console.log('cache del', key);
  // do nothing
};

NoCache.prototype.close = function (flush) {
  DEBUG && console.log('cache close', flush);
  // do nothing
};

//
//
//

function MemoryCache() {
  DEBUG && console.log('create MemoryCache');
  this.storage = {};
  this.stats = {
    get: 0,
    hit: 0,
    miss: 0,
    set: 0,
    update: 0,
    insert: 0,
    del: 0
  };
}

MemoryCache.prototype.get = function (key) {
  var value = this.storage[key];
  this.stats.get += 1;
  if (value) {
    this.stats.hit += 1;
  } else {
    this.stats.miss += 1;
  }
  return Q(value);
};

MemoryCache.prototype.set = function (key, value) {
  this.stats.set += 1;
  if (this.storage[key]) {
    this.stats.update += 1;
  } else {
    this.stats.insert += 1;
  }
  this.storage[key] = value;
};

MemoryCache.prototype.del = function (key) {
  this.stats.del += 1;
  delete this.storage[key];
};

MemoryCache.prototype.close = function () {
  DEBUG && console.log('MemoryCache stats:', this.stats);
};

//
//
//

function RedisCache(redisClient) {
  DEBUG && console.log('create RedisCache');
  this.redisClient = redisClient;
}

RedisCache.prototype.get = function (key) {
  var d = Q.defer();
  this.redisClient.get(key, function (err, value) {
    return err ? d.reject(err) : d.resolve(value);
  });
  return d.promise;
};

RedisCache.prototype.set = function (key, value) {
  this.redisClient.set(key, value);//no callback!
};

RedisCache.prototype.del = function (key) {
  this.redisClient.del(key);//no callback!
};

RedisCache.prototype.close = function () {
  this.redisClient.close();
  this.redisClient = null;
};

//
//
//

/**
 * Promise-oriented wrapper for mongodb.Collection.
 *
 * @param {monodb.Collection} collection
 * @param {Object} [options]
 * @constructor
 */
function MangoCollection(collection, options) {
  DEBUG && console.log('create MangoCollection for', collection.collectionName, 'on', collection.db.databaseName, 'with options', options);

  this.collection = collection; // underlying mongodb.Collection

  this._options = _.defaults(options, {
    defaults: function () {
      return {};
    }
  });

  this._cacheKeyPrefix = 'mango_' + collection.db.databaseName + '_' + collection.collectionName + '_';

  denodeifyAll(this, this.collection, MONGODB_COLLECTION_FUNCTIONS);
}

/**
 *
 * @param {Object} obj
 * @returns {boolean}
 */
MangoCollection.prototype.isNew = function (obj) {
  return obj && !obj._id;
};

/**
 *
 * @param {Object} [obj]
 * @returns {Object} new object with defaults
 */
MangoCollection.prototype.createNew = function (obj) {
  return _.defaults(obj || {}, this._options.defaults());
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @returns {Promise} loaded object
 */
MangoCollection.prototype.load = function (id) {
  var query = {_id: id};
  var options = {fields: {}};//TODO: custom fields

  if (!mango.cache) {
    DEBUG && console.log('load findOne-->', query, options);
    return this.findOne(query, options);
  }

  var self = this;
  var cacheKey = this._cacheKeyPrefix + id;
  return mango.cache.get(cacheKey)
    .then(function (result) {
      if (result) {
        return result;
      }
      DEBUG && console.log('load findOne-->', query, options);
      return self.findOne(query, options).then(function (result) {
        if (result) {
          mango.cache.set(cacheKey, result);
        }
        return result;
      });
    })
};

/**
 *
 * @param {Object} obj
 * @returns {Promise} stored object
 */
MangoCollection.prototype.store = function (obj) {
  var promise;

  if (this.isNew(obj)) {
    DEBUG && console.log('store insert-->');
    promise = this.insert(obj, {w: 1});
  } else {
    var query = {_id: obj._id};
    var update = {$set: _.omit(obj, '_id')};
    var options = {w: 1, upsert: true, new: 1, fields: {}};//TODO: custom fields

    DEBUG && console.log('store findAndModify-->', query, update, options);
    promise = this.findAndModify(query, [], update, options);
  }

  var self = this;
  return promise.then(function (result) {
    var obj = result && result[0];
    if (obj && mango.cache) {
      mango.cache.set(self._cacheKeyPrefix + obj._id, obj);
    }
    return obj; // stored doc
  });
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @returns {Promise} affected rows(should be 1)
 */
MangoCollection.prototype.destroy = function (id) {
  var self = this;
  var query = {_id: id};
  var options = {w: 1, single: true};

  DEBUG && console.log('destroy remove-->', query, options);
  return this.remove(query, options).then(function (result) {
    if (result && mango.cache) {
      mango.cache.del(self._cacheKeyPrefix + id);
    }
    return result; // affectedRow
  });
};

/**
 * TODO: read cache support?
 *
 * @param [options] {Object}
 * @returns {Promise}
 */
MangoCollection.prototype.all = function (options) {
  var self = this;
  var query = {};
  var options = _.defaults({fields: {}}, options);

  DEBUG && console.log('all find-->', query, options);

  // this will cause OOM
  //return Q.ninvoke(this.collection.find(query, options), 'toArray');

  // to avoid OOM
  // using cursor
  var d = Q.defer();
  var stream = this.collection.find(query, options).stream();
  var count = 0;
  stream.on('data', function (data) {
    ++count;
    if (data && mango.cache) {
      mango.cache.set(self._cacheKeyPrefix + data._id, data);
    }
    return d.notify(data);
  });
  stream.on('end', function () {
    return d.resolve(count);
  });
  stream.on('error', function (err) {
    return d.reject(err);
  });
  return d.promise;
};

//
//
//

MangoCollection.prototype._findOneAndModify = function (query, update, options) {
  if (!mango.cache) {
    return this.findAndModify(query, [], update, options).then(function (result) {
      return result && result[0];
    });
  }

  var self = this;
  options.new = 1;
  options.fields = {};//TODO: custom fields
  return this.findAndModify(query, [], update, options).then(function (result) {
    var obj = result && result[0];
    if (obj) {
      mango.cache.set(self._cacheKeyPrefix + obj._id, obj);
    }
    return obj; // modified doc
  });
};

MangoCollection.prototype._update = function (query, update, options) {
  if (!mango.cache) {
    return this.update(query, update, options).then(function (result) {
      return result && result[0];//0=affected rows,1=raw result
    });
  }

  return this._findOneAndModify(query, update, options).then(function (result) {
    return result ? 1 : 0; // mimic affected rows
  });
};

//
// embedded field helpers
//

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @returns {Promise}
 */
MangoCollection.prototype.getField = function (id, field) {
  if (mango.cache) {
    DEBUG && console.log('getField load-->', id, field);
    return this.load(id).then(function (result) {
      return result && result[field];
    });
  }

  var query = {_id: id};
  var options = {fields: tuple(field, 1)};
  DEBUG && console.log('getField findOne-->', query, options);
  return this.findOne(query, options).then(function (result) {
    return result && result[field];
  });
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {*} value
 * @returns {Promise} affected rows(might be 1)
 */
MangoCollection.prototype.setField = function (id, field, value) {
  var query = {_id: id};
  var update = {$set: tuple(field, value)};
  var options = {w: 1};

  DEBUG && console.log('setField update-->', query, update, options);
  return this._update(query, update, options);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @returns {Promise} affected rows(might be 1)
 */
MangoCollection.prototype.removeField = function (id, field) {
  var query = {_id: id};
  var update = {$unset: tuple(field, 1)};
  var options = {w: 1};

  DEBUG && console.log('removeField update-->', query, update, options);
  return this._update(query, update, options);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {String} [collectionName] for manual reference
 * @param {String} [dbName] for manual reference
 * @returns {Promise}
 */
MangoCollection.prototype.loadField = function (id, field, collectionName, dbName) {
  DEBUG && console.log('loadField-->', id, field);
  var self = this;
  return this.getField(id, field).then(function (value) {
    var docId;
    if (_.isObject(value)) {
      // FIXME: not working yet! mongodb-native-driver modified result document!
      docId = value.$id;
      collectionName = value.$ref || self.collection.collectionName;
      dbName = value.$db || self.collection.db.databaseName;
      DEBUG && console.log('loadField load with DBRef-->', docId, collectionName, dbName);
    } else {
      docId = value;
      collectionName = collectionName || self.collection.collectionName;
      dbName = dbName || self.collection.db.databaseName;
      DEBUG && console.log('loadField load with manual reference', docId, collectionName, dbName);
    }
    return mango[dbName][collectionName].load(docId);
  });
};

//
// embedded array field helper
//

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {*} value
 * @returns {Promise}
 */
MangoCollection.prototype.addElement = function (id, field, value) {
  var query = {_id: id};
  var update = {$push: tuple(field, value)};
  var options = {w: 1};

  DEBUG && console.log('addElement update-->', query, update, options);
  return this._update(query, update, options);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {Array} values
 * @returns {Promise}
 */
MangoCollection.prototype.addElements = function (id, field, values) {
  var query = {_id: id};
  var update = {$push: tuple(field, {$each: values})};
  var options = {w: 1};

  DEBUG && console.log('addElements update-->', query, update, options);
  return this._update(query, update, options);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {*} value
 * @returns {Promise}
 */
MangoCollection.prototype.removeElement = function (id, field, value) {
  var query = {_id: id};
  var update = {$pull: tuple(field, value)};
  var options = {w: 1};

  DEBUG && console.log('removeElement update-->', query, update, options);
  return this._update(query, update, options);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {Array} values
 * @returns {Promise}
 */
MangoCollection.prototype.removeElements = function (id, field, values) {
  var query = {_id: id};
  var update = {$pullAll: tuple(field, values)};
  var options = {w: 1};

  DEBUG && console.log('removeElements update -->', query, update, options);
  return this._update(query, update, options);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {*} value
 * @returns {Promise} contains
 */
MangoCollection.prototype.hasElement = function (id, field, value) {
  if (mango.cache) {
    DEBUG && console.log('hasElement load-->', id, field);
    return this.load(id).then(function (result) {
      return result && _.contains(result[field], value);
    });
  }

  var query = {_id: id};
  query[field] = value;

  DEBUG && console.log('hasElement count-->', query);
  return this.count(query).then(function (result) {
    return result > 0;
  });
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {Array} values
 * @returns {Promise} contains any or not
 */
MangoCollection.prototype.hasSomeElements = function (id, field, values) {
  if (mango.cache) {
    DEBUG && console.log('hasSomeElements load-->', id, field);
    return this.load(id).then(function (result) {
      return result && _.some(values, function (value) {
        return _.contains(result[field], value);
      });
    });
  }

  var query = {_id: id};
  query[field] = {$in: values};

  DEBUG && console.log('hasSomeElements count-->', query);
  return this.count(query).then(function (result) {
    return result > 0;
  });
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {Array} values
 * @returns {Promise} contains all or not
 */
MangoCollection.prototype.hasAllElements = function (id, field, values) {
  if (mango.cache) {
    DEBUG && console.log('hasAllElements load-->', id, field);
    return this.load(id).then(function (result) {
      return result && _.every(values, function (value) {
        return _.contains(result[field], value);
      });
    });
  }

  var query = {_id: id};
  query[field] = {$all: values};

  DEBUG && console.log('hasAllElements count-->', query);
  return this.count(query).then(function (result) {
    return result > 0;
  });
};

//
// embedded object field helper
//

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {String} key
 * @returns {Promise} property value
 */
MangoCollection.prototype.getProperty = function (id, field, key) {
  if (mango.cache) {
    DEBUG && console.log('getProperties load-->', id, field, key);
    return this.load(id).then(function (result) {
      return result && result[field] && result[field][key];
    });
  }

  var query = {_id: id};
  var options = {fields: tuple(key, 1, field)};

  DEBUG && console.log('getProperties findOne-->', query, options);
  return this.findOne(query, options).then(function (result) {
    return result && result[field] && result[field][key];
  });
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {String} key
 * @param {*} value
 * @returns {Promise} affected rows(should be 1)
 */
MangoCollection.prototype.setProperty = function (id, field, key, value) {
  var query = {_id: id};
  var update = {$set: tuple(key, value, field)};
  var options = {w: 1};

  DEBUG && console.log('setProperty update-->', query, update, options);
  return this._update(query, update, options);
};

/**
 * TODO: cache support
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {String} key
 * @param {*} value
 * @returns {Promise} incremented value
 */
MangoCollection.prototype.incProperty = function (id, field, key, value) {
  var query = {_id: id};
  var update = {$inc: tuple(key, value, field)};
  var options = {w: 1, new: 1, fields: tuple(key, 1, field)};

  DEBUG && console.log('incProperty findAndModify-->', query, update, options);
  return this._findOneAndModify(query, update, options).then(function (result) {
    return result && result[field] && result[field][key];
  });
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @param {String} field
 * @param {String} key
 * @returns {Promise} affected row(should be 1)
 */
MangoCollection.prototype.removeProperty = function (id, field, key) {
  var query = {_id: id};
  var update = {$unset: tuple(key, 1, field)};
  var options = {w: 1};

  DEBUG && console.log('removeProperties update-->', query, update, options);
  return this._update(query, update, options);
};

//
//
//


/**
 * wrapper for mongodb.Db.
 *
 * @param {monodb.Db} db
 * @param {Object} [options]
 * @constructor
 */
function MangoDb(db, options) {
  DEBUG && console.log('create MangoDb for', db.databaseName, 'with options', options);

  this.db = db; //underlying mongodb.Db

  this._mangoCollections = {};

  // for convenience,
  // allow shortcut format: array of collection name
  if (_.isArray(options)) {
    // turn into standard options format
    options = {
      collections: _.reduce(options, function (collections, name) {
        collections[name] = {_auto: 1};
        return collections;
      }, {})
    };
  }

  var collections = options.collections || options || {};

  var self = this;
  db.collectionNames({namesOnly: 'true'}, function (err, names) {
    if (err) throw err;

    collections = _.reduce(names, function (collections, name) {
      if (!collections[name]) {
        var matches = /^\w+\.(\w+)$/.exec(name);
        if (matches) {
          collections[matches[1]] = {_auto: 2};
        }
      }
      return collections;
    }, collections);

    _.each(collections, function (options, name) {
      self.__defineGetter__(name, function () {
        var mongoCollection = self._mangoCollections[name];
        if (mongoCollection) {
          DEBUG && console.log('use existing MangoCollection for', name);
          return mongoCollection;
        }
        return self._mangoCollections[name] = new MangoCollection(db.collection(name), options);
      });
    });

    DEBUG && console.log('available collections on', db.databaseName, 'are:', _.keys(collections));
  });
}

/**
 *
 * @param {String} name
 * @returns {MangoCollection}
 */

/**
 * close the underlying db connection.
 *
 * @param callback
 */
MangoDb.prototype.close = function (callback) {
  if (this.db) {
    this.db.close(callback);
    this.db = null;
  }
};

//
//
//

function Mango() {
  this._mangoDbs = {};

  process.on('exit', this.close.bind(this));
};

/**
 *
 * @param {String} url MongoClient.connect url
 * @param {Object} [options] MongoClient.connect options
 * @param {Object} [mangoOptions] mango specific options
 * @returns {Promise} MangoDb instance with mongodb connection
 */
Mango.prototype.connect = function (url, options, mangoOptions) {
  var self = this;
  DEBUG && console.log('create mongodb connection to', url);
  return Q.ninvoke(MongoClient, 'connect', url, options)
    .then(function (db) {
      var dbName = db.databaseName;
      var mangoDb = self._mangoDbs[dbName] = new MangoDb(db, mangoOptions);
      self.__defineGetter__(dbName, function () {
        return mangoDb;
      });
      return mangoDb;
    });
};

/**
 * close all db connections
 */
Mango.prototype.close = function () {
  if (this.cache) {
    this.cache.close();
  }

  _.each(this._mangoDbs, function (mangoDb) {
    mangoDb.close();
  });
};


/**
 *
 * @param {Object|String} config mango configuration
 * @param {Object} [cache] cache driver
 * @returns {Mango} Mango instance(itself)
 */
Mango.prototype.configure = function (config, cache) {
  var self = this;

  // for convenience,
  // allow shortcut form: configure(url)
  if (_.isString(config)) {
    // transform to standard config format
    config = {db: {mongo: {url: config}, mango: {collections: {}}, _auto: 1}};
  }

  _.each(config, function (dbConfig) {
    var url = (dbConfig.mongo && dbConfig.mongo.url) || dbConfig.url || ''; //''=error prone
    var options = (dbConfig.mongo && dbConfig.mongo.options) || dbConfig.options || {};
    var mangoOptions = dbConfig.mango || dbConfig || {};
    self.connect(url, options, mangoOptions)
      .then(function (mangoDb) {
        DEBUG && console.log('connection established for', mangoDb.db.databaseName);
      }).fail(function () {
        DEBUG && console.log('mongodb connection failed', err);
      }).done();
    // FIXME: how to wait until connection is established?
  });

  if (cache) {
    DEBUG && console.log('mango cache enabled!');
    mango.cache = cache;
  }

  return this;
};


module.exports = mango = new Mango();

// for convenience,
module.exports.ObjectID = ObjectID;
module.exports.newObjectID = newObjectID;
module.exports.tuple = tuple;
module.exports.denodeifyAll = denodeifyAll;
module.exports.NoCache = NoCache;
module.exports.MemoryCache = MemoryCache;
module.exports.RedisCache = RedisCache;
module.exports.MangoCollection = MangoCollection;
module.exports.MangoDb = MangoDb;
module.exports.Mango = Mango;
