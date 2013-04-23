'use strict';

var
  _ = require('underscore'),
  Q = require('q'),
  MONGODB_COLLECTION_FUNCTIONS = [
    'insert', 'remove', 'rename', 'save', 'update', 'distinct', 'count',
    'findAndModify', 'findAndRemove', 'find', 'findOne',
    'createIndex', 'ensureIndex', 'indexInformation', 'dropIndex', 'dropAllIndexes', 'reIndex',
    'mapReduce', 'group', 'options', 'isCapped', 'indexExists',
    'geoNear', 'geoHaystackSearch', 'indexes', 'aggregate', 'stats'
  ],
  mango = require('./mango'),
  DEBUG = !!process.env['MANGO_DEBUG'];

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
 * Promise-oriented wrapper for mongodb.Collection.
 *
 * @param {MangoDb} mangoDb container
 * @param {monodb.Collection} collection underlying mongodb.Collection instance
 * @param {Object} [options]
 * @param {MangoCache} [cache]
 * @constructor
 */
function MangoCollection(mangoDb, collection, options, cache) {
  DEBUG && console.log('create MangoCollection for', collection.collectionName, 'on', collection.db.databaseName);

  this.mangoDb = mangoDb;
  this.collection = collection;
  this.options = _.extend({
    defaults: function () {
      return {};
    }
  }, options);
  this.cache = cache && cache.derive(collection.collectionName);

  // make promise-returning for most collection methods
  var self = this;
  MONGODB_COLLECTION_FUNCTIONS.forEach(function (funcName) {
    var func = collection[funcName];
    if (_.isFunction(func)) {
      self[funcName + 'Promise'] = Q.nbind(func, collection);
    }
  });
}

/**
 * close the underlying db connection.
 */
MangoCollection.prototype.close = function () {
  if (this.cache) {
    this.cache.close();
    this.cache = null;
  }

  if (this.collection) {
    //this.collection.close();
    this.collection = null;
  }
};

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
  return _.extend(this.options.defaults(), obj);
};

/**
 *
 * @param {ObjectID|String|Number} id
 * @returns {Promise} loaded object
 */
MangoCollection.prototype.load = function (id) {
  var query = {_id: id};
  var options = {fields: {}};//TODO: custom fields

  if (!this.cache) {
    DEBUG && console.log('load findOne-->', query, options);
    return this.findOnePromise(query, options);
  }

  var self = this;
  return this.cache.get(id)
    .then(function (result) {
      if (result) {
        DEBUG && console.log('load cache-->');
        return result; // cache hit!
      }
      DEBUG && console.log('load findOne-->', query, options);
      return self.findOnePromise(query, options).then(function (result) {
        if (result) {
          self.cache.set(id, result);
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
    promise = this.insertPromise(obj, {w: 1});
  } else {
    var query = {_id: obj._id};
    var update = {$set: _.omit(obj, '_id')};
    var options = {w: 1, upsert: true, new: 1, fields: {}};//TODO: custom fields

    DEBUG && console.log('store findAndModify-->', query, update, options);
    promise = this.findAndModifyPromise(query, [], update, options);
  }

  var self = this;
  return promise.then(function (result) {
    var obj = result && result[0];
    if (obj && self.cache) {
      self.cache.set(obj._id, obj);
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
  var query = {_id: id};
  var options = {w: 1, single: true};

  DEBUG && console.log('destroy remove-->', query, options);
  var self = this;
  return this.removePromise(query, options).then(function (result) {
    if (result && self.cache) {
      self.cache.del(id);
    }
    return result; // affectedRow
  });
};

/**
 * TODO: read cache support?
 *
 * @param {Object} [options]
 * @param {boolean} [asArray]
 * @returns {Promise}
 */
MangoCollection.prototype.all = function (options, asArray) {
  var query = {};
  var options = _.extend({fields: {}}, options);

  var self = this;
  if (asArray) {
    // NOTE: this will cause OOM
    DEBUG && console.log('all find toArray-->', query, options);
    return Q.ninvoke(this.collection.find(query, options), 'toArray').then(function (result) {
      if (result && self.cache) {
        _.each(result, function (doc) {
          self.cache.set(doc._id, data);
        });
      }
      return result;
    });
  }

  // to avoid OOM, using stream
  var d = Q.defer();
  DEBUG && console.log('all find stream-->', query, options);
  var stream = this.collection.find(query, options).stream();
  var count = 0;
  stream.on('data', function (doc) {
    ++count;
    if (doc && self.cache) {
      self.cache.set(doc._id, doc);
    }
    return d.notify(doc);
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
  if (!this.cache) {
    return this.findAndModifyPromise(query, [], update, options).then(function (result) {
      return result && result[0];
    });
  }

  options.new = 1;
  options.fields = {};//TODO: custom fields

  var self = this;
  return this.findAndModifyPromise(query, [], update, options).then(function (result) {
    var obj = result && result[0];
    if (obj && self.cache) {
      self.cache.set(obj._id, obj);
    }
    return obj; // modified doc
  });
};

MangoCollection.prototype._update = function (query, update, options) {
  if (!this.cache) {
    return this.updatePromise(query, update, options).then(function (result) {
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
  if (this.cache) {
    DEBUG && console.log('getField load-->', id, field);
    return this.load(id).then(function (result) {
      return result && result[field];
    });
  }

  var query = {_id: id};
  var options = {fields: tuple(field, 1)};
  DEBUG && console.log('getField findOne-->', query, options);
  return this.findOnePromise(query, options).then(function (result) {
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
      collectionName = value.$ref;
      dbName = value.$db;
      DEBUG && console.log('loadField load with DBRef-->', docId, collectionName, dbName);
    } else {
      docId = value;
      DEBUG && console.log('loadField load with manual ref-->', docId, collectionName, dbName);
    }

    if (dbName) {
      // ref to another db
      return self.mangoDb.mango.db(dbName).then(function (mangoDb) {
        return mangoDb.collection(collectionName).load(docId);
      });
    }

    if (collectionName) {
      // ref to same db
      return self.mangoDb.collection(collectionName).load(docId);
    }

    // collection self ref
    return self.load(docId);
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
  if (this.cache) {
    DEBUG && console.log('hasElement load-->', id, field);
    return this.load(id).then(function (result) {
      return result && _.contains(result[field], value);
    });
  }

  var query = {_id: id};
  query[field] = value;

  DEBUG && console.log('hasElement count-->', query);
  return this.countPromise(query).then(function (result) {
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
  if (this.cache) {
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
  return this.countPromise(query).then(function (result) {
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
  if (this.cache) {
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
  return this.countPromise(query).then(function (result) {
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
  if (this.cache) {
    DEBUG && console.log('getProperties load-->', id, field, key);
    return this.load(id).then(function (result) {
      return result && result[field] && result[field][key];
    });
  }

  var query = {_id: id};
  var options = {fields: tuple(key, 1, field)};

  DEBUG && console.log('getProperties findOne-->', query, options);
  return this.findOnePromise(query, options).then(function (result) {
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

module.exports.MangoCollection = MangoCollection;

