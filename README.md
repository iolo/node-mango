node-mango
==========

simple mongodb wrapper library for nodejs

Features
--------

* promise-returning wrapper methods for all mongodb collection methods.
* DAO-like methods: createNew/load/store/destroy/all
* embedded fields helper methods: getField/setField/removeField/loadField/...
* embedded array fields helper methods: addElement/addElements/removeElement/removeElements/hasElemnt/hasSomeElements/hasAllElements
* embedded object fields helper methods: getProperty/setProperty/incProperty/removeProperty
* document level cache with memory, redis or something(**EXPREIMENTAL**)

Getting Started
---------------

install module using npm:

```
npm install mango
```

prepare configuration:

```javascript
var config = {
  test: {
    mongo: {
      url: 'mongodb://localhost/test',
      options: {
      }
    },
    mango: {
      collections: {
        users: {
        },
        posts: {
        },
        tags: {
        }
      }
    }
  }
};
```

load configuration from file:

```javascript
var config = require('../config.json');
```

configure without cache:

```javascript
var mango = require('mango').configure(config);
```

configure with cache:

```javascript
var mango = require('mango').configure(config, new mango.RedisCache(require('redis').createRedisClient()));
```

configure single db connection with default configuration:

```javascript
var mango = require('mango').configure('mongodb://localhost/test');
```

execute queries using promise:

```javascript
mango.test.users.findOne({name: 'foo'});
  .then(function (result) {
  })
  .fail(function (err) {
  })
  .done();
```

Configuration
-------------

**TBW**

Cache
-----

**TBW**

API Reference
-------------

**TBW**

Dependencies
------------

* [node-mongodb-native](https://github.com/mongodb/node-mongodb-native)
* [underscore](http://underscorejs.org)
* [q](http://documentup.com/kriskowal/q/)
* and optional and test dependencies...

Notes
-----

plz, see source code in ```libs``` directory and test code in ```tests``` directory until documents are available ;).

*May the source be with you...*
