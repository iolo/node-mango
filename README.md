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

configure mango with configuration:

```javascript
var mango = require('mango').configure(config);
```

or, configure it later, if you want:

```javascript
var mango = require('mango');
...
mango.configure('mongodb://localhost/test');
```

alternatively, connect directly if you need a single db connection:

```javascript
var mango = require('mango');
mango.connect('mongodb://localhost/test').then(function(testDb) {
  ... // NOTE: testDb is not mongodb.Db instance but mango.MangoDb instance
}).done();
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

to generate api reference:

```
grunt jsdoc
```

to browse api reference:

```
open docs/index.html
```

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
