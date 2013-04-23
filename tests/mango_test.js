'use strict';

var
  util = require('util'),
  _ = require('underscore'),
  Q = require('q'),
  mango = require('../libs/mango'),
  testdocs,
  fixtures = require('./mango_fixtures');

module.exports = {
  setUp: function (callback) {
    var fixturesLoader = require('pow-mongodb-fixtures').connect('test');
    fixturesLoader.clearAndLoad(fixtures, function (err) {
      if (err) throw err;
      fixturesLoader.client.close();

      var cache = process.env['MANGO_CACHE'] && new mango.MemoryCache();
      var config = {
        test: {
          mongo: {
            url: 'mongodb://localhost/test'
          },
          mango: {
            collections: {
              testdocs: {},
              testusers: {}
            }
          }
        }
      };
      mango.configure(config, cache, function () {
        testdocs = mango.test.testdocs;
        callback();
      });
    });
  },
  tearDown: function (callback) {
    mango.close();
    callback();
  },
  test_load: function (test) {
    testdocs.load(fixtures.testdocs.doc1._id)
      .then(function (result) {
        console.log('-->load:', result);
        test.ok(result);
        test.ok(!testdocs.isNew(result));
        test.equal(result._id.toString(), fixtures.testdocs.doc1._id.toString());
        test.equal(result.name, fixtures.testdocs.doc1.name);
        test.deepEqual(result.arr, fixtures.testdocs.doc1.arr);
        test.deepEqual(result.props, fixtures.testdocs.doc1.props);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_load_not_found: function (test) {
    testdocs.load(mango.newObjectID())
      .then(function (result) {
        console.log('-->load:', result);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_store_create: function (test) {
    var newDoc = testdocs.createNew({name: 'foo', arr: ['a', 'b', 'c'], props: {a: 'a', b: 'b', c: 'c'}});
    console.log('createNew:', newDoc);
    test.ok(testdocs.isNew(newDoc));
    test.equal(newDoc.name, 'foo');
    testdocs.store(newDoc)
      .then(function (result) {
        console.log('-->store:', result, typeof result._id);
        test.ok(result);
        test.ok(!testdocs.isNew(result));
        test.equal(result.name, newDoc.name);
        test.deepEqual(result.arr, newDoc.arr);
        test.deepEqual(result.props, newDoc.props);

        return [result, testdocs.load(result._id)];
      })
      .spread(function (storedDoc, result) {
        console.log('-->load:', result);
        test.ok(result);
        test.equal(result._id.toString(), storedDoc._id.toString());
        test.equal(result.name, storedDoc.name);
        test.deepEqual(result.arr, storedDoc.arr);
        test.deepEqual(result.props, storedDoc.props);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_store_update: function (test) {
    testdocs.load(fixtures.testdocs.doc1._id)
      .then(function (result) {
        console.log('-->load:', result);
        test.ok(result);
        test.equal(result._id.toString(), fixtures.testdocs.doc1._id.toString());

        result.name = 'hello';
        result.arr = ['quick', 'brown', 'silver', 'fox'];
        result.props = {'quick': 'brown', 'silver': 'fox'};
        return testdocs.store(result, true);
      })
      .then(function (result) {
        console.log('-->store:', result);
        test.ok(result);
        test.equal(result._id.toString(), fixtures.testdocs.doc1._id.toString());
        test.equal(result.name, 'hello');
        test.deepEqual(result.arr, ['quick', 'brown', 'silver', 'fox']);
        test.deepEqual(result.props, {'quick': 'brown', 'silver': 'fox'});

        return [result, testdocs.load(result._id)];
      })
      .spread(function (storedDoc, result) {
        console.log('-->load:', result);
        test.ok(result);
        test.equal(result._id.toString(), storedDoc._id.toString());
        test.equal(result.name, storedDoc.name);
        test.deepEqual(result.arr, storedDoc.arr);
        test.deepEqual(result.props, storedDoc.props);
      })
      .fail(test.ifError)
      .done(test.done);

  },
  test_destroy: function (test) {
    testdocs.destroy(fixtures.testdocs.doc1._id)
      .then(function (result) {
        console.log('-->destroy:', result);
        test.ok(result, 1);

        return testdocs.load(fixtures.testdocs.doc1._id);
      })
      .then(function (result) {
        console.log('-->load:', result);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_destroy_not_found: function (test) {
    testdocs.destroy(mango.newObjectID())
      .then(function (result) {
        console.log('-->destroy:', result);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getField: function (test) {
    testdocs.getField(fixtures.testdocs.doc1._id, 'props')
      .then(function (result) {
        //console.log(arguments);
        test.ok(result);
        test.equal(result.one, fixtures.testdocs.doc1.props.one);
        test.equal(result.two, fixtures.testdocs.doc1.props.two);
        test.equal(result.three, fixtures.testdocs.doc1.props.three);

        return testdocs.getField(fixtures.testdocs.doc2._id, 'props');
      })
      .then(function (result) {
        //console.log(arguments);
        test.ok(result);
        test.equal(result.one, fixtures.testdocs.doc2.props.one);
        test.equal(result.two, fixtures.testdocs.doc2.props.two);
        test.equal(result.three, fixtures.testdocs.doc2.props.three);

        return testdocs.getField(fixtures.testdocs.doc3._id, 'props');
      })
      .then(function (result) {
        //console.log(arguments);
        test.ok(result);
        test.equal(result.one, fixtures.testdocs.doc3.props.one);
        test.equal(result.two, fixtures.testdocs.doc3.props.two);
        test.equal(result.three, fixtures.testdocs.doc3.props.three);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getField_not_found_doc: function (test) {
    testdocs.getField(mango.newObjectID(), 'props')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getField_not_found_field: function (test) {
    testdocs.getField(fixtures.testdocs.doc1._id, '__not_found__')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_setField: function (test) {
    testdocs.setField(fixtures.testdocs.doc1._id, 'props', '__new_value__')
      .then(function (result) {
        console.log(arguments);
        test.ok(result);

        return testdocs.getField(fixtures.testdocs.doc1._id, 'props');
      })
      .then(function (result) {
        console.log(arguments);
        test.equal(result, '__new_value__');
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeField: function (test) {
    testdocs.removeField(fixtures.testdocs.doc1._id, 'props')
      .then(function (result) {
        console.log(arguments);
        test.equal(result, 1);

        return testdocs.getField(fixtures.testdocs.doc1._id, 'props');
      })
      .then(function (result) {
        console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_loadField_manual: function (test) {
    testdocs.loadField(fixtures.testdocs.doc1._id, 'author', 'testusers')
      .then(function (result) {
        console.log(arguments);
        test.ok(result);
        test.equal(result._id, fixtures.testdocs.doc1.author);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_addElement: function (test) {
    testdocs.hasElement(fixtures.testdocs.doc1._id, 'arr', 'foo')
      .then(function (result) {
        test.ok(!result);
        return testdocs.addElement(fixtures.testdocs.doc1._id, 'arr', 'foo');
      })
      .then(function (result) {
        test.ok(result, 1);
        return testdocs.hasElement(fixtures.testdocs.doc1._id, 'arr', 'foo');
      })
      .then(function (result) {
        test.ok(result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_addElements: function (test) {
    testdocs.hasAllElements(fixtures.testdocs.doc1._id, 'arr', ['foo', 'bar'])
      .then(function (result) {
        test.ok(!result);
        return testdocs.addElements(fixtures.testdocs.doc1._id, 'arr', ['foo', 'bar']);
      })
      .then(function (result) {
        test.ok(result, 1);
        return testdocs.hasAllElements(fixtures.testdocs.doc1._id, 'arr', ['foo', 'bar']);
      })
      .then(function (result) {
        test.ok(result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeElement: function (test) {
    testdocs.hasElement(fixtures.testdocs.doc1._id, 'arr', 'one')
      .then(function (result) {
        test.ok(result);
        return testdocs.removeElement(fixtures.testdocs.doc1._id, 'arr', 'one');
      })
      .then(function (result) {
        test.ok(result, 1);
        return testdocs.hasElement(fixtures.testdocs.doc1._id, 'arr', 'one');
      })
      .then(function (result) {
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeElements: function (test) {
    testdocs.hasAllElements(fixtures.testdocs.doc1._id, 'arr', ['one', 'two'])
      .then(function (result) {
        test.ok(result);
        return testdocs.removeElements(fixtures.testdocs.doc1._id, 'arr', ['one', 'two']);
      })
      .then(function (result) {
        test.ok(result, 1);
        return testdocs.hasAllElements(fixtures.testdocs.doc1._id, 'arr', ['one', 'two']);
      })
      .then(function (result) {
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_hasSomeElements: function (test) {
    testdocs.hasSomeElements(fixtures.testdocs.doc1._id, 'arr', ['one', '1', '2', '3'])
      .then(function (result) {
        test.ok(result);
        return testdocs.hasSomeElements(fixtures.testdocs.doc1._id, 'arr', ['1', '2', '3']);
      })
      .then(function (result) {
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getProperty: function (test) {
    testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', 'one')
      .then(function (result) {
        //console.log(arguments);
        test.ok(result);
        test.equal(result, fixtures.testdocs.doc1.props.one);

        return testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', 'two');
      })
      .then(function (result) {
        //console.log(arguments);
        test.ok(result);
        test.equal(result, fixtures.testdocs.doc1.props.two);

        return testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', 'three');
      })
      .then(function (result) {
        //console.log(arguments);
        test.ok(result);
        test.equal(result, fixtures.testdocs.doc1.props.three);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getProperty_not_found_key: function (test) {
    testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', '__not_found__')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getProperty_not_found_field: function (test) {
    testdocs.getProperty(fixtures.testdocs.doc1._id, '__not_found__', 'one')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_getProperty_not_found_doc: function (test) {
    testdocs.getProperty(mango.newObjectID(), 'props', 'one')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_setProperty: function (test) {
    testdocs.setProperty(fixtures.testdocs.doc1._id, 'props', 'one', 'foo')
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 1);

        return testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', 'one');
      })
      .then(function (result) {
        //console.log(arguments);
        test.equal('foo', result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_setProperty_not_found_key: function (test) {
    testdocs.setProperty(fixtures.testdocs.doc1._id, 'props', '__not_found__', 'hello')
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 1);

        return testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', '__not_found__');
      })
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 'hello');
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_setProperty_not_found_field: function (test) {
    testdocs.setProperty(fixtures.testdocs.doc1._id, '__not_found__', 'foo', 'bar')
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 1);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_setProperty_not_found_doc: function (test) {
    testdocs.setProperty(mango.newObjectID(), 'props', 'one', 'hello')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_incProperty: function (test) {
    testdocs.incProperty(fixtures.testdocs.doc2._id, 'props', 'one', 100)
      .then(function (result) {
        console.log(arguments);
        test.equal(result, fixtures.testdocs.doc2.props.one + 100);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_incProperty_not_number: function (test) {
    testdocs.incProperty(fixtures.testdocs.doc1._id, 'props', 'one', 100)
      .then(function (result) {
        //console.log(arguments);
        test.ifError(result);
      })
      .fail(function (err) {
        //console.error(arguments);
        test.ok(err);
      })
      .done(test.done);
  },
  test_incProperty_not_found_key: function (test) {
    testdocs.incProperty(fixtures.testdocs.doc1._id, 'props', '__not_found__', 100)
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 100);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_incProperty_not_found_field: function (test) {
    testdocs.incProperty(fixtures.testdocs.doc1._id, '__not_found__', 'foo', 100)
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 100);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_incProperty_not_found_doc: function (test) {
    testdocs.incProperty(mango.newObjectID(), 'props', 'one', 100)
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeProperty: function (test) {
    testdocs.removeProperty(fixtures.testdocs.doc1._id, 'props', 'one')
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 1);

        return testdocs.getProperty(fixtures.testdocs.doc1._id, 'props', 'one')
      })
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeProperty_not_found_key: function (test) {
    testdocs.removeProperty(fixtures.testdocs.doc1._id, 'props', '__not_found__')
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 1);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeProperty_not_found_field: function (test) {
    testdocs.removeProperty(fixtures.testdocs.doc1._id, '__not_found__', 'foo')
      .then(function (result) {
        //console.log(arguments);
        test.equal(result, 1);
      })
      .fail(test.ifError)
      .done(test.done);
  },
  test_removeProperty_not_found_doc: function (test) {
    testdocs.removeProperty(mango.newObjectID(), 'props', 'one')
      .then(function (result) {
        //console.log(arguments);
        test.ok(!result);
      })
      .fail(test.ifError)
      .done(test.done);
  }
};
