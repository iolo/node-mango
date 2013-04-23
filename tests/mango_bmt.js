'use strict';

var
  util = require('util'),
  _ = require('underscore'),
  Q = require('q'),
  mango = require('../libs/mango'),
  testdocs,
  fixtures = require('./mango_bmt_fixtures');

module.exports = {
  setUp: function (callback) {
    var fixturesLoader = require('pow-mongodb-fixtures').connect('test');
    fixturesLoader.clearAndLoad(fixtures, function (err) {
      if (err) throw err;
      fixturesLoader.client.close();

      var cache = process.env['MANGO_CACHE'] && new mango.MemoryCache();
      mango.connect('mongodb://localhost/test', {}, {}, cache)
        .then(function (testdb) {
          testdocs = testdb.testdocs;
        })
        .done(callback);
    });
  },
  tearDown: function (callback) {
    mango.close();
    callback();
  },
  test_bmt: function (test) {
    console.time('BMT');
    var queued = 0, completed = 0, jobcount = 500, jobinterval = 10, samplecount = 100;
    var timer = setInterval(function jobmain() {
      var x = Math.floor(Math.random() * samplecount);
      testdocs.load(fixtures.testdocs[x]._id)
        .then(function (result) {
          console.log(completed, result._id);
        }).done(function () {
          if (++completed === jobcount) {
            console.timeEnd('BMT');
            test.done();
          }
        });
      if (++queued === jobcount) {
        clearInterval(timer);
      }
    }, jobinterval);
  }
};