'use strict';

var ObjectID = require('mongodb').ObjectID;

var testdocs = [];
var words = ['foo', 'bar', 'baz', 'qux', 'hello', 'world', 'quick', 'brown', 'silver', 'fox'];

for (var i = 0; i < 1000; i++) {
  testdocs.push({
    _id: new ObjectID(),
    name: words[i % words.length],
    foo: words[i % words.length] + words[i % words.length],
    bar: words[i % words.length] + words[i % words.length] + words[i % words.length],
    baz: words[i % words.length] + words[i % words.length] + words[i % words.length] + words[i % words.length],
    num: i
  });
}

exports.testdocs = testdocs;
