'use strict';

var ObjectID = require('mongodb').ObjectID;

var doc1_id = new ObjectID();
var doc2_id = new ObjectID();
var doc3_id = new ObjectID();

exports.testdocs = {
  doc1: {
    _id: doc1_id,
    arr: [ 'one', 'two', 'three' ],
    props: {
      one: 'one',
      two: 'two',
      three: 'three'
    },
    author: 'iolo'
  },
  doc2: {
    _id: doc2_id,
    arr: [ '1', '2', '3' ],
    props: {
      one: 1,
      two: 2,
      three: 3
    },
    author: 'dupre'
  },
  doc3: {
    _id: doc3_id,
    arr: [ 'first', 'second', 'third' ],
    props: {
      one: 'first',
      two: 'second',
      three: 'third'
    },
    author: 'iolo'
  }
};

exports.testusers = {
  user1: {
    _id: 'iolo',
    name: 'IOLO'
  },
  user2: {
    _id: 'shamino',
    name: 'SHAMINO'
  },
  user3: {
    _id: 'dupre',
    name: 'DUPRE'
  }
};
