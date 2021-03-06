var assert = require('assert');
var async = require('async');
var PouchDB = require('pouchdb');

var metaOperators = {
  $comment: true
, $explain: true
, $hint: true
, $maxScan: true
, $max: true
, $min: true
, $orderby: true
, $returnKey: true
, $showDiskLoc: true
, $snapshot: true
, $count: true
};

var cursorOperators = {
  $limit: 'limit'
, $skip: 'skip'
};

/* There are two ways to instantiate a livedb-mongo wrapper.
 *
 * 1. The simplest way is to just invoke the module and pass in your mongoskin
 * arguments as arguments to the module function. For example:
 *
 * var db = require('livedb-mongo')('localhost:27017/test?auto_reconnect', {safe:true});
 *
 * 2. If you already have a mongoskin instance that you want to use, you can
 * just pass it into livedb-mongo:
 *
 * var skin = require('mongoskin')('localhost:27017/test?auto_reconnect', {safe:true});
 * var db = require('livedb-mongo')(skin);
 */
exports = module.exports = function(options) {
  return new LiveDbPouch(options);
};

function LiveDbPouch(options) {
  this.dbs = {};
  this.options = options;
  this.closed = false;
}

LiveDbPouch.prototype._open = function(dbName) {
  this.dbs[dbName] = this.dbs[dbName] || new PouchDB(dbName, this.options);
  return this.dbs[dbName];
};

LiveDbPouch.prototype.close = function(callback) {
  if (this.closed) return callback('db already closed');
  this.dbs = {}
  if (typeof callback === 'function') {
    callback();
  }
  this.closed = true;
};


// **** Snapshot methods

LiveDbPouch.prototype.getSnapshot = function(dbName, docName, callback) {
  if (this.closed) return callback('db already closed');
  if (/_ops$/.test(dbName)) return callback('Invalid collection name');

  this._open(dbName).get(docName, function(err, doc) {
    if (err) {
      if (err.name == 'not_found') {
        return callback(null, null);
      }
      return callback(err);
    }
    callback(null, castToSnapshot(doc));
  });
};


LiveDbPouch.prototype.bulkGetSnapshot = function(requests, callback) {
  if (this.closed) return callback('db already closed');

  var db = this._open.bind(this);
  var results = {};

  var getSnapshots = function(cName, callback) {
    if (/_ops$/.test(cName)) return callback('Invalid collection name');
    var cResult = results[cName] = {};

    var docNames = requests[cName];
    var query = {
      keys: docNames
    , include_docs: true
    };

    db(cName).allDocs(query, function(err, response) {
      if (err) return callback(err);

      var rows = response.rows;

      for (var i = 0; i < rows.length; i++) {
        if (rows[i].doc != void 0) {
          var snapshot = castToSnapshot(rows[i].doc);
          cResult[snapshot.docName] = snapshot;
        }
      }
      callback();
    });
  };

  async.each(Object.keys(requests), getSnapshots, function(err) {
    callback(err, err ? null : results);
  });
};

LiveDbPouch.prototype.writeSnapshot = function(cName, docName, data, callback) {
  if (this.closed) return callback('db already closed');
  var doc = castToDoc(docName, data);
  /* XXX
     This is nasty. We should be able to round trip _rev through livedb somehow
     but the test suite does a bunch of deepEquals and tries to delete a doc
     by specifying an object with only a version number.

     If livedb allowed for non-integer versions we could map it directly to
     revs but it doesn't. So we deal. It's not ideal.
  */
  var db = this._open(cName);
  db.get(doc._id, function (err, existing) {
    if (existing) doc._rev = existing._rev;
    db.put(doc, callback);
  });
};


// ******* Oplog methods

// Overwrite me if you want to change this behaviour.
LiveDbPouch.prototype.getOplogCollectionName = function(cName) {
  // Using an underscore to make it easier to see whats going in on the shell
  return cName + '_ops';
};

// Get and return the op collection.
LiveDbPouch.prototype._opCollection = function(cName) {
  return this._open(this.getOplogCollectionName(cName));
};

LiveDbPouch.prototype.writeOp = function(cName, docName, opData, callback) {
  assert(opData.v !== null);

  if (this.closed) return callback('db already closed');
  if (/_ops$/.test(cName)) return callback('Invalid collection name');
  var self = this;

  var data = shallowClone(opData);
  data._id = docName + ' v' + opData.v;
  data.name = docName;

  this._opCollection(cName).put(data, function (err) {
    if (err && err.status != 409) return callback(err);
    callback();
  });
};

LiveDbPouch.prototype.getVersion = function(cName, docName, callback) {
  if (this.closed) return callback('db already closed');
  if (/_ops$/.test(cName)) return callback('Invalid collection name');

  var query = {
    startkey: docName + ' v\ufff0'
  , endkey: docName + ' v'
  , limit: 1
  , descending: true
  , include_docs: true
  };
  this._opCollection(cName).allDocs(query, function(err, data) {
    if (err) return callback(err);
    var op = data.rows[0];

    if (op === void 0) {
      callback(null, 0);
    } else {
      callback(null, op.doc.v + 1);
    }
  });
};

LiveDbPouch.prototype.getOps = function(cName, docName, start, end, callback) {
  if (this.closed) return callback('db already closed');
  if (/_ops$/.test(cName)) return callback('Invalid collection name');
  if (start == end) return callback(null, []);
  if (end == null) end = '\ufff0';

  var query = {
    startkey: docName + ' v' + start
  , endkey: docName + ' v' + end
  , include_docs: true
  };

  this._opCollection(cName).allDocs(query, function (err, data) {
    if (err) return callback(err);

    var ops = data.rows.map(function (row) {
      delete row.doc._id;
      delete row.doc._rev;
      delete row.doc.name;
      return row.doc;
    });

    callback(null, ops);
  });
};


// ***** Query methods

// Internal method to actually run the query.
// LiveDbPouch.prototype._query = function(mongo, cName, query, callback) {
//   // For count queries, don't run the find() at all.
//   if (query.$count) {
//     delete query.$count;
//     mongo.collection(cName).count(query.$query || {}, function(err, count) {
//       if (err) return callback(err);

//       // This API is kind of awful. FIXME in livedb.
//       callback(err, {results:[], extra:count});
//     });
//   } else {
//     var cursorMethods = extractCursorMethods(query);

//     mongo.collection(cName).find(query, function(err, cursor) {
//       if (err) return callback(err);

//       for (var i = 0; i < cursorMethods.length; i++) {
//         var item = cursorMethods[i];
//         var method = item[0];
//         var arg = item[1];
//         cursor[method](arg);
//       }

//       cursor.toArray(function(err, results) {
//         results = results && results.map(castToSnapshot);
//         callback(err, results);
//       });
//     });
//   }

// };

// LiveDbPouch.prototype.query = function(livedb, cName, inputQuery, opts, callback) {
//   if (this.closed) return callback('db already closed');
//   if (/_ops$/.test(cName)) return callback('Invalid collection name');

//   // To support livedb <=0.2.8
//   if (typeof opts === 'function') {
//     callback = opts;
//     opts = {};
//   }

//   var query = normalizeQuery(inputQuery);

//   // Use this.mongoPoll if its a polling query.
//   if (opts.mode === 'poll' && this.mongoPoll) {
//     var self = this;
//     // This timeout is a dodgy hack to work around race conditions replicating the
//     // data out to the polling target replica.
//     setTimeout(function() {
//       self._query(self.mongoPoll, cName, query, callback);
//     }, 300);
//   } else {
//     this._query(this.mongo, cName, query, callback);
//   }
// };

// LiveDbPouch.prototype.queryDoc = function(livedb, index, cName, docName, inputQuery, callback) {
//   if (this.closed) return callback('db already closed');
//   if (/_ops$/.test(cName)) return callback('Invalid collection name');
//   var query = normalizeQuery(inputQuery);

//   // Run the query against a particular mongo document by adding an _id filter
//   var queryId = query.$query._id;
//   if (queryId) {
//     delete query.$query._id;
//     query.$query.$and = [{_id: docName}, {_id: queryId}];
//   } else {
//     query.$query._id = docName;
//   }

//   this.mongo.collection(cName).findOne(query, function(err, doc) {
//     callback(err, castToSnapshot(doc));
//   });
// };

// // Test whether an operation will make the document its applied to match the
// // specified query. This function doesn't really have enough information to know
// // in all cases, but if we can determine whether a query matches based on just
// // the operation, it saves doing extra DB calls.
// //
// // currentStatus is true or false depending on whether the query currently
// // matches. return true or false if it knows, or null if the function doesn't
// // have enough information to tell.
// LiveDbPouch.prototype.willOpMakeDocMatchQuery = function(currentStatus, query, op) {
//   return null;
// };

// // Does the query need to be rerun against the database with every edit?
// LiveDbPouch.prototype.queryNeedsPollMode = function(index, query) {
//   return query.hasOwnProperty('$orderby') ||
//     query.hasOwnProperty('$limit') ||
//     query.hasOwnProperty('$skip') ||
//     query.hasOwnProperty('$count');
// };


// // Utility methods

// function extractCursorMethods(query) {
//   var out = [];
//   for (var key in query) {
//     if (cursorOperators[key]) {
//       out.push([cursorOperators[key], query[key]]);
//       delete query[key];
//     }
//   }
//   return out;
// }

// function normalizeQuery(inputQuery) {
//   // Box queries inside of a $query and clone so that we know where to look
//   // for selctors and can modify them without affecting the original object
//   var query;
//   if (inputQuery.$query) {
//     query = shallowClone(inputQuery);
//     query.$query = shallowClone(query.$query);
//   } else {
//     query = {$query: {}};
//     for (var key in inputQuery) {
//       if (metaOperators[key] || cursorOperators[key]) {
//         query[key] = inputQuery[key];
//       } else {
//         query.$query[key] = inputQuery[key];
//       }
//     }
//   }

//   // Deleted documents are kept around so that we can start their version from
//   // the last version if they get recreated. When they are deleted, their type
//   // is set to null, so don't return any documents with a null type.
//   if (!query.$query._type) query.$query._type = {$ne: null};

//   return query;
// }

function castToDoc(docName, data) {
  var doc = (
    typeof data.data === 'object' &&
    data.data !== null &&
    !Array.isArray(data.data)
  ) ?
    shallowClone(data.data) :
    {data: (data.data === void 0) ? null : data.data};
  doc.type = data.type || null;
  doc.m = data.m;
  doc.v = data.v;
  doc._id = docName;
  return doc;
}

function castToSnapshot(doc) {
  if (!doc) return;
  var type = doc.type;
  var v = doc.v;
  var docName = doc._id;
  var data = doc.data;
  var meta = doc.m;
  if (data === void 0) {
    doc = shallowClone(doc);
    delete data.type;
    delete data.v;
    delete data._rev;
    delete data._id;
    return {
      data: doc
    , type: type
    , v: v
    , docName: docName
    , m: meta
    };
  }
  return {
    data: data
  , type: type
  , v: v
  , docName: docName
  , m: meta
  };
}

function shallowClone(object) {
  var out = {};
  for (var key in object) {
    out[key] = object[key];
  }
  return out;
}
