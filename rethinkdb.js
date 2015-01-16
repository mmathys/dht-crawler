// A fork of the [node.js chat app](https://github.com/eiriksm/chat-test-2k)
// by [@orkj](https://twitter.com/orkj) using socket.io, rethinkdb, passport and bcrypt on an express app.
//
// See the [GitHub README](https://github.com/rethinkdb/rethinkdb-example-nodejs-chat/blob/master/README.md)
// for details of the complete stack, installation, and running the app.

var r = require('rethinkdb')
  , util = require('util')
  , assert = require('assert')
  , logdebug = require('debug')('rdb:debug')
  , logerror = require('debug')('rdb:error');


// #### Connection details

// RethinkDB database settings. Defaults can be overridden using environment variables.
var dbConfig = {
  host: process.env.RDB_HOST || 'localhost',
  port: parseInt(process.env.RDB_PORT) || 28015,
  db  : process.env.RDB_DB || 'dht_crawler',
  tables: {
    'crawls': 'id',
    'crawl_queue': 'id',
    'magnets': 'id'
  }
};

module.exports.setup = function() {
  r.connect({host: dbConfig.host, port: dbConfig.port }, function (err, connection) {
    assert.ok(err === null, err);
    r.dbCreate(dbConfig.db).run(connection, function(err, result) {
      if(err) {
        logdebug("[DEBUG] RethinkDB database '%s' already exists (%s:%s)\n%s", dbConfig.db, err.name, err.msg, err.message);
      }
      else {
        logdebug("[INFO ] RethinkDB database '%s' created", dbConfig.db);
      }

      for(var tbl in dbConfig.tables) {
        (function (tableName) {
          r.db(dbConfig.db).tableCreate(tableName, {primaryKey: dbConfig.tables[tbl]}).run(connection, function(err, result) {
            if(err) {
              logdebug("[DEBUG] RethinkDB table '%s' already exists (%s:%s)\n%s", tableName, err.name, err.msg, err.message);
            }
            else {
              logdebug("[INFO ] RethinkDB table '%s' created", tableName);
            }
          });
        })(tbl);
      }
    });
  });
};


/**
 * Get all magnets in the table
 *
 * @param {Function} callback
 *    callback invoked after collecting all the results
 *
 * @returns {Array}
 */
module.exports.getMagnets = function (callback) {
  onConnect(function (err, connection) {
      r.db(dbConfig['db'])
      .table('magnets')
      .run(connection, function(err, cursor){
        if(err) throw err;
        cursor.toArray(function(err, result) {
          if(err) throw err;
          callback(err, result);
        })
      });
    });
};

/**
 * Insert a new magnet to crawl.
 *
 * RethinkDB will use the primary key index to fetch the result.
 *
 * @param {String} name
 *    The description of the torrent. optional
 *
 * @param {String} infoHash
 *    The infoHash of the torrent.
 *
 * @param {Boolean} shouldCrawl
 *    Whether the torrent should be crawled.
 *
 * @param {Function} callback
 *    callback invoked after collecting all the results
 *
 * @returns {Object} the user if found, `null` otherwise
 */
module.exports.insertMagnet = function (name, infoHash, shouldCrawl, callback) {
  onConnect(function (err, connection) {
      r.db(dbConfig['db'])
      .table('magnets')
      .insert(
        {
          name: name,
          infoHash: infoHash,
          shouldCrawl: shouldCrawl
        }).run(connection, function(err, result){
        if(err) throw err;
        callback(null, []);
      });
    });
}

/**
 * Insert a new crawl result
 *
 * @param {String} infoHash
 *    The infoHash of the torrent.
 *
 * @param {Number} time
 *    The time in ms when the crawl was finished
 *
 * @param {Array} peers
 *    The peers. includes ip addr and geoloc: ip, country, region, city, ll
 *
 * @param {Function} callback
 *    callback invoked after collecting all the results
 *
 * @returns {Array} if success, if not null.
 */
module.exports.insertCrawl = function (infoHash, time, peers, callback) {
  onConnect(function (err, connection) {
    r.db(dbConfig['db']).table('crawls').insert([
      {
        time: time,
        infoHash: infoHash,
        peers: peers
      }
    ]).run(connection, function(err, result){
      if(err) throw err;
      callback(null, []);
    });
  });
};

module.exports.newQueue = function (callback) {
  onConnect(function (err, connection) {
    r.db(dbConfig['db'])
    .table('crawl_queue')
    .delete()
    .run(connection, function(err, res){
      if(err) throw err;
      r.db(dbConfig['db'])
      .table('crawl_queue')
      .insert(
        r.db(dbConfig['db'])
        .table('magnets')
        .filter(r.row('shouldCrawl').eq(true))
      )
      .run(connection, function(err, res){
        //inserted
        if(err) throw err;
        callback(err, res);
        r.db(dbConfig['db']).table("crawl_queue")
        .filter(r.row('shouldCrawl').eq(true))
        .count()
        .run(connection, function(err, res){
          if(err) throw err;
          //console.log("Beginning crawl queue with "+res+" magnets.")
        });
      })
    });
  });
};

module.exports.nextCrawl = function(magnetCount, callback){
  onConnect(function (err, connection) {
    r.db(dbConfig['db']).table("crawl_queue")
    .filter({'shouldCrawl':true})
    .limit(magnetCount)
    .run(connection, function(err, res){
      if(err) throw err;
      res.toArray(function(err2, res2){
        var ret = res2;
        r.db(dbConfig['db']).table("crawl_queue")
        .filter({'shouldCrawl':true})
        .update({'shouldCrawl':false})
        .run(connection, function(err3, res3){
          callback(err, ret)
        });
      });
    });
  });
};

module.exports.delete = function(infoHash, callback){
  onConnect(function (err, connection) {
    r.db(dbConfig['db']).table("magnets")
    .filter(r.row('infoHash').eq(infoHash))
    .delete()
    .run(connection, function(err, res){
      if(err) throw err;
      callback(err, res);
    });
  });
};

module.exports.export = function(callback){
  onConnect(function (err, connection) {
    callback(err, connection, r.db(dbConfig['db']));
  });
};

// #### Helper functions

/**
 * A wrapper function for the RethinkDB API `r.connect`
 * to keep the configuration details in a single function
 * and fail fast in case of a connection error.
 */
function onConnect(callback) {
  r.connect({host: dbConfig.host, port: dbConfig.port }, function(err, connection) {
    assert.ok(err === null, err);
    connection['_id'] = Math.floor(Math.random()*10001);
    callback(err, connection);
  });
}

// #### Connection management
//
// This application uses a new connection for each query needed to serve
// a user request. In case generating the response would require multiple
// queries, the same connection should be used for all queries.
//
// Example:
//
//     onConnect(function (err, connection)) {
//         if(err) { return callback(err); }
//
//         query1.run(connection, callback);
//         query2.run(connection, callback);
//     }
//
