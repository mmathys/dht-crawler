var bencode = require('bencode'),
    dgram = require('dgram'),
    hat = require('hat'),
    _ = require('lodash');

// Put in a function. The returned function won't ever throw an error. This is
// quite useful for malformed messages.
var makeSafe = function (fn, onFuckedUp) {
  return function () {
    try {
      return fn.apply(null, arguments);
    } catch (e) {
      // console.log(e);
      return onFuckedUp;
    }
  };
};

// See https://github.com/bencevans/node-compact2string.
var compact2string = makeSafe(require('compact2string'));

// Necessary formatting for the protocols we are using.
var transactionIdToBuffer = makeSafe(function (transactionId) {
  var buf = new Buffer(2);
  buf.writeUInt16BE(transactionId, 0);
  return buf;
});

// Necessary formatting for the protocols we are using.
var idToBuffer = makeSafe(function (id) {
  return new Buffer(id, 'hex');
});

var decode = makeSafe(bencode.decode, {}),
    encode = makeSafe(bencode.encode, {});

var ROUTERS = [
  'router.bittorrent.com:6881',
  'router.utorrent.com:6881',
  'dht.transmissionbt.com:6881'
  ],
  BOOTSTRAP_NODES = ROUTERS.slice();

var nodeID = hat(160),
    port = process.env.UDP_PORT || 6881,
    socket = dgram.createSocket('udp4');

// Update our id once in a while, since we are esentially spamming the DHT
// network and this might prevent other nodes from blocking us.
setInterval(function () {
  nodeID = hat(160);
}, 10000);

// Key: infoHash; Value: Object representing the current results of this crawl
// job (peers and nodes set using object).
var jobs = {};

// Key: transactionId; Value: infoHash
var transactions = {};

// This function will be invoked as soon as a node/peer sends a message. It does
// a lot of formatting for the protocols.
socket.on('message', function (msg, rinfo) {
  // Add to out bootstrap nodes. This means, we'll be able to query the DHT
  // network in a more direct way in the future.
  BOOTSTRAP_NODES.push(rinfo.address + ':' + rinfo.port);

  if (BOOTSTRAP_NODES.length > 100) {
    BOOTSTRAP_NODES.shift();
  }

  // console.log('Received message from ' + rinfo.address);
  msg = decode(msg);
  var transactionId = Buffer.isBuffer(msg.t) && msg.t.length === 2 && msg.t.readUInt16BE(0);
  var infoHash = transactions[transactionId];
  if (transactionId === false || infoHash === undefined || jobs[infoHash] === undefined) {
    return;
  }
  delete transactions[transactionId];
  if (msg.r && msg.r.values) {
    _.each(msg.r.values, function (peer) {
      peer = compact2string(peer);
      if (peer && !jobs[infoHash].peers[peer]) {
        //console.log('Found new peer ' + peer + ' for ' + infoHash);
        jobs[infoHash].peers[peer] = true;
        jobs[infoHash].queue.push(peer);
      }
    });
  }
  if (msg.r && msg.r.nodes && Buffer.isBuffer(msg.r.nodes)) {
    for (var i = 0; i < msg.r.nodes.length; i += 26) {
      var node = compact2string(msg.r.nodes.slice(i + 20, i + 26));
      if (node && !jobs[infoHash].peers[node]) {
        //console.log('Found new node ' + node + ' for ' + infoHash);
        jobs[infoHash].nodes[node] = true;
        jobs[infoHash].queue.push(node);
      }
    }
  }
});

// Sends the get_peers request to a node.
var getPeers = function (infoHash, addr) {
  // console.log('Sending get_peers to ' + addr + ' for ' + infoHash);
  addr = addr.split(':');
  var ip = addr[0],
      port = parseInt(addr[1]);
  if (port <= 0 || port >= 65536) {
    return;
  }
  // var transactionId = _.random(Math.pow(2, 16));
  var transactionId = _.random(Math.pow(2, 12));
  transactions[transactionId] = infoHash;
  setTimeout(function () {
    // Delete transaction after five seconds, if we didn't get a response.
    // This is extremely important. Otherwise we might get a memory leak.
    delete transactions[transactionId];
  }, 5000);
  var message = encode({
    t: transactionIdToBuffer(transactionId),
    y: 'q',
    q: 'get_peers',
    a: {
      id: idToBuffer(nodeID),
      info_hash: idToBuffer(infoHash)
    }
  });
  socket.send(message, 0, message.length, port, ip);
};

var crawl = function (infoHash, ttl, callback, benchmark) {
  console.log('[START] Crawling ' + infoHash);

  if (jobs[infoHash]) {
    return callback(new Error('Crawljob already in progress'));
  }

  var queue = [];

  // Packages might get lost. This sends each get_peers request multiple times.
  // Routers provided by BitTorrent, Inc. are sometimes down. This way we
  // ensure that we correctly enter the DHT network. Otherwise, we might not get
  // a single peer/ node.
  _.times(5, function () {
    queue = queue.concat(BOOTSTRAP_NODES);
  });

  jobs[infoHash] = {
    peers: {},
    nodes: {},
    queue: queue
  };

  var bench = null;
  if(benchmark){
    bench = setInterval(function(){
      console.log(_.keys(jobs[infoHash].peers).length+" "+_.keys(jobs[infoHash].nodes).length);
    }, 10*1000);
  }
  setTimeout(function () {

    // Clear interval. Don't mess up the event loop!
    clearInterval(crawling);

    if(benchmark){
      clearInterval(bench);
    }

    var peers = _.keys(jobs[infoHash].peers);
    var nodes = _.keys(jobs[infoHash].nodes);

    console.log('[DONE]  Done Crawling '+infoHash+'. \n        Found ' + peers.length + ' peers and ' + nodes.length + ' nodes.');

    // Delete the job! This is for future crawling and in order to prevent
    // memory leaks very important!
    delete jobs[infoHash];

    callback(null, {
      peers: peers,
      nodes: nodes
    });

    // Time in ms for a job to live.
  }, ttl);

  // We limit the number of outgoing UDP requests to 1000 packages per second.
  // We clear this interval in the setTimeout function above.
  var crawling = setInterval(function () {
    if (jobs[infoHash].queue.length > 0) {
      getPeers(infoHash, jobs[infoHash].queue.shift());
    }
  }, 1);
};

module.exports = exports = crawl;
module.exports.init = function (callback) {
  socket.bind(port, callback);
};



// Example usage:
// var crawl = require('./crawl');
// crawl.init(function () {
//   crawl('8CA378DBC8F62E04DF4A4A0114B66018666C17CD', function (err, result) {
//     console.log(result);
//     process.exit(1);
//   });
// });
