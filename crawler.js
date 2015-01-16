var crawl = require('./trrnts'),
    _ = require('lodash'),
    db = require('./rethinkdb'),
    geoip = require('geoip-lite');


crawler = {};
var interval = null;
db.setup();

crawler.start = function(timePerCrawl, parallelCrawls, intervalTime, once, benchmark) {

  console.log("Time Per Crawl Unit: "+timePerCrawl/1000+" seconds");
  console.log("Max. Parallel Crawls: "+parallelCrawls);
  console.log("Repeat interval: "+intervalTime/1000+" seconds ");

  if(!timePerCrawl)
    timePerCrawl = 20*1000;
  else
    timePerCrawl = parseInt(timePerCrawl);
  if(!intervalTime)
    intervalTime = 60*1000;
  else
    intervalTime = parseInt(intervalTime);
  if(!parallelCrawls)
    parallelCrawls = 4;
  else
    parallelCrawls = parseInt(parallelCrawls);
  if(!benchmark)
    benchmark = false;

  crawl.init(function () {
    crawler.onCrawled = function (infoHash) {
      return function (err, result) {
        if (err) {
          return;
        }
        var time = _.now();

        var peers = [];
        _.each(result.peers, function (peer) {
          var ip = peer.split(':')[0];
          var geo = geoip.lookup(ip) || {};
          geo.country = geo.country || '?';
          geo.region = geo.region || '?';
          geo.city = geo.city || '?';
          geo.ll = geo.ll || ['?', '?'];
          geo.ll = geo.ll.join(',');
          peers.push({
            ip: ip,
            country: geo.country,
            region: geo.region,
            city: geo.city,
            ll: geo.ll
          });
        });

        db.insertCrawl(infoHash, time, peers, function(err){
          if(err) throw err;
        });
      };
    };

    crawler.next = function () {
      //4 Torrents gleichzeitig crawlen.
      var c = 0;
      db.nextCrawl(parallelCrawls, function(err, arr){
        if(err) throw err;
        _.each(arr, function(obj){
          if(obj.infoHash){
            c++;
            crawl(obj.infoHash, timePerCrawl, crawler.onCrawled(obj.infoHash), benchmark);
            if(c == parallelCrawls){
              // set the next crawl
              setTimeout(next(timePerCrawl), timePerCrawl);
            }
          }else{
          }
        })
      });
    };


    crawler.beginQueue = function(){
      db.newQueue(function(err, res){
        if(err) throw err;
        crawler.next(timePerCrawl);
      });

    };

    crawler.clearInterval = function(){
      clearInterval(interval);
    }


    //Actually start it
    crawler.beginQueue();
    if(!once){
      interval = setInterval(crawler.beginQueue, intervalTime);
    }
  });
};



module.exports = exports = crawler;
