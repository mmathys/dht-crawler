var crawler = require('./crawler');
var db = require('./rethinkdb');
var parseMagnetURI = require('magnet-uri');
var _ = require('lodash');

var opt = require('node-getopt').create([
  ['a' , 'add=ARG'             , 'add magnet URI to crawler'],
  ['l' , 'list'                , 'list the magnets which we want to parse'],
  ['r' , 'remove=ARG'          , 'remove the magnet with its infoHash'],
  ['t' , 'time=ARG'            , 'time for each crawl in s'],
  ['p' , 'parallel=ARG'        , 'parallel crawls to perform'],
  ['i' , 'interval=ARG'        , 'interval between the crawls in s'],
  ['o' , 'once'                , 'once: don\'t repeat crawling, just crawl once.'],
  ['b' , 'benchmark'           , 'benchmark: print found peers and nodes every 10s'],
  ['e' , 'export=ARG'          , 'export all data in the database to CSV with path ARG'],
  ['h' , 'help'                , 'display this help']
  ])              // create Getopt instance
.bindHelp()     // bind option 'help' to default action
.parseSystem(); // parse command line
db.setup();
if(opt.options.add){
  var parsedMagnetURI = {};
  try {
    parsedMagnetURI = parseMagnetURI(opt.options.add);
  } catch (e) {  }
  // Empty parsed object -> invalid magnet link!
  if (_.isEmpty(parsedMagnetURI)) {
    console.log('Invalid Magnet URI');
    process.exit(1);
  }

  if(!parsedMagnetURI.name) parsedMagnetURI.name = "torrent";
  db.insertMagnet(parsedMagnetURI.name, parsedMagnetURI.infoHash, true, function(err, success){
    if(err){
      console.log("Failed to insert Magnet");
      process.exit(1);
    }else{
      console.log("Added "+parsedMagnetURI.name+" "+parsedMagnetURI.infoHash);
      process.exit(0);
    }
  });
} else if(opt.options.remove){
  var magnet = opt.options.remove.toLowerCase();
  db.delete(magnet, function(err, res){
    if(err) throw err;
    console.log("Removed: "+res.deleted);
    process.exit(0);
  });
} else if(opt.options.list){
  db.getMagnets(function(err, res){
    var c = 0;
    _.forEach(res, function(obj){
      console.log((++c)+" Name: "+obj.name+"\n  Info hash: "+obj.infoHash);
    });
    if(err) throw err;
    process.exit(0);
  })

} else if(opt.options.export){
  db.export(opt.options.export, function(err, ret){
    if(err) throw err;
    else console.log("Succesfully exported csv to "+opt.options.export);
    process.exit(0);
  });
} else {
  if(!opt.options.time)
    opt.options.time = 20;
  if(!opt.options.parallel)
    opt.options.parallel = 4;
  if(!opt.options.once)
    opt.options.once = false;
  if(!opt.options.benchmark)
    opt.options.benchmark = false;


  db.getMagnets(function(err, res){
    var magnets = res.length;
    var minInterval = Math.ceil(res.length/opt.options.parallel)*opt.options.time;
    if(!opt.options.interval && !opt.options.once){
      opt.options.interval = minInterval;
    }else if(opt.options.interval<minInterval && !opt.options.once){
      console.log("Error: Crawls overlap. With this setting, a new crawl queue begins before the old one has finished:");
      console.log("We have "+magnets+" magnets, "+opt.options.parallel+" will be crawled parallel, before we move to the next ones in the queue");
      console.log("That means we have "+Math.ceil(res.length/opt.options.parallel)+" set * "+opt.options.time+"s, this is greater than your interval "+opt.options.interval+"s");
      console.log("Exiting.");
      process.exit(1);
    }
    crawler.start(opt.options.time*1000, opt.options.parallel, opt.options.interval*1000, opt.options.once, opt.options.benchmark);
  });
  //if no interval is given, set the minimum interval.

}
