var csv = require('fast-csv'),
db = require('./rethinkdb'),
_ = require('lodash');

db.export(function(err, connection, database){

  //your export code here


  //example: see how from which countries the peers came from
  database
  .table("crawls")
  .limit(1)
  .run(connection, function(err, res){
    if(err) throw err;

    res.toArray(function(err, crawls){
      // The following method accept an array of values to be written,
      // however each value must be an array of arrays or objects.
      var crawlObj = crawls[0];
      var peersPerCountry = {};

      _.each(crawlObj.peers, function(peer){
        if(!peersPerCountry[peer.country]){
          peersPerCountry[peer.country] = 1;
        }else{
          peersPerCountry[peer.country]++;
        }
      });


      //tweak a bit so we have arrays in the data array, just like a table
      var data = [];
      var countries = _.keys(peersPerCountry);

      var c = 0;
      _.each(peersPerCountry, function(country){
        data.push([countries[++c], country]);
      });

      console.log(peersPerCountry);
      //Export to CSV
      csv
       .writeToPath("export.csv", data, {headers: true})
       .on("finish", function(){
           console.log("done!");
           process.exit(0);
       });
    });
  });


});
