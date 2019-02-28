var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const redis = require('redis');
const ioredis = require('ioredis');
const deasync = require('deasync');
var fs = require("fs");
const AWS = require('aws-sdk');
var zlib = require('zlib');
var stream = require("stream");

router.get('/', async function(req, res, next) {
  req.setTimeout(0);
  //res.status(400).send('Not implemented yet since we are saving the weights locally on fs.');
  //return;
  /*var infile = fs.openSync('michael_test.txt', 'r');
  var infiles = fs.readFileSync(infile, 'utf-8');
  var toks = infiles.split('\n');
  fs.closeSync(infile);

  var infile2 = fs.openSync('url_imuids_map', 'r');
  var urls = fs.readFileSync(infile2, 'utf-8');
  fs.closeSync(infile2);
  var inObj = JSON.parse(urls);

  for (var tok of toks) {
    var line = tok.trim();
    if (inObj.hasOwnProperty(line)) {
      var imuids = inObj[line];
      console.log(imuids);
    } else {
      console.log('[]');
    }
  }

  res.status(200).send('done');
  return;*/

/*
  var urls = ["/health/lung-cancer-risk-factors","/health/childrens-health-tests"];

  const lambda = new AWS.Lambda({
    region: "us-west-2"
  });

  var indexes = [];
  for (var i=0; i<urls.length; i++) {
    indexes.push(i);
  }

  var outfile = fs.openSync('sh_primary.txt', 'a');

  for (var idx of indexes) {

    const params = {
      FunctionName: "be-lambda-classifier-prod",
      InvocationType: "RequestResponse",
      Payload: JSON.stringify({'url':urls[idx]})
    };

    var t0 = Date.now();
    await new Promise(function(resolve, reject) {
      lambda.invoke(params, function(error, tmp) {
        if (error) {
          resolve(0);
        } else if (tmp) {
          var plJson = tmp['Payload'];
          var jsonObj = JSON.parse(plJson);
          if (Object.keys(jsonObj).indexOf('body') == -1) {
            return finalResult;
          }

          var result = JSON.parse(jsonObj['body']);
          console.log(result['primary']);
          var primaries = '';
          if (typeof result['primary'] != 'undefined') {
            var num = result['primary'].length;
            for (var j=0; j<num; j++) {
              primaries += result['primary'][j]['prototype_title'];
              primaries += "\t";
              primaries += result['primary'][j]['distance'];
              break;
              if (j != num-1){
                primaries += "|";
              }
            }
            if (num == 0) {
              primaries = "-\t0\n";
            }
            fs.writeFileSync(outfile, primaries+'\n');
          } else {
            fs.writeFileSync(outfile, '-\n');
          }
        }
        resolve(1);
      });
    });

    var t1 = Date.now();
    //console.log((t1-t0));
  }
  fs.closeSync(outfile);
  */

  res.status(200).send('Stat complete');

});

module.exports = router;
