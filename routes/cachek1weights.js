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
  res.status(400).send('Not implemented yet since we are saving the weights locally on fs.');
  return;

  /*
  const params = {
    FunctionName: "be-lambda-classify-qa",
    InvocationType: "RequestResponse",
    Payload: JSON.stringify({'url':'/health/stomach-ulcer', 'chunk_size': 100, 'memory':true})
  };

  const lambda = new AWS.Lambda({
    region: "us-west-2"
  });

  var indexes = [];
  for (var i=1; i<=100; i++) {
    indexes.push(i);
  }

  for (var idx of indexes) {
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
        }
        resolve(1);
      });
    });

    var t1 = Date.now();
    console.log((t1-t0));
  }

  res.status(200).send('Stat complete');
  */
});

module.exports = router;
