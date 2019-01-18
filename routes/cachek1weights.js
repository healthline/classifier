var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const redis = require('redis');
const ioredis = require('ioredis');
const deasync = require('deasync');

router.get('/', async function(req, res, next) {
  req.setTimeout(0);
  var results = await cacheWeights(req);
  if (results.length == 0) {
    res.status(400).send('Failed to store k1 weights');
  } else {
    res.json(results);
  }
});

async function cacheWeights(req) {
  var result = {};

  var mysqlObj = new MySQLWrapper();
  var config_path = 'config/database.cf.dev';
  if (__ENV__ == 'qa' || __ENV__ == 'stage' || __ENV__ == 'prod') {
    config_path = 'config/database.cf.' + __ENV__;
  }
  var configJson = mysqlObj.getDatabaseConfig(config_path);
  var conn = await mysqlObj.getConnection(configJson);
  if (conn == null) {
    res.status(400).send('Failed to connect to the database');
    return;
  }

  var k1s = await mysqlObj.getDistinctK1s(conn);

  var k1_str = '(';
  for (const k1 of k1s) {
    k1_str += '"' + k1 + '",';
  }

  var k1s_len = k1_str.length;
  k1_str = k1_str.substring(0, k1s_len-1);
  k1_str += ')';
  var k1_weights = await mysqlObj.getWeights(conn, k1_str);
  await mysqlObj.closeConnect(conn);

  for (const k1 of k1s) {
    var client = redis.createClient(6379, '172.29.152.15');
    var tmp = k1_weights[k1];
    var k1_terms = Object.keys(tmp);
    var cnt = 0;
    for (term of k1_terms) {
      try {
        client.hset('new-'+k1, term, tmp[term], function(err, res) {
          if (err) {
            console.log(err);
          }
          cnt++;
        });
      } catch(ex) {
        console.log(ex);
        cnt++;
      }
    }
    deasync.loopWhile(function(){return (k1_terms.length > cnt);});
    client.quit();
  }

  return {'num_k1s_cached:' + k1s.length};
}

module.exports = router;
