var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const S3Wrapper = require('../S3Wrapper.js');
var fs = require("fs");
var SolrNode = require('solr-node');

async function processSolrDocs(client, imuids) {
  var testimuid = '(';
  var len = imuids.length;
  console.log('num imuids: ' + len);
  var max = Math.min(len, 500);
  for (var i=0; i<max; i++) {
    testimuid = testimuid + imuids[i];
    if (i < max-1) {
      testimuid += ' OR ';
    }
  }
  testimuid += ')';

  var result = await new Promise(function(resolve, reject) {
    var strQuery = client.query().q({sourcename:'hlcms', lang:'en', imuids:testimuid}).fl('cmsurl').start(0).rows(100000);
    //.fl('sourcename, contenttype, cmsurl, url, subtype, title, nid, imuids');
    console.log(strQuery['params'][0].substring(0, 100));
    client.search(strQuery, function (err, result) {
       if (err) {
          console.log(err);
          return 'error';
       }
       resolve(result);
     });
  });

  var resp = await result.response;
  return resp;
}

router.get('/', async function(req, res, next) {
  req.setTimeout(0);
  var mysqlObj = new MySQLWrapper();
  var configJson = mysqlObj.getDatabaseConfig('config/database.cf');
  var conn = await mysqlObj.getConnection(configJson);
  if (conn == null) {
    res.status(400).send('Failed to connect to the database');
    return;
  }

  var k1s = await mysqlObj.getDistinctK1s(conn);

  // Create client
  var client = new SolrNode({
      host: '172.29.27.21', //solr-master.eng.healthline.com
      port:80,
      path:'/solr',
      protocol: 'http',
      debugLevel: 'ERROR'
  });
  var resp = {};
  for (const k1 of k1s) {
    if (k1 == "idf") {
      continue;
    }
    var imuids = await mysqlObj.getImuidsForK1(conn, k1);
    resp = await processSolrDocs(client, imuids);
  }
  await mysqlObj.closeConnect(conn);

  console.log('Response:', JSON.stringify(resp)); // only last one for now
  res.send(JSON.stringify(resp));
});

module.exports = router;
