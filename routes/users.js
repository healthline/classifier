var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const S3Wrapper = require('../S3Wrapper.js');
var fs = require("fs");
var ClassifierResult = require('./ClassifierResult.js');

var distinct_k1s = [];
var k1_weights = {};

/* GET users listing. */
router.get('/', async function(req, res, next) {
  req.setTimeout(0);
  var results = await classifyArticle(req)
  res.send(JSON.stringify(results));
});

/*function storeK1sToFS(filename, k1s) {
  var data = "";
  for (var i=0; i<k1s.length; i++) {
    data += k1s[i] + "\n";
  }
  fs.writeFileSync(filename, data);
}

function retrieveK1sFromFS(filename) {
  var k1s = [];
  var data = fs.readFileSync(filename, 'utf-8');
  var lines = data.split("\n");
  for (var i=0; i<lines.length; i++) {
    var line = lines[i];
    if (line.length > 0) {
      k1s.push(line);
    }
  }
  return k1s;
}

function storeWeightsToFS(filename, weights) {
  var data = "";
  var keys = Object.keys(weights);
  for (var i=0; i<keys.length; i++) {
    data += keys[i] + "," + weights[keys[i]] + "\n";
  }
  fs.writeFileSync(filename, data);
}

function retrieveWeightsFromFS(filename) {
  var weights = {};
  var data = fs.readFileSync(filename, 'utf-8');
  var lines = data.split("\n");
  for (var i=0; i<lines.length; i++) {
    if (lines[i].length > 0) {
      var toks = lines[i].split(",");
      weights[toks[0]] = parseFloat(toks[1]);
    }
  }
  return weights;
}*/

async function classifyArticle(req) {
  console.log('path: ' + req.baseUrl);
  console.log('query: ' + JSON.stringify(req.query));
  var classifierResults = [];

  var url = req.query['url'];
  console.log('Url to classify: ' + url);
  var t1 = new Date();
  var jsonObj = await getContentFromS3(url);
  if (Object.keys(jsonObj).length == 0) {
    res.status(400).send('Failed to get content from S3 for the url: ' + url);
    return;
  } else {
    t2 = new Date();
    var delta = parseInt((t2-t1)/1000);
    console.log("S3 time: " + delta);
    var docVector = parseContentBody(jsonObj);
    var normalizedDocVector = normalizeVector(docVector);

    var mysqlObj = new MySQLWrapper();
    var config = {'host':'172.29.155.248', 'user':'hl_dev', 'password':'hl_dev', 'database':'hl_dev'};
    var conn = await mysqlObj.getConnection(config);
    if (conn == null) {
      res.status(400).send('Failed to connect to the database');
      return;
    }

    var k1s = distinct_k1s;
    if (distinct_k1s.length == 0) {
      k1s = await mysqlObj.getDistinctK1s(conn);
      distinct_k1s = k1s;
    }
    var prototypeIds = getPrototypeIds();
    var i = 0;
    for (const k1 of k1s) {
      i += 1;
      if (k1 == "idf") {
        continue;
      }
      console.log(k1);
      var prototypeId = prototypeIds[k1];
      var weights = {};
      if (k1_weights.hasOwnProperty(k1)) {
        weights = k1_weights[k1];
      } else {
        console.log("Retrieving weights for " + k1);
        weights = await mysqlObj.getWeights(conn, k1);
        k1_weights[k1] = weights;
        var keys1 = Object.keys(weights);
        if (keys1.length == 0) {
          console.log("db error");
        }
      }

      var normalizedWeights = weights;
      var result = computeDotProduct(k1, prototypeId, normalizedDocVector, normalizedWeights);
      classifierResults.push(result);
      if (i%50 == 1) {
        console.log(i + " : " + new Date());
      }
    }
    console.log(i + " : " + new Date());
    await mysqlObj.closeConnect(conn);
  }
  var sortedResults = classifierResults.sort((a, b) => (a.distance < b.distance) ? 1 : ((a.distance > b.distance) ? -1 : 0));
  return sortedResults.slice(0, 5);
}

function getPrototypeIds() {
  var data = fs.readFileSync('20180803_thresholds_full.xml', 'utf-8');
  var lines = data.split("\n");
  var prototypeIds = {};
  var parseMode = 0;
  for (var i=0; i<lines.length; i++) {
    if (!parseMode && lines[i].search("<vocab_id>3</vocab_id>") == -1) {
      continue;
    } else if (lines[i].search("<vocab_id>3</vocab_id>") > -1) {
      parseMode = 1;
      continue;
    } else if (lines[i].search("<prototypes>") > -1) {
      continue;
    } else if (lines[i].search("</prototypes>") > -1 || lines[i].search("</vocab>") > -1) {
      break;
    } else {
      var p1 = lines[i].indexOf('<prototype_id>');
      var p2 = lines[i].indexOf('</prototype_id>');
      var id = parseInt(lines[i].substring(p1+14, p2));
      p1 = lines[i].indexOf('<title>');
      p2 = lines[i].indexOf('</title>');
      var title = lines[i].substring(p1+7, p2);
      prototypeIds[title] = id;
    }
  }
  return prototypeIds;
}

function computeDotProduct(k1, prototypeId, normalizedDocVector, normalizedWeights) {
  var classifierResult = new ClassifierResult();
  classifierResult.prototype_title = k1;
  classifierResult.prototype_id = prototypeId;
  var dotProduct = 0.0;
  var words = Object.keys(normalizedDocVector);
  for (var i=0; i<words.length; i++) {
    var word = words[i];
    if (normalizedWeights.hasOwnProperty(word)) {
      dotProduct += (normalizedDocVector[word] * normalizedWeights[word]);
    }
  }
  classifierResult.distance = dotProduct;
  return classifierResult;
}

function normalizeVector(vector) {
  var normalizedVector = {};
  var tot = 0;
  var keys = Object.keys(vector);
  for (var i=0; i<keys.length; i++) {
    var word_count = vector[keys[i]];
    tot += Math.pow(word_count, 2);
  }
  for (var i=0; i<keys.length; i++) {
    var key = keys[i];
    normalizedVector[key] = vector[key]/Math.sqrt(tot);
  }
  return normalizedVector;
}

function removeStopWords(clean_body) {
  var body_words = clean_body.split(" ");
  var data = fs.readFileSync('stopwords.txt', 'utf-8');
  var stop_words = data.split("\n");
  var new_body_words = [];
  for (var i=0; i<body_words.length; i++) {
    if (body_words[i].length < 2) {
      continue;
    }
    var pos = stop_words.indexOf(body_words[i]);
    if (pos == -1) {
      new_body_words.push(body_words[i]);
    }
  }
  return new_body_words;
}

function parseContentBody(jsonObj) {
  var docVector = {};
  var body = jsonObj["body"];
  var clean_body = body.replace(/[\r\n]+/g, " ").replace(/<\/?[^>]+>/g, "").toLowerCase().replace(/\s+/g, " ").replace(/[\"\',\.\(\)\[\]\?“”]/g, "");//body.replace(/[“”\[\]’>&\/…‘~',\.()!?\"\':;%*\-]/g, "").toLowerCase();
  var body_no_stop_words = removeStopWords(clean_body);
  var body_words = body_no_stop_words;
  for (var i=0; i<body_words.length; i++) {
    var tmp1 = body_words[i];
    var keys = Object.keys(docVector);
    if (keys.indexOf(tmp1) > -1) {
      docVector[tmp1] += 1;
    } else {
      docVector[tmp1] = 1;
    }
    if (i < body_words.length-1) {
      var tmp2 = body_words[i+1];
      var bigram = tmp1+'_'+tmp2;
      if (keys.indexOf(bigram) > -1) {
        docVector[bigram] += 1;
      } else {
        docVector[bigram] = 1;
      }
    }
  }
  return docVector;
}

async function getContentFromS3(url) {
  var jsonObj = {};
  try {
    var s3Wrapper = new S3Wrapper();
    var bodyObj = await s3Wrapper.getS3Data(url);
    var jsonStr = bodyObj.Body.toString();
    jsonObj = JSON.parse(jsonStr);
  } catch(ex) {
    console.log(ex);
  }
  return jsonObj;
}

module.exports = router;
