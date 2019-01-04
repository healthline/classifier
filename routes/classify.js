var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const S3Wrapper = require('../S3Wrapper.js');
var fs = require("fs");
var ClassifierResult = require('./ClassifierResult.js');
var FinalResult = require('./FinalResult.js');

var distinct_k1s = [];
var k1_weights = {};
var k1_primary_thresholds = {};
var k1_secondary_thresholds = {};

router.get('/', async function(req, res, next) {
  req.setTimeout(0);
  var results = await classifyArticle(req);
  if (results.length == 0) {
    res.status(400).send('Failed to get content from S3 for the url: ' + req.query['url']);
  } else {
    var output = {};
    output.article = {};
    output.article.closest = results.closest;
    output.article.primary = results.primary;
    output.article.secondary = results.secondary;
    res.send('' + JSON.stringify(output));
  }
});

async function classifyArticle(req) {
  console.log('path: ' + req.baseUrl);
  console.log('query: ' + JSON.stringify(req.query));
  var finalResult = new FinalResult();
  var closest = [];
  var primary = [];
  var secondary = [];

  var url = req.query['url'];
  console.log('Url to classify: ' + url);
  var jsonObj = await getContentFromS3(url);
  if (Object.keys(jsonObj).length == 0) {
    return [];
  }

  var docVector = parseContentBody(jsonObj);
  var sortedWords = Object.keys(docVector).sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
  var parsed_content = '';
  for (var word of sortedWords) {
    parsed_content += word + ", " + docVector[word] + "\n";
  }
  fs.writeFileSync('parsed_input.txt', parsed_content);
  var normalizedDocVector = normalizeVector(docVector);

  var mysqlObj = new MySQLWrapper();
  var configJson = mysqlObj.getDatabaseConfig('config/database.cf');
  var conn = await mysqlObj.getConnection(configJson);
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

  if (Object.keys(k1_weights).length > 0) {
    await mysqlObj.closeConnect(conn);
  } else {
    var k1_str = '(';
    for (const k1 of k1s) {
      if (k1 == "idf") {
        continue;
      }
      k1_str += '"' + k1 + '",';
    }

    var k1s_len = k1_str.length;
    k1_str = k1_str.substring(0, k1s_len-1);
    k1_str += ')';
    k1_weights = await mysqlObj.getWeights(conn, k1_str);
    await mysqlObj.closeConnect(conn);
  }

  for (const k1 of k1s) {
    if (k1 == "idf") {
      continue;
    }
    var prototypeId = prototypeIds[k1];

    var normalizedWeights = k1_weights[k1];
    var result = computeDotProduct(k1, prototypeId, normalizedDocVector, normalizedWeights);
    var primary_threshold = 0.0;
    if (k1_primary_thresholds.hasOwnProperty(k1)) {
      primary_threshold = k1_primary_thresholds[k1];
    }
    var secondary_threshold = 0.0;
    if (k1_secondary_thresholds.hasOwnProperty(k1)) {
      secondary_threshold = k1_secondary_thresholds[k1];
    }
    result.primary_threshold = primary_threshold;
    result.secondary_threshold = secondary_threshold;
    closest.push(result);

    var primary_candidate = false;
    if (primary_threshold > 0.1 && result.distance > primary_threshold) {//} && primary.length < 5) {
      primary.push(result);
      primary_candidate = true;
    }

    if (secondary_threshold > 0.1 && primary_candidate == false && result.distance > secondary_threshold) {//} && secondary.length < 5) {
      secondary.push(result);
    }
  }

  var sortedClosest = closest.sort((a, b) => (a.distance < b.distance) ? 1 : ((a.distance > b.distance) ? -1 : 0));
  finalResult.closest = sortedClosest.slice(0, 5);
  finalResult.primary = primary;
  finalResult.secondary = secondary;

  return finalResult;
}

function getPrototypeIds() {
  var data = fs.readFileSync('20181119_thresholds_full.xml', 'utf-8');
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

      p1 = lines[i].indexOf('<primary_threshold>');
      p2 = lines[i].indexOf('</primary_threshold>');
      var primary_threshold = parseFloat(lines[i].substring(p1+19, p2));
      k1_primary_thresholds[title] = primary_threshold;

      p1 = lines[i].indexOf('<related_threshold>');
      p2 = lines[i].indexOf('</related_threshold>');
      var secondary_threshold = parseFloat(lines[i].substring(p1+19, p2));
      k1_secondary_thresholds[title] = secondary_threshold;
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
  var clean_body = body.replace(/[\:\-’]/g, "").replace(/[\r\n]+/g, " ").replace(/<\/?[^>]+>/g, " ").toLowerCase().replace(/\s+/g, " ").replace(/[\"\',\.\(\)\[\]\?“”]/g, "");//body.replace(/[“”\[\]’>&\/…‘~',\.()!?\"\':;%*\-]/g, "").toLowerCase();
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
