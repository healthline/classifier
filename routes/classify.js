var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const S3Wrapper = require('../S3Wrapper.js');
var fs = require("fs");
var ClassifierResult = require('./ClassifierResult.js');
var FinalResult = require('./FinalResult.js');
//var Redis = require('ioredis');
//const redis = require('redis');

var distinct_k1s = [];
var k1_primary_thresholds = {};
var k1_related_thresholds = {};

router.get('/', async function(req, res, next) {
  req.setTimeout(0);
  var results = await classifyArticle(req, 'get');
  if (results.length == 0) {
    res.status(400).send('Failed to get content from S3 for the url: ' + req.query['url']);
  } else {
    var output = {};
    output.article = {};
    output.article.closest = results.closest;
    output.article.primary = results.primary;
    output.article.related = results.related;
    res.json(output);
  }
});

router.post('/', async function (req, res) {
  if (!(req.body).hasOwnProperty('body')) {
    res.status(400).send('data must be in format {body : value}');
    return;
  }
  var data = req.body['body'];
  if (typeof data == 'undefined' || data == null || data.length < 100) {
    res.status(400).send('Insufficient data to classify');
    return;
  }

  var results = await classifyArticle(req.body, 'post');
  if (results.length == 0) {
    res.status(400).send('Failed to get results for the content');
  } else {
    var output = {};
    output.article = {};
    output.article.closest = results.closest;
    output.article.primary = results.primary;
    output.article.related = results.related;
    res.json(output);
  }
});

async function classifyArticle(req, methodType) {
  console.log('path: ' + req.baseUrl);
  console.log('query: ' + JSON.stringify(req.query));
  var finalResult = new FinalResult();
  var closest = [];
  var primary = [];
  var related = [];
  var jsonObj;

  if (methodType == 'post') {
    jsonObj = req;
  } else {
    var url = req.query['url'];
    console.log('Url to classify: ' + url);
    var jsonObj = await getContentFromS3(url);
    if (Object.keys(jsonObj).length == 0) {
      return [];
    }
  }

  var docVector = parseContentBody(jsonObj);
  var sortedWords = Object.keys(docVector).sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
  var normalizedDocVector = normalizeVector(docVector);
  var parsed_content = '';
  for (var word of sortedWords) {
    //parsed_content += "\"" + word + "\" : " + normalizedDocVector[word] + ", ";
    parsed_content += word + "\n";
  }
  fs.writeFileSync('parsed_input.txt', parsed_content);

  var mysqlObj = new MySQLWrapper();
  var config_path = 'config/database.cf.dev';
  //if (__ENV__ == 'qa' || __ENV__ == 'stage' || __ENV__ == 'prod') {
    //config_path = 'config/database.cf.' + __ENV__;
  //}
  var configJson = mysqlObj.getDatabaseConfig(config_path);
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

  var k1_str = '(';
  for (const k1 of k1s) {
    k1_str += '"' + k1 + '",';
  }

  var k1s_len = k1_str.length;
  k1_str = k1_str.substring(0, k1s_len-1);
  k1_str += ')';
  var k1_weights = await mysqlObj.getWeights(conn, k1_str);

  var db_keys = Object.keys(k1_weights);
  db_keys = db_keys.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
  var num_keys = db_keys.length;
  var file_num = 0;
  var weights_file = fs.openSync('db_weights_0', 'w');
  var cur_map = {};
  for (var k=0; k<num_keys; k++) {
    if (k > 0 && k%100 == 0) {
      file_num++;
      fs.writeFileSync(weights_file, JSON.stringify(cur_map));
      cur_map = {};
      cur_map[db_keys[k]] = k1_weights[db_keys[k]];
      fs.closeSync(weights_file);
      weights_file = fs.openSync('db_weights_'+file_num, 'w');
    } else {
      cur_map[db_keys[k]] = k1_weights[db_keys[k]];
    }
  }
  fs.writeFileSync(weights_file, JSON.stringify(cur_map));
  fs.closeSync(weights_file);

  for (const k1 of k1s) {
    var word_map = k1_weights[k1];
    getResult(k1, word_map, normalizedDocVector, prototypeIds, closest);
  }

  await mysqlObj.closeConnect(conn);

  var sortedClosest = closest.sort((a, b) => (a.distance < b.distance) ? 1 : ((a.distance > b.distance) ? -1 : 0));

  if(closest[0].distance == 0.0) {
    closest.pop();
    finalResult.primary = primary;
    finalResult.related = related;
    return finalResult;
  }
  var index = fillPrimaryRelated(closest, primary, related);
  primary.sort((a, b) => (a.distance < b.distance) ? 1 : ((a.distance > b.distance) ? -1 : 0));
  related.sort((a, b) => (a.distance < b.distance) ? 1 : ((a.distance > b.distance) ? -1 : 0));
  finalResult.closest = sortedClosest.slice(index + 1, index + 6);
  finalResult.primary = primary;
  finalResult.related = related;

  return finalResult;
}

function getResult(k1, word_map, normalizedDocVector, prototypeIds, closest) {
  var prototypeId = prototypeIds[k1];

  var normalizedWeights = word_map;
  var result = computeDotProduct(k1, prototypeId, normalizedDocVector, normalizedWeights);
  var primary_threshold = 0.0;
  if (k1_primary_thresholds.hasOwnProperty(k1)) {
    primary_threshold = k1_primary_thresholds[k1];
  }
  var related_threshold = 0.0;
  if (k1_related_thresholds.hasOwnProperty(k1)) {
    related_threshold = k1_related_thresholds[k1];
  }
  result.primary_threshold = primary_threshold;
  result.related_threshold = related_threshold;
  closest.push(result);
}

function fillPrimaryRelated(closest, primary, related) {

    var distances;
    var firstBreak = 0.0;
    var secondBreak = 0.0;
    var firstIndex = 0;
    var secondIndex = 0;
    for(var i = 0; i < closest.length && i < 10; i++) {
      var thisBreak = closest[i].distance - closest[i+1].distance;
      if(thisBreak > firstBreak) {
        firstBreak = thisBreak;
        firstIndex = i;
      }
    }
    for(var i = firstIndex + 1; i <  closest.length && i < 15; i++) {
      var thisBreak = closest[i].distance - closest[i+1].distance;
      if(thisBreak > secondBreak) {
        secondBreak = thisBreak;
        secondIndex = i;
      }
    }

    for(var j = 0; j <= firstIndex; j++) {
      primary.push(closest[j]);
    }
    for(var k = (firstIndex + 1); k <= secondIndex; k++) {
      related.push(closest[k]);
    }

    return secondIndex;
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
      var related_threshold = parseFloat(lines[i].substring(p1+19, p2));
      k1_related_thresholds[title] = related_threshold;
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
  words.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
  for (var i=0; i<words.length; i++) {
    var word = words[i];
    if (typeof normalizedWeights == 'undefined') {
      continue;
    }
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
  body = body.toLowerCase().replace(/<script[^>]*>.*?<\/script>/g, '');
  body = body.replace(/<!\-\-.*?\-\->/g, '');
  //var clean_body = body.replace(/[\:\-’]/g, "").replace(/[\r\n]+/g, " ").replace(/<\/?[^>]+>/g, " ").toLowerCase().replace(/\s+/g, " ").replace(/[\"\',\.\(\)\[\]\?“”]/g, "");

// michael parsing
//  var content = body.replace(/[“”\[\]’>&\/…‘~',\.()!?\"\':;%*\-]/g, "").toLowerCase();

// my parsing
  var content = body.replace(/<\/?[^>]+>/g, " ").replace(/\s+/g, " ").replace(/[\(\)]/g, '').replace(/&nbsp;/g, ' ').replace(/&hellip;/g, ' ').replace(/&#8217;/g, '\'').replace(/&amp;/g, '&');
  var content = content.replace(/[“”\[\]’>&\/…‘~',\.()!?\"\':;%*\-]/g, "");
  var toks = content.split(' ');
  var clean_content = '';
  for (var w=0; w<toks.length; w++) {
    var wd = toks[w];
    if (!isNaN(wd)) {
      continue;
    }
    var wdlen = wd.length;
    var firststr = wd.substring(0,1);
    var laststr = wd.substring(wdlen-1);
    if (!laststr.match(/^[0-9a-zA-Z]+/)) {
      wd = wd.substring(0, wdlen-1);
    }
    if (!firststr.match(/^[0-9a-zA-Z]+/)) {
      wd = wd.substring(1);
    }

    var fullmatch = true;
    for (var l=0; l<wd.length; l++) {
      var tmp = wd.substring(l, l+1);
      if (!tmp.match(/[\W0-9]+/)) {
        fullmatch = false;
        break;
      }
    }
    if (fullmatch) {
      continue;
    }
    if (wd.match(/[°]+/)) {
      continue;
    }

    clean_content = clean_content + ' ' + wd;
  }

  var body_no_stop_words = removeStopWords(clean_content);
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
    var bodyObj = await s3Wrapper.getS3DataViaUrl(url);
    var jsonStr = bodyObj.Body.toString();
    jsonObj = JSON.parse(jsonStr);
  } catch(ex) {
    console.log(ex);
  }
  return jsonObj;
}

module.exports = router;
