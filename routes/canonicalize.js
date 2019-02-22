var express = require('express');
var router = express.Router();
const MySQLWrapper = require('../MySQLWrapper.js');
const S3Wrapper = require('../S3Wrapper.js');
var fs = require("fs");
var SolrNode = require('solr-node');
var rimraf = require("rimraf");

async function processSolrDocs(client, k1, imuids) {
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
    var strQuery = client.query().q({sourcename:'hlcms', lang:'en', imuids:testimuid}).fl('sourcename, contenttype, cmsurl, url, subtype, title, nid, imuids, nonanalyzedbody').start(0).rows(100000);
    console.log(strQuery['params'][0].substring(0, 100));
    client.search(strQuery, function (err, result1) {
       if (err) {
          console.log(err);
          return 'error';
       }
       resolve(result1);
     });
  });

  var resp = await result.response;

  var super_doc = fs.openSync('super_docs/' + k1, "a");
  for (var i=0; i<resp.docs.length; i++) {
    var doc = resp.docs[i];
    var contentType = doc["contenttype"];
    if (contentType != "healthfeature" && contentType != "authoritynutrition") {
      continue;
    }
    if (doc.hasOwnProperty("nonanalyzedbody")) {
      var content = (doc["nonanalyzedbody"] + "\n\n").toLowerCase();
      fs.writeFileSync(super_doc, content);
    } else {
      fs.writeFileSync(super_doc, "\n\n");
    }
  }
  fs.closeSync(super_doc);
  return resp;
}

function getUrlImuidsMap() {
  var infile = fs.openSync('url_imuids_map', 'r');
  var content = fs.readFileSync(infile);
  var jsonObj = JSON.parse(content);
  fs.closeSync(infile);

  return jsonObj;
}

function getDistinctK1sFromFile() {
  var k1_data = fs.readFileSync('distinct_k1s.txt', 'utf-8');
  var distinct_k1s_1 = JSON.parse(k1_data);
  var distinct_k1s = [];
  for (var k=0; k<distinct_k1s_1.length; k++) {
    distinct_k1s.push(distinct_k1s_1[k]);
  }
  return distinct_k1s;
}

async function getDataFromDatabase(k1s) {
  var k1ImuidsMap = {};
  var mysqlObj = new MySQLWrapper();
  var configJson = mysqlObj.getDatabaseConfig('config/database.cf');
  var conn = await mysqlObj.getConnection(configJson);
  if (conn == null) {
    return {};
  }

  var tmpk1s = await mysqlObj.getDistinctK1s(conn);
  for (var k=0; k<tmpk1s.length; k++) {
    k1s.push(tmpk1s[k]);
  }

  for (const k1 of k1s) {
    var imuids = await mysqlObj.getImuidsForK1(conn, k1);
    k1ImuidsMap[k1] = imuids;
  }

  //await mysqlObj.closeConnect(conn);
  return k1ImuidsMap;
}

async function createUrlImuidsMapFromS3() {
  var s3 = new S3Wrapper();
  await s3.createUrlImuidsMap();
}

async function createK1SuperDocumentsFromS3(k1s, k1ImuidsMap, url_imuids_map) {
  var num_files = 0;
  var outfile = fs.openSync('k1_num_urls', 'a');
  var s3 = new S3Wrapper();
  for (const k1 of k1s) {
    if (fs.existsSync('super_docs/' + k1)) {
      continue;
    }
    var k1_super_doc = fs.openSync('super_docs/' + k1, 'a');
    var k1_imuids = k1ImuidsMap[k1];
    console.log('+++ processing +++ ' + k1);

    var urls = Object.keys(url_imuids_map);
    var urlcnt = 0;
    for (var u=0; u<urls.length; u++) {
      var temp_url = urls[u];
      var this_url_imuids = url_imuids_map[temp_url];

      var found_first = false;
      var first_imuid = parseInt(this_url_imuids[0]);
      for (var k=0; k<k1_imuids.length; k++) {
        if (k1_imuids[k] == first_imuid) {
          found_first = true;
          break;
        }
      }
      if (!found_first) {
        continue;
      }

      var contentType = '';
      if (temp_url.indexOf("healthfeature") > -1) {
        contentType = "healthfeature";
      } else if (temp_url.indexOf("authoritynutrition") > -1) {
        contentType = "authoritynutrition";
      } else if (temp_url.indexOf("newsarticles") > -1) {
        contentType = "newsarticles";
      } else if (temp_url.indexOf("sponsoredprogram") > -1) {
        contentType = "sponsoredprogram";
      }
      var obj = await s3.getS3Data(urls[u], contentType);
      num_files += 1;
      if (obj == null) {
        console.log('null content for ' + urls[u]);
        continue;
      }
      var body = obj['Body'];
      var articleJson = JSON.parse(body);
      var articleTitle = articleJson['title'];
      var title3Times = articleTitle + ' ' + articleTitle + ' ' + articleTitle + ' ';
      var articleBody = title3Times + articleJson['body'];
      var oneLineArticleBody = articleBody.replace(/[\r\n]+/g, " ").replace('&nbsp;', ' ').replace('&hellip;', ' ').replace('&amp;', '&');
      //console.log(urls[u]);
      fs.writeFileSync(k1_super_doc, oneLineArticleBody + '\n\n');
      urlcnt += 1;
    }

    fs.closeSync(k1_super_doc);
    var stat = k1 + '--' + urlcnt + '\n';
    fs.writeFileSync(outfile, stat);
  }
  fs.closeSync(outfile);
}

function createK1CleanDocuments(k1s) {
  for (const k1 of k1s) {
    if (fs.existsSync('super_docs/'+k1)) {
      continue;
    }
    console.log('processing ' + k1);

    var input_file = fs.openSync('super_docs/'+k1, 'r');
    var input_content = fs.readFileSync(input_file);
    input_content = input_content.toString();
    fs.closeSync(input_file);

    var output_file1 = fs.openSync('noscript_super_docs/'+k1, 'w');
    input_content = input_content.toLowerCase().replace(/<script[^>]*>.*?<\/script>/g, '');
    input_content = input_content.replace(/<!\-\-.*?\-\->/g, '');
    fs.writeFileSync(output_file1, input_content);
    fs.closeSync(output_file1);

    var output_file = fs.openSync('clean_super_docs/'+k1, 'w');
    //var content = input_content.replace(/[\:’]/g, "").replace(/<\/?[^>]+>/g, " ").replace(/\s+/g, " ").replace(/[\"\',\.\(\)\[\]\?“”]/g, "");
    var content = input_content.replace(/<\/?[^>]+>/g, " ").replace(/\s+/g, " ").replace(/[\(\)]/g, '').replace(/&nbsp;/g, ' ').replace(/&hellip;/g, ' ').replace(/&#8217;/g, '\'').replace(/&amp;/g, '&');
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
    fs.writeFileSync(output_file, clean_content);
    fs.closeSync(output_file);
  }
}

function createTfDfMaps(k1s, doc_word_freq_map, word_doc_freq_map) {
  for (const k1 of k1s) {
    doc_word_freq_map[k1] = {};

    console.log('processing ' + k1);

    var input_file = fs.openSync('clean_super_docs/'+k1, 'r');
    var input_content = fs.readFileSync(input_file);
    input_content = input_content.toString().toLowerCase();
    fs.closeSync(input_file);
    var toks = removeStopWords(input_content);//input_content.split(' ');

    var bigrams = [];
    var distinct_word_map = {};

    for (var w=1; w<toks.length; w++) {
      var bigram = toks[w-1] + "_" + toks[w];
      bigrams.push(bigram);
    }

    for (var w=0; w<toks.length; w++) {
      if (!distinct_word_map.hasOwnProperty(toks[w])) {
        distinct_word_map[toks[w]] = 1;
      } else {
        var freq = distinct_word_map[toks[w]];
        distinct_word_map[toks[w]] = freq+1;
      }
    }

    for (var w=0; w<bigrams.length; w++) {
      if (!distinct_word_map.hasOwnProperty(bigrams[w])) {
        distinct_word_map[bigrams[w]] = 1;
      } else {
        var freq = distinct_word_map[bigrams[w]];
        distinct_word_map[bigrams[w]] = freq+1;
      }
    }

    doc_word_freq_map[k1] = distinct_word_map;

    console.log('computing doc freqs');

    var distinct_words = Object.keys(distinct_word_map);
    for (var dword of distinct_words) {
      if (word_doc_freq_map.hasOwnProperty(dword)) {
        word_doc_freq_map[dword] += 1;
      } else {
        word_doc_freq_map[dword] = 1;
      }
    }
  }
}

function createWeightsFiles(doc_word_freq_map, word_doc_freq_map) {
  var WEIGHT_THRESHOLD = 0.005;
  var docKeys = Object.keys(doc_word_freq_map);
  var numDocs = docKeys.length;
  for (var thisK1 of docKeys) {
    console.log('calculating weights for ' + thisK1);
    var word_weights_map = {};
    var word_freq_map = doc_word_freq_map[thisK1];
    var words1 = Object.keys(word_freq_map);
    var sorted_words1 = words1.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
    var total_distance_squared = 0.0;
    var wdcntr = 0;
    for (var word of sorted_words1) {

      var tf = word_freq_map[word];
      var idf = 1;
      var df = word_doc_freq_map[word];
      if (word_doc_freq_map.hasOwnProperty(word)) {
        idf = (Math.log10((numDocs/df), 10)).toFixed(4);
      }
      var word_weight = tf * idf;
      wdcntr++;
      if (word_weight < WEIGHT_THRESHOLD) {
        continue;
      }
      total_distance_squared += word_weight * word_weight;

      word_weights_map[word] = [df, idf, word_weight];
    }
    var total_distance = Math.sqrt(total_distance_squared);

    var normalized_weights = {};
    var tf_idf_doc_debug = fs.openSync('tf_idf_docs/' + thisK1 + '_debug', "a");
    var words2 = Object.keys(word_weights_map);
    var sorted_words2 = words2.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
    for (var wd of sorted_words2) {
      var wt = (word_weights_map[wd][2]/total_distance);
      wt = parseFloat(wt);
      if (wt < WEIGHT_THRESHOLD) {
        continue;
      }
      normalized_weights[wd] = wt.toFixed(4);

      var df = word_weights_map[wd][0];
      var idf = word_weights_map[wd][1];

      var line = wd + '\t' + tf + '\t' + df + '\t' + idf + '\t' + normalized_weights[wd] + '\n';
      fs.writeFileSync(tf_idf_doc_debug, line);
    }
    fs.closeSync(tf_idf_doc_debug);
    console.log(thisK1 + ', word_count: ' + Object.keys(word_weights_map).length + ', word_count_above_threshold: ' + Object.keys(normalized_weights).length);

    var tf_idf_doc = fs.openSync('tf_idf_docs/' + thisK1, "a");
    for (var wd of Object.keys(normalized_weights)) {
      fs.writeFileSync(tf_idf_doc, wd + '\t' + normalized_weights[wd] + '\n');
    }
    fs.closeSync(tf_idf_doc);
  }
}

async function getDocsFromSolr(k1s, mysqlObj) {
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
    var imuids = await mysqlObj.getImuidsForK1(conn, k1);
    resp = await processSolrDocs(client, k1, imuids);
    console.log(k1);
  }
}

router.get('/', async function(req, res, next) {
  req.setTimeout(0);

  // cleanup result directories
  //rimraf.sync('./super_docs');
  //rimraf.sync('./noscript_super_docs');
  //rimraf.sync('./clean_super_docs');
  rimraf.sync('./tf_idf_docs');

  //fs.mkdirSync('./super_docs');
  //fs.mkdirSync('./noscript_super_docs');
  //fs.mkdirSync('./clean_super_docs');
  fs.mkdirSync('./tf_idf_docs');

  // time consuming step - 1
  if (!fs.existsSync('url_imuids_map')) {
    console.log('Creating UrlImuidsMap from S3');
    await createUrlImuidsMapFromS3();
  } else {
    console.log('UrlImuidsMap exists - skipping step-1');
  }

  var url_imuids_map = getUrlImuidsMap();

  var k1s = [];
  var k1ImuidsMap = await getDataFromDatabase(k1s);

  // time consuming step - 2
  await createK1SuperDocumentsFromS3(k1s, k1ImuidsMap, url_imuids_map);

  createK1CleanDocuments(k1s);

  var doc_word_freq_map = {};
  var word_doc_freq_map = {};

  createTfDfMaps(k1s, doc_word_freq_map, word_doc_freq_map);

  createWeightsFiles(doc_word_freq_map, word_doc_freq_map);

  var json_obj = {k1_count : k1s.length};
  res.send(JSON.stringify(json_obj));
});

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

module.exports = router;
