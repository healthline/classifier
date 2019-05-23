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

async function getDatabaseConnection(mysqlObj) {
  var configJson = mysqlObj.getDatabaseConfig('config/database.cf');
  conn = await mysqlObj.getConnection(configJson);
  return conn;
}

async function getDataFromDatabase(mysqlObj, conn, k1s) {
  var k1ImuidsMap = {};
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
  var counter = 0;
  for (const k1 of k1s) {
    console.log(k1);
    if (fs.existsSync('super_docs/' + k1)) {
      console.log('super_doc for ' + k1 + ' exists - skipping');
      continue;
    }
    counter++;
    //var json_files = fs.openSync('json_files_processed/' + k1, 'a');
    var k1_super_doc = fs.openSync('super_docs/' + k1, 'a'); // append mode is VERY important
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
      } else if (temp_url.indexOf("partner_article") > -1) {
        contentType = "partner_article";
      }
      var obj = await s3.getS3Data(urls[u], contentType);
      num_files += 1;
      if (obj == null) {
        console.log('null content for ' + urls[u]);
        continue;
      }
      //fs.writeFileSync(json_files, urls[u] + '\n');
      var body = obj['Body'];
      router.createSingleK1Document(body, k1_super_doc);
      urlcnt += 1;
    }

    fs.closeSync(k1_super_doc);
    //fs.closeSync(json_files);
    var stat = k1 + '--' + urlcnt + '\n';
    fs.writeFileSync(outfile, stat);
  }
  fs.closeSync(outfile);
}

router.createSingleK1Document = (body, k1_super_doc) => {
  var articleJson = JSON.parse(body);
  var articleTitle = articleJson['title'];
  if (articleTitle.indexOf("QA") > -1) {
    return;
  }
  var title3Times = articleTitle + ' ' + articleTitle + ' ' + articleTitle + ' ';
  var articleBody = title3Times + articleJson['body'];
  var oneLineArticleBody = articleBody.replace(/[\r\n]+/g, " ").replace('&nbsp;', ' ').replace('&hellip;', ' ').replace('&amp;', '&');
  fs.writeFileSync(k1_super_doc, oneLineArticleBody + '\n');
}

function createK1CleanDocuments(k1s) {
  var counter = 0;
  for (const k1 of k1s) {
    if (fs.existsSync('clean_super_docs/'+k1)) {
      console.log('clean superdoc for ' + k1 + ' exists');
      continue;
    }
    counter++;
    console.log('processing ' + k1);

    console.log('creating clean superdoc for ' + k1);
    var output_file = fs.openSync('clean_super_docs/'+k1, 'a'); // append mode is VERY VERY important

    router.createSingleK1CleanSuperDoc(k1, output_file);
  }
}

router.createSingleK1CleanSuperDoc = (k1, output_file) => {
  var input_file = fs.openSync('super_docs/'+k1, 'r');
  var input_content = fs.readFileSync(input_file);
  input_content = input_content.toString();
  fs.closeSync(input_file);

  input_content = input_content.toLowerCase().replace(/<script[^>]*>.*?<\/script>/g, '');
  input_content = input_content.replace(/<!\-\-.*?\-\->/g, '');

  var input_lines = input_content.split("\n");

  var linecount = 0;
  for (var input_line of input_lines) {
    linecount++;
    console.log(linecount + ": " + input_line.substring(0, 10));
    var content = input_line.replace(/<\/?[^>]+>/g, " ").replace(/\s+/g, " ").replace(/[\(\)]/g, '').replace(/&nbsp;/g, ' ').replace(/&hellip;/g, ' ').replace(/&#8217;/g, '\'').replace(/&amp;/g, '&');
    var content = content.replace(/[“”\[\]’>&\/…‘~',\.()!?\"\':;%*\-]/g, "");
    var toks = content.split(' ');
    var clean_content = '';
    for (var w=0; w<toks.length; w++) {
      var wd = toks[w];
      if ((!isNaN(wd) || wd.length < 3) && !(wd == "ms" || wd == "ed")) {
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
    fs.writeFileSync(output_file, clean_content + '\n');
  }
  fs.closeSync(output_file);
}

function createTfDfMaps(k1s, doc_word_freq_map, word_doc_freq_map, word_doc_freq_map_mz) {
  var counter = 0;
  var global_doc_count = 0;
  var total_distinct_word_count = 0;
  for (const k1 of k1s) {
    counter++;
    doc_word_freq_map[k1] = {};

    console.log('processing ' + k1);

    var distinct_word_map = {};
    var linecount = router.createSingleK1TfIdfMap(k1, distinct_word_map, doc_word_freq_map, word_doc_freq_map, word_doc_freq_map_mz);
    global_doc_count += linecount;

    var distinct_words = Object.keys(distinct_word_map);
    total_distinct_word_count += distinct_words.length;
  }

  console.log("Total distinct words: " + total_distinct_word_count);
  return global_doc_count;
}

router.createSingleK1TfIdfMap = (k1, distinct_word_map, doc_word_freq_map, word_doc_freq_map, word_doc_freq_map_mz) => {
  var input_file = fs.openSync('clean_super_docs/'+k1, 'r');
  var input_content = fs.readFileSync(input_file);
  input_content = input_content.toString().toLowerCase();
  fs.closeSync(input_file);

  var input_lines = input_content.split("\n");
  var linecount = 0;
  for (var input_line of input_lines) {
    linecount++;
    var this_doc_word_map = {};
    var toks = removeStopWords(input_line);

    var prevWord = '';
    for (var tk of toks) {
      if (!this_doc_word_map.hasOwnProperty(tk)) {
        this_doc_word_map[tk] = 1;
      } else {
        this_doc_word_map[tk] += 1;
      }
      if (tk == prevWord) {
        continue;
      }
      prevWord = tk;
    }

    var bigrams = [];
    for (var w=0; w<toks.length-1; w++) {
      var bigram = toks[w] + "_" + toks[w+1];
      bigrams.push(bigram);
      this_doc_word_map[bigram] = 1;
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

    var this_doc_distinct_words = Object.keys(this_doc_word_map);
    for (var dword of this_doc_distinct_words) {
      if (word_doc_freq_map_mz.hasOwnProperty(dword)) {
        word_doc_freq_map_mz[dword] += this_doc_word_map[dword];
      } else {
        word_doc_freq_map_mz[dword] = this_doc_word_map[dword];
      }
    }
  }

  doc_word_freq_map[k1] = distinct_word_map;

  //var tmp2 = Object.keys(distinct_word_map);
  //var tmp2 = tmp2.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
  //var words_file = fs.openSync('k1_words/'+k1, 'a');
  //for (var wd of tmp2) {
  //  fs.writeFileSync(words_file, wd + '\n');
  //}
  //fs.closeSync(words_file);

  var distinct_words = Object.keys(distinct_word_map);
  for (var dword of distinct_words) {
    if (word_doc_freq_map.hasOwnProperty(dword)) {
      word_doc_freq_map[dword] += 1;
    } else {
      word_doc_freq_map[dword] = 1;
    }
  }

  return linecount;
}

async function createWeightsFiles(mysqlObj, conn, doc_word_freq_map, word_doc_freq_map, globalDocCount) {

  //await mysqlObj.deleteWeightsForK1(conn, "idf");

  var WEIGHT_THRESHOLD = 0.003;
  var docKeys = Object.keys(doc_word_freq_map);
  var numDocs = globalDocCount*3;
  for (var thisK1 of docKeys) {
    console.log('calculating weights for ' + thisK1);
    var word_weights_map = {};
    var thisIdfMap = {};
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
        thisIdfMap[word] = idf;
        if (wdcntr>0 && wdcntr%1000 == 0) {
          //await mysqlObj.insertWeightForK1(conn, "idf", thisIdfMap);
          thisIdfMap = {};
        }
      }
      var word_weight = tf * idf;
      wdcntr++;
      if (word_weight < WEIGHT_THRESHOLD) {
        continue;
      }
      total_distance_squared += word_weight * word_weight;

      word_weights_map[word] = [tf, df, word_weight];
    }
    //await mysqlObj.insertWeightForK1(conn, "idf", thisIdfMap); // remaining ones
    var total_distance = Math.sqrt(total_distance_squared);

    //await mysqlObj.deleteWeightsForK1(conn, thisK1);

    var normalized_weights = {};
    var words2 = Object.keys(word_weights_map);
    var sorted_words2 = words2.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
    for (var wd of sorted_words2) {
      var wt = (word_weights_map[wd][2]/total_distance);
      wt = parseFloat(wt);
      if (wt < WEIGHT_THRESHOLD) {
        continue;
      }
      normalized_weights[wd] = wt.toFixed(4);
    }
    console.log(thisK1 + ', word_count: ' + Object.keys(word_weights_map).length + ', word_count_above_threshold: ' + Object.keys(normalized_weights).length);

    //await mysqlObj.insertWeightForK1(conn, thisK1, normalized_weights);

    //var tf_idf_doc = fs.openSync('tf_idf_docs/' + thisK1, "a");
    //for (var wd of Object.keys(normalized_weights)) {
    //  var tf = word_weights_map[wd][0];
    //  var df = word_weights_map[wd][1];
    //  fs.writeFileSync(tf_idf_doc, wd + '\t' + tf + '\t' + df + '\t' + normalized_weights[wd] + '\n');
    //}
    //fs.closeSync(tf_idf_doc);
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

  var k1s = await handleCanonicalize();

  var json_obj = {k1_count : k1s.length};
  res.send(JSON.stringify(json_obj));
});

async function handleCanonicalize() {
  // cleanup result directories
  //rimraf.sync('./super_docs');
  //rimraf.sync('./noscript_super_docs');
  //rimraf.sync('./clean_super_docs');
  //rimraf.sync('./tf_idf_docs');
  //rimraf.sync('./k1_words');

  //fs.mkdirSync('./super_docs');
  //fs.mkdirSync('./noscript_super_docs');
  //fs.mkdirSync('./clean_super_docs');
  //fs.mkdirSync('./tf_idf_docs');
  //fs.mkdirSync('./k1_words');

  // time consuming step - 1
  if (!fs.existsSync('url_imuids_map')) {
    console.log('Creating UrlImuidsMap from S3');
    await createUrlImuidsMapFromS3();
  } else {
    console.log('UrlImuidsMap exists - skipping step-1');
  }

  var url_imuids_map = getUrlImuidsMap();

  var mysqlObj = new MySQLWrapper();

  var k1s = [];
  var conn = await getDatabaseConnection(mysqlObj);
  var k1ImuidsMap = await getDataFromDatabase(mysqlObj, conn, k1s);

  console.log('step-2');

  // time consuming step - 2
  await createK1SuperDocumentsFromS3(k1s, k1ImuidsMap, url_imuids_map);

  console.log('step-3');

  createK1CleanDocuments(k1s);

  var word_doc_freq_map_mz = {};
  var doc_word_freq_map = {};
  var word_doc_freq_map = {};

  var globalDocCount = createTfDfMaps(k1s, doc_word_freq_map, word_doc_freq_map, word_doc_freq_map_mz);
  console.log("Global doc count: " + globalDocCount);

  await createWeightsFiles(mysqlObj, conn, doc_word_freq_map, word_doc_freq_map_mz, globalDocCount); // rplaced with MZ's doc freq map

  return k1s;
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

async function createNonK1Prototypes(category_files_map) {

  var cats = Object.keys(category_files_map);
  await createNonK1SuperDocumentsFromS3(cats, category_files_map);
}

async function createNonK1SuperDocumentsFromS3(cats, catFileMap) {
  var s3 = new S3Wrapper();
  for (const k1 of cats) {
    if (fs.existsSync('nonk1_super_docs/' + k1)) {
      console.log('nonk1_super_doc for ' + k1 + ' exists - skipping');
      continue;
    }
    counter++;
    var k1_super_doc = fs.openSync('nonk1_super_docs/' + k1, 'a');
    console.log('+++ processing +++ ' + k1);

    var urls = catFileMap[k1];
    var urlcnt = 0;
    for (var u=0; u<urls.length; u++) {
      var temp_url = urls[u];

      var contentType = '';
      if (temp_url.indexOf("healthfeature") > -1) {
        contentType = "healthfeature";
      } else if (temp_url.indexOf("authoritynutrition") > -1) {
        contentType = "authoritynutrition";
      } else if (temp_url.indexOf("newsarticles") > -1) {
        contentType = "newsarticles";
      } else if (temp_url.indexOf("partner_article") > -1) {
        contentType = "partner_article";
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
      if (articleTitle.indexOf("QA") > -1) {
        continue;
      }
      var title3Times = articleTitle + ' ' + articleTitle + ' ' + articleTitle + ' ';
      var articleBody = title3Times + articleJson['body'];
      var oneLineArticleBody = articleBody.replace(/[\r\n]+/g, " ").replace('&nbsp;', ' ').replace('&hellip;', ' ').replace('&amp;', '&');
      fs.writeFileSync(k1_super_doc, oneLineArticleBody + '\n');
      urlcnt += 1;
    }

    fs.closeSync(k1_super_doc);
    var stat = k1 + '--' + urlcnt + '\n';
    console.log(stat);
  }
}

function createNonK1CleanDocuments(nonK1s) {
  var counter = 0;
  for (const k1 of nonK1s) {
    if (fs.existsSync('nonk1_clean_super_docs/'+k1)) {
      console.log('clean superdoc for ' + k1 + ' exists');
      continue;
    }
    counter++;
    console.log('processing ' + k1);

    var input_file = fs.openSync('nonk1_super_docs/'+k1, 'r');
    var input_content = fs.readFileSync(input_file);
    input_content = input_content.toString();
    fs.closeSync(input_file);

    input_content = input_content.toLowerCase().replace(/<script[^>]*>.*?<\/script>/g, '');
    input_content = input_content.replace(/<!\-\-.*?\-\->/g, '');

    var input_lines = input_content.split("\n");

    console.log('creating clean superdoc for ' + k1);
    var output_file = fs.openSync('nonk1_clean_super_docs/'+k1, 'a');

    var linecount = 0;
    for (var input_line of input_lines) {
      linecount++;
      console.log(linecount + ": " + input_line.substring(0, 10));
      var content = input_line.replace(/<\/?[^>]+>/g, " ").replace(/\s+/g, " ").replace(/[\(\)]/g, '').replace(/&nbsp;/g, ' ').replace(/&hellip;/g, ' ').replace(/&#8217;/g, '\'').replace(/&amp;/g, '&');
      var content = content.replace(/[“”\[\]’>&\/…‘~',\.()!?\"\':;%*\-]/g, "");
      var toks = content.split(' ');
      var clean_content = '';
      for (var w=0; w<toks.length; w++) {
        var wd = toks[w];
        if ((!isNaN(wd) || wd.length < 3) && !(wd == "ms" || wd == "ed")) {
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
      fs.writeFileSync(output_file, clean_content + '\n');
    }
    fs.closeSync(output_file);
  }
}

function createNonk1TfDfMaps(k1s, doc_word_freq_map, word_doc_freq_map, word_doc_freq_map_mz) {
  var counter = 0;
  var global_doc_count = 0;
  var total_distinct_word_count = 0;
  for (const k1 of k1s) {
    counter++;
    doc_word_freq_map[k1] = {};

    console.log('processing ' + k1);

    var input_file = fs.openSync('nonk1_clean_super_docs/'+k1, 'r');
    var input_content = fs.readFileSync(input_file);
    input_content = input_content.toString().toLowerCase();
    fs.closeSync(input_file);

    var input_lines = input_content.split("\n");
    var distinct_word_map = {};
    var linecount = 0;
    for (var input_line of input_lines) {
      global_doc_count++;
      linecount++;
      var this_doc_word_map = {};
      var toks = removeStopWords(input_line);

      var prevWord = '';
      for (var tk of toks) {
        if (!this_doc_word_map.hasOwnProperty(tk)) {
          this_doc_word_map[tk] = 1;
        } else {
          this_doc_word_map[tk] += 1;
        }
        if (tk == prevWord) {
          continue;
        }
        prevWord = tk;
      }

      var bigrams = [];
      for (var w=0; w<toks.length-1; w++) {
        var bigram = toks[w] + "_" + toks[w+1];
        bigrams.push(bigram);
        this_doc_word_map[bigram] = 1;
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

      var this_doc_distinct_words = Object.keys(this_doc_word_map);
      for (var dword of this_doc_distinct_words) {
        if (word_doc_freq_map_mz.hasOwnProperty(dword)) {
          word_doc_freq_map_mz[dword] += this_doc_word_map[dword];
        } else {
          word_doc_freq_map_mz[dword] = this_doc_word_map[dword];
        }
      }
    }

    doc_word_freq_map[k1] = distinct_word_map;

    var distinct_words = Object.keys(distinct_word_map);
    for (var dword of distinct_words) {
      if (word_doc_freq_map.hasOwnProperty(dword)) {
        word_doc_freq_map[dword] += 1;
      } else {
        word_doc_freq_map[dword] = 1;
      }
    }

    total_distinct_word_count += distinct_words.length;
  }

  console.log("Total distinct words: " + total_distinct_word_count);
  return global_doc_count;
}

async function createNonK1WeightsFiles(mysqlObj, conn, doc_word_freq_map, word_doc_freq_map, globalDocCount) {

  await mysqlObj.deleteWeightsForNonK1(conn, "idf");

  var WEIGHT_THRESHOLD = 0.003;
  var docKeys = Object.keys(doc_word_freq_map);
  var numDocs = globalDocCount*3;
  for (var thisK1 of docKeys) {
    console.log('calculating weights for ' + thisK1);
    var word_weights_map = {};
    var thisIdfMap = {};
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
        thisIdfMap[word] = idf;
        if (wdcntr>0 && wdcntr%1000 == 0) {
          await mysqlObj.insertWeightForNonK1(conn, "idf", thisIdfMap);
          thisIdfMap = {};
        }
      }
      var word_weight = tf * idf;
      wdcntr++;
      if (word_weight < WEIGHT_THRESHOLD) {
        continue;
      }
      total_distance_squared += word_weight * word_weight;

      word_weights_map[word] = [tf, df, word_weight];
    }
    await mysqlObj.insertWeightForNonK1(conn, "idf", thisIdfMap); // remaining ones
    var total_distance = Math.sqrt(total_distance_squared);

    await mysqlObj.deleteWeightsForNonK1(conn, thisK1);

    var normalized_weights = {};
    var words2 = Object.keys(word_weights_map);
    var sorted_words2 = words2.sort((a, b) => (a > b) ? 1 : ((a < b) ? -1 : 0));
    for (var wd of sorted_words2) {
      var wt = (word_weights_map[wd][2]/total_distance);
      wt = parseFloat(wt);
      if (wt < WEIGHT_THRESHOLD) {
        continue;
      }
      normalized_weights[wd] = wt.toFixed(4);
    }
    console.log(thisK1 + ', word_count: ' + Object.keys(word_weights_map).length + ', word_count_above_threshold: ' + Object.keys(normalized_weights).length);

    await mysqlObj.insertWeightForNonK1(conn, thisK1, normalized_weights);
  }
}

router.get('/report', async function(req, res, next) {
  req.setTimeout(0);

  rimraf.sync('./nonk1_super_docs');
  rimraf.sync('./nonk1_clean_super_docs');

  fs.mkdirSync('./nonk1_super_docs');
  fs.mkdirSync('./nonk1_clean_super_docs');

  var category_files_map = {
    "postpartum_depression":["healthfeature-depression__postpartum-depression.json","healthfeature-depression__how-to-deal-with-postpartum-depression.json","newsarticles-does-painful-childbirth-increase-post-partum-depression-risk.json","newsarticles-children-suffer-when-mothers-have-postpartum-depression.json","healthfeature-postpartum-depression__importance-of-maternal-mental-health.json","healthfeature-depression__ivanka-trump-postpartum-depression.json","healthfeature-depression__best-postpartum-depression-blogs.json","newsarticles-why-women-may-need-to-read-this-disclaimer-before-seeing-tully.json","healthfeature-postpartum-depression__lessons-learn-as-new-mom.json","newsarticles-mental-what-women-should-know-about-postpartum-depression-111913.json"],
    "postpartum_anxiety":["healthfeature-pregnancy__i-tried-therapy-app-postpartum-anxiety.json","healthfeature-depression__postpartum-depression.json","healthfeature-pregnancy__i-had-postpartum-anxiety.json","healthfeature-postpartum-depression__lessons-learn-as-new-mom.json","healthfeature-pregnancy__i-had-postpartum-anxiety.json","healthfeature-parenting__motherhood-and-anxiety.json"],
    "mental_health_during_pregnancy":["healthfeature-pregnancy__anxiety-coping-tips.json","newsarticles-what-parents-should-know-about-postpatrum-and-peripartum-treatment.json","newsarticles-women-depression-during-pregnancy-increases-childs-risk-of-mood-disorders-100913.json","healthfeature-perinatal-depression-is-depression-during-pregnancy-and-its-real.json","healthfeature-perinatal-depression-is-depression-during-pregnancy-and-its-real.json"],
    "pregnancy_nutrition":["healthfeature-pregnancy__diet-nutrition.json","authoritynutrition-13-foods-to-eat-when-pregnant.json","authoritynutrition-11-foods-to-avoid-during-pregnancy.json","healthfeature-pregnancy__nutrition.json","healthfeature-pregnancy__second-trimester-diet-nutrition.json","healthfeature-food-safety-pregnancy.json","healthfeature-pregnancy__best-fruits-to-eat.json","healthfeature-pregnancy__crab-and-seafood.json","healthfeature-baby__pregnancy-myths.json","authoritynutrition-supplements-during-pregnancy.json","healthfeature-pregnancy__gestational-diabetes-food-list.json","healthfeature-pregnancy__paleo-diet.json","healthfeature-pregnancy__food-aversions.json","authoritynutrition-foods-high-in-folate-folic-acid.json"],
    "postpartum_nutrition":["authoritynutrition-breastfeeding-diet-101.json","authoritynutrition-breastfeeding-and-weight-loss.json","healthfeature-parenting__lactation-boosting-recipes.json","healthfeature-eating-healthy-as-new-parent.json","healthfeature-pregnancy__nourishing-soups-postpartum.json","authoritynutrition-weight-loss-after-pregnancy.json","newsarticles-are-placenta-pills-safe-for-your-baby.json","newsarticles-heres-how-vitamin-d-supplements-can-help-new-moms.json"]
  }

  await createNonK1Prototypes(category_files_map);

  var cats = Object.keys(category_files_map);

  createNonK1CleanDocuments(cats);

  var mysqlObj = new MySQLWrapper();
  var conn = await getDatabaseConnection(mysqlObj);
  var word_doc_freq_map_mz = {};
  var doc_word_freq_map = {};
  var word_doc_freq_map = {};

  var globalDocCount = createNonk1TfDfMaps(cats, doc_word_freq_map, word_doc_freq_map, word_doc_freq_map_mz);
  console.log("Global doc count: " + globalDocCount);

  await createNonK1WeightsFiles(mysqlObj, conn, doc_word_freq_map, word_doc_freq_map_mz, globalDocCount);

  res.send(JSON.stringify({done: true}));
});

module.exports = router;
