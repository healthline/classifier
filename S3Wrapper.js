
const AWS = require('aws-sdk');
var fs = require("fs");

class S3Wrapper {

  async createImuidUrlMapPerContentType(imuid_url_map, contentType) {

    var s3 = new AWS.S3();
    var params = {
      Bucket: "hl-json-data-prod",
      MaxKeys: 1000000,
      Prefix: 'data/hlcmsresource/articles/'+contentType
    };

    var resp = [];
    var loop = 0;

    while (true) {
      resp = await new Promise( function( resolve, reject ) {
        s3.listObjectsV2(params, function (err, data) {
          if (err) {
            resolve({});
          } else {
            resolve( data );
          }
        });
      });

      for (var i=0; i<resp['Contents'].length; i++) {
        var name = resp['Contents'][i]['Key'];
        var pos = name.lastIndexOf('/');
        var nameOnly = name.substring(pos+1);
        var pos1 = nameOnly.lastIndexOf('-');
        var tok = nameOnly.substring(pos1+1).replace(/\.json/, '');
        if ((!isNaN(tok) && nameOnly == contentType+'-'+tok+'.json') || name.indexOf(contentType+'/'+contentType+'-') == -1) {
          continue;
        }
        if (i%100 == 0) {
          console.log(nameOnly);
        }

        var body = await this.getS3Data(nameOnly, contentType);
        var imuids = [];
        if (typeof body == 'undefined' || body == null) {
        } else {
          var jsonObj = JSON.parse(body['Body']);
          if (jsonObj.hasOwnProperty('imuids') && jsonObj['imuids'] != null) {
            imuids = jsonObj['imuids'];
          }
        }

        if (imuids.length > 0) {
          imuid_url_map[nameOnly] = imuids;
        }
      }

      if (!resp.hasOwnProperty('NextContinuationToken')) {
        break;
      }
      var token = resp['NextContinuationToken'];
      params['ContinuationToken'] = token;
      loop += 1;
    }
  }

  async createUrlImuidsMap() {
    var imuid_url_map = {};

    await this.createImuidUrlMapPerContentType(imuid_url_map, "healthfeature");
    await this.createImuidUrlMapPerContentType(imuid_url_map, "authoritynutrition");
    await this.createImuidUrlMapPerContentType(imuid_url_map, "newsarticles");
    await this.createImuidUrlMapPerContentType(imuid_url_map, "partner_article");

    var respStr = JSON.stringify(imuid_url_map);
    var outFile = fs.openSync('url_imuids_map', 'a');
    fs.writeFileSync(outFile, respStr);
    fs.closeSync(outFile);

    return Object.keys(imuid_url_map).length;
  }

  modifyUrl(url) {
    var pos = 0;
    if (url.indexOf('/') == 0) {
      pos = 1;
    }
    var modUrl = url.substring(pos);
    var toks = modUrl.split('/');
    var first = toks[0];
    var newUrl = modUrl.substring(first.length+1).replace(/\//g, '__');
    if (first == 'health') {
      return ('healthfeature/healthfeature-' + newUrl);
    } else if (first == 'nutrition') {
      return ('authoritynutrition/authoritynutrition-' + newUrl);
    } else if (first == 'health-news') {
      return ('newsarticles/newsarticles-' + newUrl);
    } else if (first == 'diabetesmine') {
      newUrl = modUrl.replace('/', '__');
      return ('partner_article/partner_article-' + newUrl);
    }
    return '';
  }

  getContentViaUrl(url) {
    var newUrl = url;
    if (newUrl.length == 0) {
      return null;
    }
    newUrl = this.modifyUrl(url);
    console.log(newUrl);
    var s3 = new AWS.S3();
    var bucketName = 'hl-json-data-prod';

    var key = 'data/hlcmsresource/articles/' + newUrl + '.json';
    console.log(key);
    var params = {Bucket: bucketName, Key: key};

    return new Promise( function( resolve, reject ) {
      s3.getObject(params, function (err, data) {
        if (err) {
          resolve(null);
        } else {
          console.log('s3 fetch success');
          resolve( data );
        }
      });
    });
  }

  getContent(url, contentType) {
    var newUrl = url;
    if (newUrl.length == 0) {
      return null;
    }
    var s3 = new AWS.S3();
    var bucketName = 'hl-json-data-prod';

    var key = 'data/hlcmsresource/articles/' + contentType + '/' + newUrl;
    var params = {Bucket: bucketName, Key: key};
    var fileObj = null;
    var done = false;

    return new Promise( function( resolve, reject ) {
      s3.getObject(params, function (err, data) {
        if (err) {
          resolve(null);
        } else {
          resolve( data );
        }
      });
    });
  }

  async getS3Data(url, contentType) {
    var jsonBody = await this.getContent(url, contentType);
    return jsonBody;
  }

  async getS3DataViaUrl(url) {
    var jsonBody = await this.getContentViaUrl(url);
    return jsonBody;
  }
};

module.exports = S3Wrapper;
