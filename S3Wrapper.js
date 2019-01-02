
const AWS = require('aws-sdk');

class S3Wrapper {

  modifyUrl(url) {
    var pos = 0;
    if (url.indexOf('/') == 0) {
      pos = 1;
    }
    var modUrl = url.substring(pos);
    var toks = modUrl.split('/');
    var first = toks[0];
    var newUrl = modUrl.substring(first.length+1).replace('/', '__');
    if (first == 'health') {
      return ('healthfeature/healthfeature-' + newUrl);
    } else if (first == 'nutrition') {
      return ('authoritynutrition/authoritynutrition-' + newUrl);
    } else if (first == 'health-news') {
      return ('newsarticles/newsarticles-' + newUrl);
    } else if (first == 'diabetesmine') {
      newUrl = modUrl.replace('/', '__');
      return ('partner_article/partner_article-' + newUrl);
    } else if (first == 'program') {
      return ('sponsoredprogram/sponsoredprogram-' + newUrl);
    }
    return '';
  }

  getContent(url) {
    var newUrl = this.modifyUrl(url);
    if (newUrl.length == 0) {
      return null;
    }
    var s3 = new AWS.S3();
    var bucketName = 'hl-json-data-prod';

    var key = 'data/hlcmsresource/articles/' + newUrl + '.json';
    console.log(key);
    var params = {Bucket: bucketName, Key: key};
    var fileObj = null;
    var done = false;

    return new Promise( function( resolve, reject ) {
      s3.getObject(params, function (err, data) {
        if (err) {
          reject('error' + err);
        }
        resolve( data );
      });
    });
  }

  async getS3Data(url) {
    var jsonBody = await this.getContent(url);
    return jsonBody;
  }
};

module.exports = S3Wrapper;
