const mysql = require('mysql');
const deasync = require('deasync');
var fs = require("fs");

class MySQLWrapper {

  constructor() {
    this.db = null;
  }

  getDatabaseConfig(filename) {
    var config = {};
    var data = fs.readFileSync(filename, 'utf-8');
    config = JSON.parse(data);
    return config;
  }

  async getConnection(configJson) {
    var connection = null;
    var connected = false;
    try {
      connection = mysql.createConnection({
        host : configJson['host'],
        user : configJson['user'],
        password : configJson['password'],
        database : configJson['database'],
      });

      connected = this.doConnectSync(connection);

      if (connected == false) {
        connection = null;
      }
    } catch (exception) {
      connection = null;
    }
    return connection;
  }

  doConnectSync(conn) {
    var done = false;
    var status = '';
    conn.connect(function(err) {
      if (err) {
        status = 'error connecting: ' + err.stack;
      } else {
        status = 'connected as id ' + conn.threadId;
      }
      done = true;
      console.log(status);
    });
    deasync.loopWhile(function(){return !done;});
  }

  closeConnect(conn) {
    conn.end();
  }

  getK1Helper(conn, imuid) {
    return new Promise(function(resolve, reject) {
      var query = 'select k1_value from tax_imuid_k1 where tax_imuid = ' + imuid;
      console.log(query);
      conn.query(query, function(err, rows) {
        if (err) {
          reject( error);
          console.log(err);
          return null;
        }
        resolve(rows);
      });
    });
  }

  async getK1Value(conn, imuid) {
    var k1val = await this.getK1Helper(conn, imuid);
    var k1 = '';
    if (k1val != null) {
      try {
        k1 = k1val[0]['k1_value'];
      } catch (ex) {
      }
    }
    return k1;
  }

  async getDistinctK1s(conn) {
    var rows = await new Promise(function(resolve, reject) {
      var query = 'select distinct k1 from classifier_weights order by k1';
      console.log(query);
      conn.query(query, function(err, rows) {
        if (err) {
          reject( error);
          console.log(err);
          return [];
        }
        resolve(rows);
      });
    });

    var k1s = [];
    for (var i=0; i<rows.length; i++) {
      var k1 = rows[i]['k1'];
      if (k1 == 'idf') {
        continue;
      }
      k1s.push(k1);
    }
    return k1s;
  }

  async getWeights(conn, k1s_str) {
    var rows = await this.getK1WeightsHelper(conn, k1s_str);
    var k1_weights = {};
    for (var i=0; i<rows.length; i++) {
      var k1_col = rows[i]['k1'];
      var term = rows[i]['term'].replace(/\\\"/g, "");
      var weight = rows[i]['weight'];
      if (!k1_weights.hasOwnProperty(k1_col)) {
        k1_weights[k1_col] = {};
      }
      k1_weights[k1_col][term] = weight;
    }
    return k1_weights;
  }

  getK1WeightsHelper(conn, k1value) {
    return new Promise(function(resolve, reject) {
      //var query = 'select term, weight from classifier_weights where k1="' + k1value + '"';
      var query = 'select k1, term, weight from classifier_weights where k1 in ' + k1value;
      //console.log(query);
      conn.query(query, function (error, rows) {
        if (error) {
          reject( error);
          console.log(error);
          return {};
        }
        resolve(rows);
      });
    });
  }

  getK2Helper(conn, k1value) {
    return new Promise(function(resolve, reject) {
      var query = 'select k2_value from tax_k1_to_k2 where tax_k1_value = "' + k1value + '"';
      console.log(query);
      conn.query(query, function (error, rows) {
        if (error) {
          reject( error);
          console.log(error);
          return null;
        }
        resolve(rows);
      });
    });
  }

  async getK2ValueFromK1(conn, k1) {
    var k2val = await this.getK2Helper(conn, k1);
    var k2 = '';
    if (k2val != null) {
      try {
        k2 = k2val[0]['k2_value'];
      } catch (ex) {
      }
    }
    return k2;
  }

  getImuidsHelper(conn, k1value) {
    return new Promise(function(resolve, reject) {
      var query = 'select distinct tax_imuid from tax_imuid_k1_copy where k1_value = "' + k1value + '" order by tax_imuid';
      console.log(query);
      conn.query(query, function (error, rows) {
        if (error) {
          reject( error);
          console.log(error);
          return [];
        }
        resolve(rows);
      });
    });
  }

  async getImuidsForK1(conn, k1) {
    var rows = await this.getImuidsHelper(conn, k1);
    var imuids = [];
    for (var i=0; i<rows.length; i++) {
      var imuid = rows[i]['tax_imuid'];
      imuids.push(imuid);
    }
    return imuids;
  }
};

module.exports = MySQLWrapper;
