const mysql = require('mysql');
const deasync = require('deasync');

class MySQLWrapper {

  constructor() {
    this.db = null;
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
          return reject(err);
        }
        resolve(rows);
      });
    });
  }

  async getK1ValuePromise(conn, imuid) {
    var k1 = await this.getK1Helper(conn, imuid);
    var k1val = '';
    try {
      k1val = k1[0]['k1_value'];
    } catch (ex) {
    }
    return k1val;
  }

  async getK1Value(conn, imuid) {
    var k1 = await this.getK1ValuePromise(conn, imuid);
    return k1;
  }

  async getDistinctK1s(conn) {
    var rows = await new Promise(function(resolve, reject) {
      var query = 'select distinct k1 from classifier_weights order by k1';
      console.log(query);
      conn.query(query, function(err, rows) {
        if (err) {
          console.log(err);
          return [];//reject(err);
        }
        resolve(rows);
      });
    });

    var k1s = [];
    for (var i=0; i<rows.length; i++) {
      var k1 = rows[i]['k1'];
      k1s.push(k1);
    }
    return k1s;
  }

  async getWeights(conn, k1) {
    var rows = await this.getK1WeightsHelper(conn, k1);
    var k1_weights = {};
    for (var i=0; i<rows.length; i++) {
      var term = rows[i]['term'];
      var weight = rows[i]['weight'];
      k1_weights[term] = weight;
    }
    return k1_weights;
  }

  getK1WeightsHelper(conn, k1value) {
    return new Promise(function(resolve, reject) {
      var query = 'select term, weight from classifier_weights where k1="' + k1value + '"';
      console.log(query);
      conn.query(query, function (error, rows) {
        if (error) {
          console.log(error);
          return reject( error);
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
        if (error)
          return reject( error);
        resolve(rows);
      });
    });
  }

  async getK2ValueFromK1Promise(conn, k1value) {
    var k2 = await this.getK2Helper(conn, k1value);
    var k2val = '';
    try {
      k2val = k2[0]['k2_value'];
    } catch (ex) {
      }
    return k2val;
  }

  async getK2ValueFromK1(conn, k1) {
    var k2 = await this.getK2ValueFromK1Promise(conn, k1);
    return k2;
  }

};

module.exports = MySQLWrapper;
