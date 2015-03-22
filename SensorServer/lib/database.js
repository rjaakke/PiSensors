var sqlite3 = require('sqlite3').verbose();
var log4js = require('log4js');

var Database = function() {
  this.db = new sqlite3.Database('./data/sensordata.db');
  this.logger = log4js.getLogger();

  //Prepare the database if it doens't exist
  this.db.serialize(this.setupDatabase());
};


Database.prototype = {
  setupDatabase: function() {
    this.logger.info('Setting up database', this.db.filename);

    this.db.run(
      'CREATE TABLE if not exists Sensors (Id INTEGER PRIMARY KEY UNIQUE, Name STRING, Units STRING, High REAL, Low REAL, Volume REAL);' +
      'CREATE TABLE if not exists SensorEvents (Id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER UNIQUE, Rate INTEGER, FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `MinuteHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER UNIQUE, Rate INTEGER, Usage REAL, FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `HourHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER UNIQUE, Rate INTEGER, Usage REAL, FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `DayHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER UNIQUE, Rate INTEGER, Usage REAL, FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `MonthHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER UNIQUE, Rate INTEGER, Usage REAL, FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TRIGGER if not exists UpdateSensorLow AFTER INSERT ON SensorEvents WHEN NEW.Rate = 1 BEGIN UPDATE Sensors SET Low = Low + Volume where Id = NEW.SensorId; END;' +
      'CREATE TRIGGER if not exists UpdateSensorHigh AFTER INSERT ON SensorEvents WHEN NEW.Rate = 2 BEGIN UPDATE Sensors SET High = High + Volume where Id = NEW.SensorId; END;'
    );
  },

  /**
   * Returns all rows from the Sensors table.
   * @param {Function} callback Returns either null or the Error.
   */
  getAllSensors: function(callback) {
    var logger = this.logger;

    this.db.all('SELECT * FROM Sensors',
      function(err, rows) {
        if (err !== null) {
          logger.error('Error in method getAllSensors:', err);
          callback(err);
        } else {
          logger.trace(rows);
          callback(rows);
        }
      });
  },

  /**
   * Returns the sensor row for the id from the Sensors table..
   * @param {int}      sensorId The id of the sensors
   * @param {Function} callback Returns either the row or the Error.
   */
  getSensorById: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT * FROM Sensors WHERE Id = ?;');

    stmt.get(sensorId,
      function(err, row) {
        if (err !== null) {
          logger.error('Error in method getSensorById:', err);
          callback(err);
        } else {
          logger.trace(row);
          callback(row);
        }
      });
    stmt.finalize();
  },

  /**
   * Creates a sensor row in the Sensors table.
   * @param {sensorInfo} sensorInfo The information object containing the sensor details.
   * @param {Function}   callback   Returns either the id or the Error.
   */
  createSensor: function(sensorInfo, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('INSERT INTO Sensors (Id, Name, Units, High, Low, Volume) VALUES (?,?,?,?,?,?);');

    stmt.run(sensorInfo.id, sensorInfo.name, sensorInfo.units, sensorInfo.high, sensorInfo.low, sensorInfo.volume,
      function(err) {
        if (err !== null) {
          logger.error('Error in method createSensor:', err);
          callback(err);
        } else {
          logger.trace(this.lastID);
          callback(this.lastID);
        }
      });
    stmt.finalize();
  },

  /**
   * Updates a sensor row in the Sensor table.
   * @param {sensorInfo} sensorInfo The information object containing the sensor details.
   * @param {Function}   callback   Returns either the id or the Error.
   */
  updateSensor: function(sensorInfo, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('UPDATE Sensors SET Name = ?, Units = ?, High = ?, Low = ?, Volume = ? WHERE SensorId = ?;');

    stmt.run(sensorInfo.name, sensorInfo.units, sensorInfo.high, sensorInfo.low, sensorInfo.volume, sensorInfo.id,
      function(err) {
        if (err !== null) {
          logger.error('Error in method updateSensor:', err);
          callback(err);
        } else {
          logger.trace(this.lastID);
          callback(this.lastID);
        }
      });
    stmt.finalize();
  },

  /**
   * Returns a row with the current usage information for a sensor
   * @param {int}      sensorId The id of the sensors
   * @param {Function} callback Returns either the row or the Error.
   */
  getCurrentUsage: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT High, Low, High + Low AS Total, 3600000 / ((1/Volume) * (Time - (SELECT Time FROM SensorEvents WHERE SensorId = ?1 ORDER BY Id DESC LIMIT 1 OFFSET 1))) * 1000 AS Usage, DateTime(Time/1000, \'unixepoch\', \'localtime\') AS UsageTime FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id  WHERE SensorId = ?1 ORDER BY E.Id DESC LIMIT 1;');

    stmt.get(sensorId,
      function(err, row) {
        if (err !== null) {
          logger.error('Error in method getCurrentUsage:', err);
          callback(err);
        } else {
          logger.trace(row);
          callback(row);
        }
      });
    stmt.finalize();
  },

  /**
   * Returns the last 10 rows for the sensorId from the SensorEvents table.
   * @param {int}      sensorId The id of the sensors
   * @param {Function} callback Returns either the row or the Error.
   */
  getSensorEvents: function(callback) {
    var logger = this.logger;

    this.db.all('SELECT * FROM SensorEvents ORDER BY Id DESC LIMIT 10;',
      function(err, rows) {
        if (err !== null) {
          logger.error('Error in method getSensorEvents:', err);
          callback(err);
        } else {
          logger.trace(rows);
          callback(rows);
        }
      });
  },

  /**
   * Returns the last 10 rows for the sensorId from the SensorEvents table.
   * @param {int}      sensorId The id of the sensors
   * @param {Function} callback Returns either the row or the Error.
   */
  getSensorEventsForId: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT * FROM SensorEvents WHERE SensorId = ? ORDER BY Id DESC LIMIT 10;');

    stmt.all(sensorId,
      function(err, rows) {
        if (err !== null) {
          logger.error('Error in method getSensorEventsForId:', err);
          callback(err);
        } else {
          logger.trace(rows);
          callback(rows);
        }
      });
    stmt.finalize();
  },

  /**
   * Inserts a sensorevent row in the SensorEvents table.
   * @param {sensordata} sensorData The information object containing the sensor event details.
   * @param {Function}   callback   Returns either the id or the Error.
   */
  createSensorEvent: function(eventData, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('INSERT INTO SensorEvents (SensorId, Time, Rate) VALUES (?,?,?);');

    stmt.run(sensorInfo.id, sensorInfo.name, sensorInfo.units, sensorInfo.high, sensorInfo.low, sensorInfo.volume,
      function(err) {
        if (err !== null) {
          logger.error('Error in method createSensorEvent:', err);
          callback(err);
        } else {
          logger.trace(this.lastID);
          callback(this.lastID);
        }
      });
    stmt.finalize();
  },

  /** TODO: Document this
   * [getFirstSensorEvent description]
   * @param {[type]}   sensorId [description]
   * @param {Function} callback [description]
   */
  getFirstSensorEvent: function(sensorId, callback) {
    var insertStmt = this.db.prepare(
      'SELECT Min(Time) as Time FROM SensorEvents WHERE SensorId = $sensorId '
    );

    insertStmt.get({
        $sensorId: sensorId
      },
      function(err, row) {
        if (err !== null) {
          this.logger.error(err);
          callback(err);
        } else {
          // err is null if insertion was successful
          // TODO: Move result logic out of here
          if (row.Time) {
            this.logger.debug('Oldeste SensorEvent retrieved: ', moment(row.Time).format());
            callback(row.Time);
          } else {
            this.logger.debug('No rows in history table for sensor');
            callback(moment().valueOf()); // Return now so no rows are processed
          }
        }
      });
    insertStmt.finalize();
  },

  /** TODO: Document this
   * [getLastHistoryEvent description]
   * @param {[type]}   sensorId    [description]
   * @param {[type]}   historyType [description]
   * @param {Function} callback    [description]
   */
  getLastHistoryEvent: function(sensorId, historyType, callback) {

    var insertStmt = this.db.prepare(
      'SELECT Max(Time) as Time FROM ' + historyType +
      'History WHERE SensorId = $sensorId '
    );

    insertStmt.get({
        $sensorId: sensorId
      },
      function(err, row) {
        if (err !== null) {
          this.logger.error(err);
          callback(err);
        } else {
          // err is null if insertion was successful
          // TODO: Move result logic out of here
          if (row.Time) {
            this.logger.debug('Last history event retrieved: ', moment(row.Time).format());
            callback(row.Time);
          } else {
            this.logger.debug('No rows in history table for sensor');
            // Return now - 1 so rows are processed
            callback(moment().minutes(0).seconds(0).subtract(1, historyType + 's').valueOf());
          }
        }
      });
    insertStmt.finalize();
  },

  /** TODO: Document this
   * [cleanSensorEvents description]
   * @param {[type]} sensorId [description]
   * @param {[type]} from     [description]
   * @param {[type]} till     [description]
   */
  cleanSensorEvents: function(sensorId, from, till) {
    db.serialize(function() {
      var insertStmt = this.db.prepare(
        'INSERT INTO MinuteHistory (SensorId, Time, Rate, Usage) ' +
        'SELECT SensorId, CAST(AVG(Time) - (AVG(Time) % 60000) AS INTEGER) AS Time, Rate, SUM(Volume)AS Usage ' +
        'FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id ' +
        'WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till ' +
        'GROUP BY SensorId'
      );

      insertStmt.run({
          $sensorId: sensorId,
          $from: from.valueOf(),
          $till: till.valueOf()
        },
        function(err) {
          if (err !== null) {
            this.logger.error(err);
            callback(err);
          } else {
            // err is null if insertion was successful
            // TODO: Move result logic out of here
            this.logger.debug('Minute history updated with rowid: ', this.lastID);
          }
        });
      insertStmt.finalize();

      var deleteStmt = this.db.prepare(
        'DELETE From SensorEvents WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till'
      );

      deleteStmt.run({
          $sensorId: sensorId,
          $from: from.valueOf(),
          $till: till.valueOf()
        },
        function(err) {
          if (err !== null) {
            this.logger.error(err);
            callback(err);
          } else {
            // err is null if insertion was successful
            // TODO: Move result logic out of here
            this.logger.debug(
              'Sensorevents table cleand up for minute, rows affected: ',
              this.changes);
          }
        });
      deleteStmt.finalize();
    });
  },

  /** TODO: Document this
   * [updateHistoryEvents description]
   * @param {[type]}   sensorId [description]
   * @param {[type]}   from     [description]
   * @param {[type]}   till     [description]
   * @param {[type]}   source   [description]
   * @param {[type]}   dest     [description]
   * @param {Function} callback [description]
   */
  updateHistoryEvents: function(sensorId, from, till, source, dest, callback) {
    var insertStmt = this.db.prepare(
      'INSERT OR REPLACE INTO ' + dest + ' (SensorId, Time, Rate, Usage) ' +
      'SELECT SensorId, CAST(AVG(Time) - (AVG(Time) % ' + from.diff(till) + ') AS INTEGER) AS Time, Rate, SUM(Usage)AS Usage ' +
      'FROM ' + source +
      ' WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till ' +
      'GROUP BY SensorId'
    );

    insertStmt.run({
        $sensorId: sensorId,
        $from: from.valueOf(),
        $till: till.valueOf()
      },
      function(err) {
        if (err !== null) {
          this.logger.error(err);
          callback(err);
        } else {
          // err is null if insertion was successful
          // TODO: Move result logic out of here
          this.logger.debug('History table was updated with rowid: ', this.lastID);

          if (callback) {
            callback(this.lastID);
          }
        }
      });
    insertStmt.finalize();
  },

  /**
   * [deleteHistoryEvents description]
   * @param {[type]}   sensorId [description]
   * @param {[type]}   from     [description]
   * @param {[type]}   till     [description]
   * @param {[type]}   table    [description]
   * @param {Function} callback [description]
   */
  deleteHistoryEvents: function(sensorId, from, till, table, callback) {
    var deleteStmt = this.db.prepare(
      'DELETE From ' + table +
      ' WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till'
    );

    deleteStmt.run({
        $sensorId: sensorId,
        $from: from.valueOf(),
        $till: till.valueOf()
      },
      function(err) {
        if (err !== null) {
          this.logger.error(err);
          callback(err);
        } else {
          // err is null if insertion was successful
          // TODO: Move result logic out of here
          this.logger.debug(
            'Sensorevents table cleand up for minute, rows affected: ',
            this.changes);

          if (callback) {
            callback(this.lastID);
          }
        }
      });
    deleteStmt.finalize();
  }
};

module.exports = Database;