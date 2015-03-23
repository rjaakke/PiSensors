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

    this.db.exec(
      'CREATE TABLE if not exists Sensors (Id INTEGER PRIMARY KEY UNIQUE, Name STRING, Units STRING, High REAL, Low REAL, Volume REAL);' +
      'CREATE TABLE if not exists SensorEvents (Id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER, Rate INTEGER, UNIQUE(Time, SensorId), FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `MinuteHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER, Rate INTEGER, Usage REAL, UNIQUE(Time, SensorId), FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `HourHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER, Rate INTEGER, Usage REAL, UNIQUE(Time, SensorId), FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
      'CREATE TABLE if not exists `DayHistory` (Id	INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER, Rate INTEGER, Usage REAL, UNIQUE(Time, SensorId), FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
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
          logger.trace('Rows:', rows);
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
          logger.trace('Row:', row);
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

    var stmt = this.db.prepare('INSERT INTO Sensors (Id, Name, Units, High, Low, Volume) VALUES (?,?,?,?,?,?)');

    stmt.run(sensorInfo.id, sensorInfo.name, sensorInfo.units, sensorInfo.high, sensorInfo.low, sensorInfo.volume,
      function(err) {
        if (err !== null) {
          logger.error('Error in method createSensor:', err);
          callback(err);
        } else {
          logger.trace('LastId:', this.lastID);
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

    var stmt = this.db.prepare('UPDATE Sensors SET Name = ?, Units = ?, High = ?, Low = ?, Volume = ? WHERE Id = ?;');

    stmt.run(sensorInfo.name, sensorInfo.units, sensorInfo.high, sensorInfo.low, sensorInfo.volume, sensorInfo.id,
      function(err) {
        if (err !== null) {
          logger.error('Error in method updateSensor:', err);
          callback(err);
        } else {
          logger.trace('LastId:', this.lastID);
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
          logger.trace('Row:', row);
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
          logger.trace('Rows:', rows);
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
          logger.trace('Rows:', rows);
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

    stmt.run(eventData.sensorId, eventData.time, eventData.rate,
      function(err) {
        if (err !== null) {
          logger.error('Error in method createSensorEvent:', err);
          callback(err);
        } else {
          logger.trace('LastId:', this.lastID);
          callback(this.lastID);
        }
      });
    stmt.finalize();
  },

  /**
   * Returns the first(oldest) sensorevent from the SensorEvent table.
   * @param {int}      sensorId Id of the sernsor to retrieve the event for.
   * @param {Function} callback Returns either unixepoch or an error.
   */
  getFirstSensorEvent: function(sensorId, callback) {
    var logger = this.logger;

    var insertStmt = this.db.prepare('SELECT Min(Time) as Time FROM SensorEvents WHERE SensorId = $sensorId ');

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

  /**
   * Processes SensorEvents for a given timespan into the MinuteHistory for a sensor.
   * SensorEvents are deleted after being processed..
   * @param {int}      sensorId Id of the sernsor to retrieve the event for.
   * @param {moment}   from     Start of the the timespan to process SensorEvent for.
   * @param {moment}   till     End of the the timespan to process SensorEvent for.
   * @param {Function} callback Returns the number off affected rows or an Error object.
   */
  processSensorEvents: function(sensorId, from, till, callback) {
    var logger = this.logger;
    var db = this.db;

    var insertStmt = this.db.prepare(
      'INSERT INTO MinuteHistory (SensorId, Time, Rate, Usage) ' +
      'SELECT SensorId, CAST(AVG(Time) - (AVG(Time) % 60000) AS INTEGER) AS Time, Rate, SUM(Volume)AS Usage ' +
      'FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id ' +
      'WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till ' +
      'GROUP BY SensorId'
    );

    insertStmt.run({
        $sensorId: this.sensorId,
        $from: this.from.valueOf(),
        $till: this.till.valueOf()
      },
      function(err) {
        if (err !== null) {
          logger.error('Error in method processSensorEvents during INSERT INTO MinuteHistory:', err);
          callback(err);
        } else {
          logger.trace('LastId:', this.lastID);

          var deleteStmt = this.db.prepare(
            'DELETE FROM SensorEvents WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till'
          );

          deleteStmt.run({
              $sensorId: this.sensorId,
              $from: this.from.valueOf(),
              $till: this.till.valueOf()
            },
            function(err) {
              if (err !== null) {
                logger.error('Error in method processSensorEvents during DELETE FROM SensorEvents:', err);
                callback(err);
              } else {
                logger.trace('Changes:', this.changes);
                callback(this.changes);
              }
            });
          deleteStmt.finalize();

          //TODO: Update History Tables as well.
        }
      });
    insertStmt.finalize();
  },

  /**
   * Deletes history events from the specified HistoryType between a given timespan and sensor.
   * @param {int}      sensorId    Id of the sernsor to delete the events for.
   * @param {[type]}   moment      Moment up to which events can be deleted.
   * @param {string}   historyType Possible values: MinuteHistory, HourHistory.
   * @param {Function} callback    Return the number of affected rows or an Error object.
   */
  deleteHistoryEvents: function(sensorId, from, historyType, callback) {
    var logger = this.logger;

    if (historyType !== 'MinuteHistory' || historyType !== 'HourHistory') {
      var err = new Error('Incorrect historyType!');
      logger.error(err);
      callback(err);
    }

    var deleteStmt = this.db.prepare(
      'DELETE From ' + historyType + ' WHERE SensorId = $sensorId AND Time < $from'
    );

    deleteStmt.run({
        $sensorId: sensorId,
        $from: from.valueOf()
      },
      function(err) {
        if (err !== null) {
          this.logger.error(err);
          callback(err);
        } else {
          if (err !== null) {
            logger.error('Error in method deleteHistoryEvents:', err);
            callback(err);
          } else {
            logger.trace('Changes:', this.changes);

            if (callback) {
              callback(this.changes);
            }
          }
        }
      });
    deleteStmt.finalize();
  }
};

module.exports = Database;