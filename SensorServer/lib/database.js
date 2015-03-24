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
      'CREATE TRIGGER if not exists UpdateSensorHigh AFTER INSERT ON SensorEvents WHEN NEW.Rate = 2 BEGIN UPDATE Sensors SET High = High + Volume where Id = NEW.SensorId; END;' +
      'CREATE TRIGGER if not exists UpdateHistory AFTER INSERT ON SensorEvents ' +
      'BEGIN' +
      ' INSERT OR REPLACE INTO MinuteHistory (SensorId, Time, Rate, Usage)' +
      '  SELECT NEW.SensorId, NEW.Time - (NEW.TIme % 60000) AS Time, NEW.Rate as Rate,' +
      '  COALESCE((SELECT USAGE FROM MinuteHistory WHERE SensorID = NEW.SensorId AND Time = NEW.Time - (NEW.Time % 60000)), 0) + Volume FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id;' +
      ' INSERT OR REPLACE INTO HourHistory (SensorId, Time, Rate, Usage)' +
      '  SELECT NEW.SensorId, NEW.Time - (NEW.TIme % 3600000) AS Time, NEW.Rate as Rate,' +
      '  COALESCE((SELECT USAGE FROM MinuteHistory WHERE SensorID = NEW.SensorId AND Time = NEW.Time - (NEW.Time % 3600000)), 0) + Volume FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id;' +
      ' INSERT OR REPLACE INTO DayHistory (SensorId, Time, Rate, Usage)' +
      '  SELECT NEW.SensorId, NEW.Time - (NEW.TIme % 86400000) AS Time, NEW.Rate as Rate,' +
      '  COALESCE((SELECT USAGE FROM MinuteHistory WHERE SensorID = NEW.SensorId AND Time = NEW.Time - (NEW.Time % 86400000)), 0) + Volume FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id;' +
      'END;'
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
   * Deletes history events from the specified HistoryType and sensor from before a certain moment.
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