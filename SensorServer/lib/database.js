var sqlite3 = require('sqlite3').verbose();
var log4js = require('log4js');

/**
 * Read logfile configuration
 */
log4js.configure('config.json', {
  reloadSecs: 300
});

var Database = function() {
  this.db = new sqlite3.Database('./data/sensordata.db');
  this.logger = log4js.getLogger('Server.Database');

  //Prepare the database if it doens't exist
  this.db.serialize(this.setupDatabase());
};


Database.prototype = {
  setupDatabase: function() {
    this.logger.info('Setting up database', this.db.filename);

    this.db.exec(
      'CREATE TABLE if not exists Sensors (Id INTEGER PRIMARY KEY UNIQUE, Name STRING, Units STRING, High REAL, Low REAL, Volume REAL);' +
      'CREATE TABLE if not exists SensorEvents (Id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE, SensorId INTEGER, Time INTEGER, Rate INTEGER, UNIQUE(Time, SensorId), FOREIGN KEY(SensorId) REFERENCES Sensors(id));' +
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
  getSensorInfo: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT Name, Units, High, Low, High + Low AS Total, 3600000000 / ((1/Volume) * (Time - (SELECT Time FROM SensorEvents WHERE SensorId = ?1 ORDER BY Id DESC LIMIT 1 OFFSET 1))) AS Usage, DateTime(Time/1000, \'unixepoch\', \'localtime\') AS UsageTime FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id  WHERE SensorId = ?1 ORDER BY E.Id DESC LIMIT 1;');

    stmt.get(sensorId,
      function(err, row) {
        if (err !== null) {
          logger.error('Error in method getSensorInfo:', err);
          callback(err);
        } else {
          logger.trace('Row:', row);
          callback(row);
        }
      });
    stmt.finalize();
  },

  /**
   * Returns the last 24 hours usage information for a sensor in 5minute interval.
   * @param {int}      sensorId The id of the sensors
   * @param {Function} callback Returns either the row or the Error.
   */
  getSensorUsageForId: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT * FROM (SELECT CAST(Time - (Time % 300000) AS INTEGER) AS Time, Rate, COUNT(Volume) AS Usage, Volume FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id WHERE SensorId = ? GROUP BY CAST(Time - (Time % 300000) AS INTEGER) ORDER BY Time DESC LIMIT 288) ORDER BY TIME ASC;');

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
   * Deletes sensorevents for the specified sensor from before a certain moment.
   * @param {int}      sensorId    Id of the sernsor to delete the events for.
   * @param {string}   moment      Moment up to which events can be deleted.
   * @param {Function} callback    Return the number of affected rows or an Error object.
   */
  deleteSensorEventsHistory: function(sensorId, from, callback) {
    var logger = this.logger;

    var deleteStmt = this.db.prepare(
      'DELETE From SensorEvents WHERE SensorId = $sensorId AND Time < $from'
    );

    deleteStmt.run({
        $sensorId: sensorId,
        $from: from
      },
      function(err) {
        if (err !== null) {
          this.logger.error(err);
          callback(err);
        } else {
          if (err !== null) {
            logger.error('Error in method deleteSensorEventsHistory:', err);
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