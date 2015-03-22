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

  getSensorById: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT * FROM Sensors WHERE Id = ?');

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

  createSensor: function(sensorInfo, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('INSERT INTO Sensors (Id, Name, Units, High, Low, Volume) VALUES (?,?,?,?,?,?)');

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

  updateSensor: function(sensorInfo, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('UPDATE Sensors SET Name = ?, Units = ?, High = ?, Low = ?, Volume = ? WHERE SensorId = ?');

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

  getCurrentUsage: function(sensorId, callback) {
    var logger = this.logger;

    var stmt = this.db.prepare('SELECT High, Low, High + Low AS Total, 3600000 / ((1/Volume) * (Time - (SELECT Time FROM SensorEvents WHERE SensorId = ?1 ORDER BY Id DESC LIMIT 1 OFFSET 1))) * 1000 AS Usage, DateTime(Time/1000, \'unixepoch\', \'localtime\') AS UsageTime FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id  WHERE SensorId = ?1 ORDER BY E.Id DESC LIMIT 1');

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