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

  getFirstEvent: function(sensorId, callback) {

    var insertStmt = db.prepare(
      'SELECT Min(Time) as Time FROM SensorEvents WHERE SensorId = $sensorId '
    );

    insertStmt.get({
        $sensorId: sensorId
      },
      function(err, row) {
        if (err !== null) {
          // Express handles errors via its next function.
          loggger.error(err);
        } else {
          // err is null if insertion was successful
          if (row.Time) {
            loggger.debug('Oldeste SensorEvent retrieved: ', moment(row.Time).format());
            callback(row.Time);
          } else {
            loggger.debug('No rows in history table for sensor');
            callback(moment().valueOf()); // Return now so no rows are processed
          }
        }
      });
    insertStmt.finalize();
  },

  getLastHistoryEvent: function(sensorId, historyType, callback) {

    var insertStmt = db.prepare(
      'SELECT Max(Time) as Time FROM ' + historyType +
      'History WHERE SensorId = $sensorId '
    );

    insertStmt.get({
        $sensorId: sensorId
      },
      function(err, row) {
        if (err !== null) {
          // Express handles errors via its next function.
          loggger.error(err);
        } else {
          // err is null if insertion was successful
          if (row.Time) {
            loggger.debug('Last history event retrieved: ', moment(row.Time).format());
            callback(row.Time);
          } else {
            loggger.debug('No rows in history table for sensor');
            // Return now - 1 so rows are processed
            callback(moment().minutes(0).seconds(0).subtract(1, historyType + 's').valueOf());
          }
        }
      });
    insertStmt.finalize();
  },

  cleanSensorEvents: function(sensorId, from, till) {
    db.serialize(function() {
      var insertStmt = db.prepare(
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
            // Express handles errors via its next function.
            loggger.error(err);
          } else {
            // err is null if insertion was successful
            loggger.debug('Minute history updated with rowid: ', this.lastID);
          }
        });
      insertStmt.finalize();

      var deleteStmt = db.prepare(
        'DELETE From SensorEvents WHERE SensorId = $sensorId AND Time BETWEEN $from AND $till'
      );

      deleteStmt.run({
          $sensorId: sensorId,
          $from: from.valueOf(),
          $till: till.valueOf()
        },
        function(err) {
          if (err !== null) {
            // Express handles errors via its next function.
            loggger.error(err);
          } else {
            // err is null if insertion was successful
            loggger.debug(
              'Sensorevents table cleand up for minute, rows affected: ',
              this.changes);
          }
        });
      deleteStmt.finalize();
    });
  },

  updateHistory: function(sensorId, from, till, source, dest, callback) {
    var insertStmt = db.prepare(
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
          // Express handles errors via its next function.
          loggger.error(err);
        } else {
          // err is null if insertion was successful
          loggger.debug('History table was updated with rowid: ', this.lastID);

          if (callback) {
            callback(this.lastID);
          }
        }
      });
    insertStmt.finalize();
  },

  deleteHistory: function(sensorId, from, till, table, callback) {
    var deleteStmt = db.prepare(
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
          // Express handles errors via its next function.
          loggger.error(err);
        } else {
          // err is null if insertion was successful
          loggger.debug(
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