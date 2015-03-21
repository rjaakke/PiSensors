var log4js = require('log4js');
var express = require('express');
var database = require('../lib/database');

var database = new database();
var router = express.Router();
var logger = log4js.getLogger();

/* GET welcome message. */
router.get('/', function(req, res) {
  res.json({
    message: 'Welcome to Sensor API!'
  });
});

// SENSOR ROUTES
// =============================================================================
//Inserts a record in to the Sensors Table
router.route('/sensors').post(function(req, res, next) {

  if (!req.body.hasOwnProperty('sensorinfo')) {
    var err = new Error('Error 400: Post syntax incorrect.');
    err.status = 400;
    next(err);
  }

  var sensorInfo = req.body.sensorinfo;

  var stmt = database.db.prepare(
    'INSERT INTO Sensors (Id, Name, Units, High, Low, Volume) VALUES (?,?,?,?,?,?)'
  );
  stmt.run(sensorInfo.id, sensorInfo.name, sensorInfo.units, sensorInfo.high,
    sensorInfo.low, sensorInfo.volume,
    function(err) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        // err is null if insertion was successful
        logger.debug('New sensor created with id: ', this.lastID);
        res.json(this.lastID);
      }
    });
  stmt.finalize();
});

//Get all sensors from the Sensors Table
router.route('/sensors').get(function(req, res, next) {
  database.db.all('SELECT * FROM Sensors',
    function(err, rows) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        if (rows.length > 0) {
          logger.debug('Getting all sensors: ' + JSON.stringify(rows));
          res.json(rows);
        } else {
          logger.debug('No sensors registered!');
          res.json({
            message: 'No sensors registered!'
          });
        }
      }
    });
});

//Update a record in the Sensors Table
router.route('/sensors').put(function(req, res, next) {

  if (!req.body.hasOwnProperty('sensorinfo')) {
    var err = new Error('Error 400: Post syntax incorrect.');
    err.status = 400;
    next(err);
  }

  var sensorInfo = req.body.sensorinfo;

  //TODO: Move code to Database
  var stmt = database.db.prepare(
    'UPDATE Sensors SET Name = ?, Units = ?, High = ?, Low = ?, Volume = ?'
  );
  stmt.run(sensorInfo.name, sensorInfo.units, sensorInfo.high, sensorInfo.low,
    sensorInfo.volume,
    function(err) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        // err is null if insertion was successful
        logger.info('Sensor was updated with values: %s %s',
          sensorInfo.name, sensorInfo.units);
        res.json({
          message: 'Sensor was updated'
        });
      }
    });
  stmt.finalize();
});

//Get sensors from the Sensors Table
router.route('/sensors/:id').get(function(req, res, next) {
  if (req.params.id < 0) {
    var err = new Error('Error 404: id parameter missing');
    err.status = 404;
    next(err);
  }

  //TODO: Move code to Database
  database.db.get('SELECT * FROM Sensors WHERE Id = ' + req.params.id,
    function(err, row) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        if (row) {
          logger.debug('Getting sensors: ' + JSON.stringify(row));
          res.json(row);
        } else {
          logger.debug('No sensor registered!');
          res.json();
        }
      }
    });
});

/**
 * Current Usage route
 */
router.route('/sensors/:id/now')
  //Get sensors from the Sensors Table
  .get(function(req, res, next) {
    if (req.params.id < 0) {
      var err = new Error('Error 404: id parameter missing');
      err.status = 404;
      next(err);
    }

    //TODO: Move code to Database
    database.db.get(
      'SELECT High, Low, High + Low AS Total, 3600000 / ((1/Volume) * (Time - (SELECT Time FROM SensorEvents ORDER BY Id DESC LIMIT 1 OFFSET 1))) * 1000 AS Usage, DateTime(Time/1000, \'unixepoch\', \'localtime\') AS UsageTime FROM SensorEvents AS E JOIN Sensors AS S ON E.SensorId = S.Id ORDER BY E.Id DESC LIMIT 1',
      function(err, row) {
        if (err !== null) {
          // Express handles errors via its next function.
          next(err);
        } else {
          if (row) {
            logger.debug('Getting most recent usage: ' + JSON.stringify(row));
            res.json(row);
          } else {
            logger.debug('No sensorevents found!');
            res.json({
              message: 'No sensorevents found!'
            });
          }
        }
      });
  });


// SENSOREVENT ROUTES
// =============================================================================

/**
 * Inserts a record in to the SensorEvents Table
 */
router.route('/sensorevents').post(function(req, res, next) {

  if (!req.body.hasOwnProperty('sensordata')) {
    res.statusCode = 400;
    return res.send('Error 400: Post syntax incorrect.');
  }

  var sensorData = req.body.sensordata;

  //TODO: Move code to Database
  var stmt = database.db.prepare(
    'INSERT INTO SensorEvents (SensorId, Time, Rate) VALUES (?,?,?);'
  );
  stmt.run(sensorData.id, sensorData.time, sensorData.rate,
    function(err) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        // err is null if insertion was successful
        logger.debug('New sensorevent created with id: ', this.lastID);
        res.json(this.lastID);
      }
    });
  stmt.finalize();
});

//Returns a JSON object of the last SensorEvents
router.route('/sensorevents').get(function(req, res, next) {

  //TODO: Move code to Database
  database.db.all('SELECT * FROM SensorEvents ORDER BY Id DESC LIMIT 10', req.params
    .id,
    function(err, rows) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        logger.debug('Event rows: ' + JSON.stringify(rows));
        res.json(rows);
      }
    });
});

//Returns a JSON object of the last SensorEvents for the specified SensorId
router.route('/sensorevents/:id').get(function(req, res, next) {
  if (req.params.id < 0) {
    var err = new Error('Error 404: id parameter missing');
    err.status = 404;
    next(err);
  }

  //TODO: Move code to Database
  database.db.all(
    'SELECT * FROM SensorEvents WHERE SensorId = ' + req.params.id +
    ' ORDER BY Id DESC LIMIT 100',
    function(err, rows) {
      if (err !== null) {
        // Express handles errors via its next function.
        next(err);
      } else {
        logger.debug('Event rows: ' + JSON.stringify(rows));
        res.json(rows);
      }
    });
});

module.exports = router;