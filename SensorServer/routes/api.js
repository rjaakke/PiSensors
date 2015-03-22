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
  logger.debug('Welcome to Sensor API!');
});

// SENSOR ROUTES
// =============================================================================
//Get all sensors from the Sensors Table
router.route('/sensors').get(function(req, res, next) {
  database.getAllSensors(function(result) {
    if (result instanceof Error) {
      next(err);
    } else {
      if (result.length > 0) {
        res.json(result);
        logger.debug('Sensors:' + JSON.stringify(result, null, 2));
      } else {
        res.json({
          message: 'No sensors registered!'
        });
        logger.debug('No sensors registered!');
      }
    }
  });
});

//Get sensors from the Sensors Table
router.route('/sensors/:id').get(function(req, res, next) {

  var sensorId = req.params.id;

  database.getSensorById(sensorId, function(result) {
    if (result instanceof Error) {
      next(err);
    } else {
      if (result) {
        res.json(result);
        logger.debug('Sensor:' + JSON.stringify(result, null, 2));
      } else {
        logger.debug('No sensor registered with id:', sensorId);
        res.json();
      }
    }
  });
});

//Inserts a record in to the Sensors Table
router.route('/sensors').post(function(req, res, next) {

  if (!req.body.hasOwnProperty('sensorinfo')) {
    var err = new Error('Error 400: Post syntax incorrect.');
    err.status = 400;
    next(err);
  }

  var sensorInfo = req.body.sensorinfo;

  database.createSensor(sensorInfo, function(result) {
    if (result instanceof Error) {
      next(err);
    } else {
      res.json(result);
      logger.debug('New sensor created with id:', result);
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

  database.updateSensor(sensorInfo, function(result) {
    if (result instanceof Error) {
      next(err);
    } else {
      res.json(result);
      logger.debug('Updated sensor with id:', result);
    }
  });
});

/**
 * Current Usage route
 */
router.route('/now/:id').get(function(req, res, next) {

  var sensorId = req.params.id;

  database.getCurrentUsage(sensorId, function(result) {
    if (result instanceof Error) {
      next(err);
    } else {
      if (result) {
        res.json(result);
        logger.debug('Current usage:' + JSON.stringify(result, null, 2));
      } else {
        logger.debug('No sensor registered with id:', sensorId);
        res.json();
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