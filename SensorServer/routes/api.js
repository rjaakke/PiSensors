var log4js = require('log4js');
var express = require('express');
var database = require('../lib/database');

/**
 * Read logfile configuration
 */
log4js.configure('config.json', {
  reloadSecs: 300
});

var database = new database();
var router = express.Router();
var logger = log4js.getLogger('Server.API');

/**
 * GET welcome message.
 */
router.get('/', function(req, res) {
  res.json({
    message: 'Welcome to Sensor API!'
  });
  logger.debug('Welcome to Sensor API!');
});

// SENSOR ROUTES
// =============================================================================
/**
 * Get all sensors from the Sensors Table
 */
router.route('/sensors').get(function(req, res, next) {
  database.getAllSensors(function(result) {
    if (result instanceof Error) {
      next(result);
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

/**
 * Get sensors from the Sensors Table
 */
router.route('/sensors/:id').get(function(req, res, next) {

  var sensorId = req.params.id;

  database.getSensorById(sensorId, function(result) {
    if (result instanceof Error) {
      next(result);
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

/**
 * Inserts a record in to the Sensors Table
 */
router.route('/sensors').post(function(req, res, next) {

  if (!req.body.hasOwnProperty('sensorinfo')) {
    var err = new Error('Error 400: Post syntax incorrect.');
    err.status = 400;
    next(err);
  } else {
    var sensorInfo = req.body.sensorinfo;

    database.createSensor(sensorInfo, function(result) {
      if (result instanceof Error) {
        next(result);
      } else {
        res.json(result);
        logger.info('New sensor created with id:', result);
      }
    });
  }
});

/**
 * Update a record in the Sensors Table
 */
router.route('/sensors').put(function(req, res, next) {

  if (!req.body.hasOwnProperty('sensorinfo')) {
    var err = new Error('Error 400: Post syntax incorrect.');
    err.status = 400;
    next(err);
  } else {
    var sensorInfo = req.body.sensorinfo;

    database.updateSensor(sensorInfo, function(result) {
      if (result instanceof Error) {
        next(result);
      } else {
        res.json(result);
        logger.info('Updated sensor with:', sensorInfo);
      }
    });
  }
});

/**
 * Sesnor Info route
 */
router.route('/info/:id').get(function(req, res, next) {

  var sensorId = req.params.id;

  database.getSensorInfo(sensorId, function(result) {
    if (result instanceof Error) {
      next(result);
    } else {
      if (result) {
        res.json(result);
        logger.debug('Sensor Info:' + JSON.stringify(result, null, 2));
      } else {
        res.json();
        logger.debug('No sensor registered with id:', sensorId);
      }
    }
  });
});


// SENSOREVENT ROUTES
// =============================================================================

/**
 * Returns a JSON object of the last 10 SensorEvents
 */
router.route('/sensorevents').get(function(req, res, next) {
  database.getSensorEvents(function(result) {
    if (result instanceof Error) {
      next(result);
    } else {
      if (result.length > 0) {
        res.json(result);
        logger.debug('SensorEvents:' + JSON.stringify(result, null, 2));
      } else {
        logger.debug('No events recorded!');
        res.json();
      }
    }
  });
});

/**
 * Returns a JSON object of the last 10 SensorEvents for the sesnsor id
 */
router.route('/sensorevents/:id').get(function(req, res, next) {

  var sensorId = req.params.id;

  database.getSensorEventsForId(sensorId, function(result) {
    if (result instanceof Error) {
      next(result);
    } else {
      if (result.length > 0) {
        res.json(result);
        logger.debug('SensorEvents:' + JSON.stringify(result, null, 2));
      } else {
        logger.debug('No events recorded for sensor with id:', sensorId);
        res.json();
      }
    }
  });
});

/**
 * Inserts a record in to the SensorEvents Table
 */
router.route('/sensorevents').post(function(req, res, next) {

  if (!req.body.hasOwnProperty('eventdata')) {
    var err = new Error('Error 400: Post syntax incorrect.');
    err.status = 400;
    next(err);
  } else {
    var eventData = req.body.eventdata;

    database.createSensorEvent(eventData, function(result) {
      if (result instanceof Error) {
        next(result);
      } else {
        res.json(result);
        logger.debug('New sensorevent created with id:', result);
      }
    });
  }
});

module.exports = router;