var database = require('../lib/database');
var express = require('express');
var log4js = require('log4js');

/**
 * Read logfile configuration
 */
log4js.configure('config.json', {
  reloadSecs: 300
});

var database = new database();
var router = express.Router();
var logger = log4js.getLogger('Server.Web');


/* GET home page. */
router.get('/', function(req, res, next) {

  var data = [];

  database.getSensorUsageForId(1, function(result) {
    if (result instanceof Error) {
      next(result);
    } else {
      if (result.length > 0) {
        result.forEach(function(row) {
          data.push('[new Date(' + row.Time + '),' + (3600000000 / ((1 / row.Volume) * 300000) * row.Usage) + ']');
        });

        res.render('index', {
          title: 'Home',
          data: '[' + data + ']'
        });

      } else {
        res.render('index', {
          title: 'Home',
          data: '[]'
        });
      }
    }
  });
});

/* GET info page. */
router.get('/info', function(req, res, next) {
  var data = [];

  database.getSensorInfo(1, function(result) {
    if (result instanceof Error) {
      next(result);
    } else {
      if (result) {
        res.render('info', {
          title: 'Current Usage',
          name: result.Name,
          units: result.Units,
          high: result.High,
          low: result.Low,
          total: result.Total,
          usage: parseInt(result.Usage),
          usagetime: result.UsageTime
        });

      } else {
        res.render('info', {
          title: 'No Sensors found',
          data: '[]'
        });
      }
    }
  });
});

/* GET settings page. */
router.get('/settings', function(req, res, next) {
  res.render('settings', {
    title: 'Settings'
  });
});

module.exports = router;