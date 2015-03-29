var express = require('express');
var router = express.Router();

/* GET home page. */
router.get('/', function(req, res, next) {
  res.render('index', {
    title: 'PiSensors'
  });
});

/* GET now page. */
router.get('/now', function(req, res, next) {
  res.render('now', {
    title: 'Current USage'
  });
});

module.exports = router;