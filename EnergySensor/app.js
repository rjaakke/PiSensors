/**
 * Module dependencies.
 */
var moment = require('moment');
var log4js = require('log4js');
//TODO Uncomment
//var GPIO = require('onoff').Gpio;

/**
 * [App constructor]
 * @param {[SensorInfo]} sensorInfo [The SensorInfo object containing all sensor values.]
 */
var App = function(sensorInfo) {
  // sensor is attached to pin 17
  this.sensor = new GPIO(17, 'in', 'both', {
    debounceTimeout: 0
  });

  this.sensorInfo = sensorInfo;
  this.logger = log4js.getLogger();
  this.previousEvent = moment();
  this.firstEvent = true;
  this.previousInterval = 0;

  this.onSensorEvent = null;
};

App.prototype = {
  watch: function(callback) {
    if (callback && typeof(callback) === "function") {
      this.onSensorEvent = callback;
      this.sensor.watch(this.sensorEventHandler.bind(this));
    } else {
      this.logger.warn('No callback supplied, aborted watching!');
    }
  },

  unwatch: function() {
    this.sensor.unwatch(this.sensorEventHandler);
  },

  sensorEventHandler: function(err, state) {
    var currentEvent = moment();

    var currentInterval = currentEvent.diff(this.previousEvent);
    var intervalFactor = this.previousInterval / currentInterval;
    var date = new Date();
    var currentRate = date.getHours() <= 7 ? 1 : 2; //Rate 1 is Low and 2 is High

    if (currentInterval > 30) {
      if (intervalFactor < 1 && this.firstEvent === false) {

        //TODO: 375 should be the C var
        var currentWatt = (3600000 / (375 * currentInterval) * 1000);

        this.logger.debug('State = ' + state + ' Another roundtrip, time: ' +
          currentEvent.format() + ' interval : ' + currentInterval +
          'ms factor : ' + intervalFactor + ' rate is: ' + currentRate +
          ' current usage:' + currentWatt);

        this.onSensorEvent({
          sensorId: this.sensorInfo.sensorId,
          time: currentEvent.valueOf(),
          rate: currentRate
        }, function(err) {
          if (err instanceof Error) {
            this.logger.warm('Event logging failed!');
          }
        }).bind(this);

      } else if (this.firstEvent === true) {
        this.firstEvent = false;
        this.logger.debug('Skipping first event to avoid getting noise in the results');

      } else {
        this.logger.debug('End of the black stripe, we can drop this event: ' +
          currentInterval + 'ms');
      }

      this.previousEvent = currentEvent;
      this.previousInterval = currentInterval;
    } else {

      this.logger.trace('Singal bounce ignored, interval: ' + currentInterval + 'ms');
    }
  }
};

module.exports = App;