var moment = require('moment');
var log4js = require('log4js');
var cronJob = require('cron').CronJob;

var CronJobs = function(sensorId) {
  this.db = new sqlite3.Database('./data/sensordata.db');
  this.logger = log4js.getLogger();

  // Register the Jobs and start them right away.
  this.processEventsJob = new cronJob('00 */10 * * * *', this.processEventsJobDef, null, true);
  this.historyCleanJob = new cronJob('00 01 00 */1 * *', this.historyCleanJobDef, null, true);
};

CronJobs.prototype = {

  processEventsJobDef: function() {
    var logger = this.logger;

    //TODO: Make it loop trough Sensor Table!!!!
    var sensorId = 1;

    loggger.debug('Cron Job running to clean up Event tables');

    getFirstSensorEvent(this.sensorId, function(result) {
      var from = moment(result).seconds(0);
      var now = moment().subtract(1, 'minutes').seconds(0); //Subtract 1 minute to prevent an empty table
      var diff = now.diff(from, 'minutes');

      this.loggger.debug('Number of minutes to process: ', diff);

      for (var i = 1; i <= diff; i++) {
        var till = moment(from).add(1, 'minutes');

        processSensorEvents(sensorId, from, till);

        from.add(1, 'minutes');
      }
    });
  },

  historyCleanJobDef: function() {
    /**
     * Cleanup MinuteTable Weekly
     * Cleanup HourTable Weekly
     */

  }
};

module.exports = CronJobs;