var moment = require('moment');
var log4js = require('log4js');
var cronJob = require('cron').CronJob;

var CronJobs = function(sensorId) {
  this.db = new sqlite3.Database('./data/sensordata.db');
  this.logger = log4js.getLogger();

  // Register the Job and start them right away.
  this.historyCleanJob = new cronJob('00 00 00 1 */1 *', this.historyCleanJobDef, null, true);
};

CronJobs.prototype = {

  historyCleanJobDef: function() {
    /**
     * Cleanup SensorEvents for data older then 2 years?
     */

    //TODO: implment this

  }
};

module.exports = CronJobs;