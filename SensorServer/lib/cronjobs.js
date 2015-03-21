var moment = require('moment');
var log4js = require('log4js');
var cronJob = require('cron').CronJob;

var logger = log4js.getLogger();

// Register the Cron Job
// =============================================================================
var sensorEventsJob = new cronJob('00 */10 * * * *', function() {

    //TODO: Make it loop trough Sensor Table!!!!

    var sensorId = 1;

    loggger.debug('Cron Job running to clean the SensorEvents tables');

    getOldestEvent(sensorId, function(result) {
      var from = moment(result).seconds(0);
      var now = moment().subtract(1, 'minutes').seconds(0); //Subtract 1 minute te prevent an empty table
      var diff = now.diff(from, 'minutes');

      loggger.debug('Number of minutes to process: ', diff);

      for (var i = 1; i <= diff; i++) {
        var till = moment(from).add(1, 'minutes');

        cleanSensorEvents(sensorId, from, till);

        from.add(1, 'minutes');
      }
    });
  },
  null,
  true /* Start the job right now */
);

var historyJob = new cronJob('00 01 */1 * * *', function() {

    //TODO: Make it loop trough Sensor Table!!!!

    var sensorId = 1;

    loggger.debug('Cron Job running to update History');

    historyJobDef(sensorId, 'hour', 'MinuteHistory', 'HourHistory');
    historyJobDef(sensorId, 'day', 'HourHistory', 'DayHistory');
    historyJobDef(sensorId, 'month', 'DayHistory', 'MonthHistory');
  },
  null,
  true /* Start the job right now */
);

function historyJobDef(sensorId, type, source, dest) {
  getLastHistoryEvent(sensorId, type, function(result) {
    var from = moment(result).seconds(0);
    var now = moment().minutes(0).seconds(0);
    var diff = now.diff(from, type);

    loggger.debug('Number of ' + type + 's to process: ', diff);

    for (var i = 1; i <= diff; i++) {
      var till = moment(from).add(1, type + 's');

      updateHistory(sensorId, from, till, source,
        dest);

      from.add(1, type + 's');
    }
  });
}