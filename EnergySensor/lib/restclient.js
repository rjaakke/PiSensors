/**
 * Module dependencies.
 */
var restler = require('restler');
var log4js = require('log4js');

/**
 * Read logfile configuration
 */
log4js.configure('config.json', {
  reloadSecs: 300
});

/**
 * RestClient constructor
 * @param {string} url The URL that is used to perfrom all REST operations.
 */
var RestClient = function(url) {
  this.url = url;
  this.logger = log4js.getLogger('RestClient');

  this.logger.info('Setting up client:', url);
};

RestClient.prototype = {
  /**
   * validateUrl validates the Url by perfoming e GET request
   * @param {Error} callback Returns either null or the Error.
   */
  validateUrl: function(callback) {
    var logger = this.logger;

    // Check if the sensorserver is running
    restler.get(this.url + '/api').on('complete', function(result) {
      if (result instanceof Error) {
        logger.error('HTTP GET failed:', result.message);
        callback(result);
      } else {
        logger.info(result);
        callback(null);
      }
    });
  },

  /**
   * updateSensor creates or updates the Sensor and sets the sensor parameters.
   * @param {SensorInfo} sensorInfo Object containing all sensor information.
   * @param {Error}      callback   Returns either null or the Error.
   */
  updateSensor: function(sensorInfo, callback) {
    var logger = this.logger;
    var url = this.url;

    restler.get(url + '/api/sensors/' + sensorInfo.id).on('complete',
      function(data, result) {
        if (result instanceof Error) {
          logger.error('HTTP GET failed:', result.message);
          callback(result);
        } else {
          if (data) {
            logger.info('Sensor allready registered!');

            log.trace('Submitting sensorinfo:', sensorInfo);
            restler.putJson(url + '/api/sensors', {
              sensorinfo: sensorInfo
            }).on('complete',
              function(data, response) {
                if (data instanceof Error) {
                  logger.error('HTTP POST failed:', data);
                  callback(response);
                } else if (response.statusCode != 200) {
                  logger.error('HTTP POST failed:', response.statusCode);
                  callback(response);
                } else {
                  logger.info('Sensor updated with:\n', sensorInfo);
                  callback(null);
                }
              });
          } else {
            logger.info("No sensor was registered, registering sensor now.");

            log.trace('Submitting sensorinfo:', sensorInfo);
            restler.postJson(url + '/api/sensors', {
              sensorinfo: sensorInfo
            }).on('complete',
              function(data, response) {
                if (data instanceof Error) {
                  logger.error('HTTP POST failed:', data);
                  callback(response);
                } else if (response.statusCode != 200) {
                  logger.error('HTTP POST failed:', response.statusCode);
                  callback(response);
                } else {
                  logger.info('Sensor registered with id:', data.toString());
                  callback(null);
                }
              });
          }
        }
      });
  },

  /**
   * postEventData sends sensor data JSON object as a POST request to the SensorServer.
   * @param {eventData} eventData Object containing all data for this event.
   * @param {Function}  callback  Returns either null or the Error.
   */
  postEventData: function(eventData, callback) {
    var logger = this.logger;

    log.trace('Submitting eventdata:', eventData);
    restler.postJson(this.url + '/api/sensorevents', {
      eventdata: eventData
    }).on('complete',
      function(data, response) {
        if (data instanceof Error) {
          logger.error('HTTP POST failed:', data);
          callback(response);
        } else if (response.statusCode != 200) {
          logger.error('HTTP POST failed:', response.statusCode);
          callback(response);
        } else {
          logger.debug('Server responded with:', data.toString());
          callback(null);
        }
      });
  }

};

module.exports = RestClient;