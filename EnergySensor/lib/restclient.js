/**
 * Module dependencies.
 */
var restler = require('restler');
var log4js = require('log4js');

/**
 * [RestClient constructor]
 * @param {[string]} url [is the URL that is used to perfrom all REST operations.]
 */
var RestClient = function(url) {
  this.url = url;
  this.logger = log4js.getLogger();

  this.logger.info('Setting up client:', url);
};

RestClient.prototype = {
  /**
   * [validateUrl validates the Url by perfoming e GET request]
   * @param {Error} callback [Returns either null or the Error.]
   */
  validateUrl: function(callback) {
    var caller = this;
    // Check if the sensorserver is running
    restler.get(this.url).on('complete', function(result) {
      if (result instanceof Error) {
        callback(result);
      } else {
        caller.logger.info(result);
        callback(null);
      }
    });
  },

  /**
   * [updateSensor creates or updates the Sensor and sets the sensor parameters.]
   * @param {[SensorInfo]}   sensorInfo [Sensor info object containing all sensor information.]
   * @param {Error} callback [Returns either null or the Error.]
   */
  updateSensor: function(sensorInfo, callback) {
    var caller = this;
    restler.get(caller.url + '/api/sensors/' + sensorInfo.sensorId).on('complete',
      function(data, result) {
        if (result instanceof Error) {
          caller.logger.error('HTTP GET failed:', result.message);
          callback(result);
        } else {
          if (data) {
            caller.logger.info('Sensor allready registered!');

            restler.putJson(caller.url + '/api/sensors', {
              sensorInfo: sensorInfo
            }).on('complete',
              function(data, response) {
                if (response instanceof Error) {
                  caller.logger.error('HTTP POST failed:', response.message);
                  callback(response);
                } else {
                  caller.logger.info('Sensor updated with:\n', sensorInfo);
                  callback(null);
                }
              });
          } else {
            caller.logger.info("No sensor was registered, registering sensor now.");

            restler.postJson(caller.url + '/api/sensors', {
              sensorInfo: sensorInfo
            }).on('complete',
              function(data, response) {
                if (response instanceof Error) {
                  caller.logger.error('HTTP POST failed:', respone.message);
                  callback(response);
                } else {
                  caller.logger.info('Sensor registered with id:', data.toString());
                  callback(null);
                }
              });
          }
        }
      });
  },

  /**
   * [postEventData sends sensor data JSON object as a POST request to the SensorServer.]
   * @param {[type]}   eventData [description]
   * @param {Function} callback  [description]
   */
  postEventData: function(eventData, callback) {
    var caller = this;
    restler.postJson(this.url + '/api/sensorevents', {
      eventData: eventData
    }).on('complete',
      function(data, response) {
        if (response instanceof Error) {
          caller.logger.error('HTTP POST failed:', respone.message);
          callback(response);
        } else {
          caller.logger.debug('Server responded with:', data.toString());
          callback(null);
        }
      });
  }

};

module.exports = RestClient;