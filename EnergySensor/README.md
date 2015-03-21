# EnergySensor
Node.js app for reading an energy sensor with a TCRT5000 Reflective Optical Sensor module

##TODO
- Move App code to app.js
- Try to determine sensor noise on standard deviation,
- Implement debug package
- Maybe change log package

## Hardware setup
Connect the sensor module VVC and Grnd to the 3.3v and Grnd pins on the GPIO.
Connect the D0 (Digital Out) to pin 17 on the GPIO port of a Raspberry PI.

## Config file
The sensor app relies on a config file.
```json
{
  "id": 1,
  "name": "Electriciteit",
  "units": "kWh",
  "high": 0,
  "low": 0,
  "volume": 0.00266666666667
}
```
## Commandline parameters
- Log  
Use to specify the loglevel, default is info:
  - 0 __EMERGENCY__  system is unusable
  - 1 __ALERT__ action must be taken immediately
  - 2 __CRITICAL__ the system is in critical condition
  - 3 __ERROR__ error condition
  - 4 __WARNING__ warning condition
  - 5 __NOTICE__ a normal but significant condition
  - 6 __INFO__ a purely informational message
  - 7 __DEBUG__ messages to debug an application
- Url
Use to specify the URL of the sensor server, default is  
http://localhost:3141

## DeBouncing the signal
Connected to the Pi the sensor D0 signal seems to bounce quite bit and doesn't always return return High and Low when watching for both (raising and falling) events. I debounced this calculating the interval between the current event and the previous event. If the interval > 30 milliseconds we have a possible hit. Now we have collection of High and Low events that we can filter with the actual event. I did this by dividing the interval of the previous event with the interval of the current event. If the result < 1 we have a hit. Basically I ignored the signal state and used the timestamp of singal to determine the event.

```javascript
function readSensor(err, state) {
  var currentEvent = Moment();

  var currentInterval = currentEvent.diff(previousEvent);
  var intervalFactor = previousInterval / currentInterval;
  var date = new Date();
  var currentRate = date.getHours() <= 7 ? 1 : 2; //Rate 1 is Low and 2 is High

  if (currentInterval > 30) {
    if (intervalFactor < 1 && firstEvent === false) {

      var currentWatt = (3600000 / (375 * currentInterval) * 1000);

      log.debug('State = ' + state + ' Another roundtrip, time: ' +
        currentEvent.format() + ' interval : ' + currentInterval +
        'ms factor : ' + intervalFactor + ' rate is: ' + currentRate +
        ' current usage:' + currentWatt);

      var eventData = {
        sensordata: {
          id: sensorId,
          time: currentEvent.valueOf(),
          rate: currentRate
        }
      };

      log.debug('Posting: ' + JSON.stringify(eventData));

    } else if (firstEvent === true) {
      firstEvent = false;
      log.debug('Skipping first event to avoid getting noise in the results');
    } else {
      log.debug('End of the black stripe, we can drop this event: ' +
        currentInterval + 'ms');
    }

    previousEvent = currentEvent;
		previousInterval = currentInterval;
  } else {

    log.debug('Singal bounce ignored, interval: ' + currentInterval +
      'ms');
  }
}
```
