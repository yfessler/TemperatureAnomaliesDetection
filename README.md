# Temperature Anomalies Detection
Flink example for Temperature Anomalies Detection simulation 

This example detect anomalies in the temperature measurements of an imaginary IOT devices (Steam is simulate by an endless data source)
The filnk job get the temprature measurement of a device which generates 100 temperature measurements in 1 minute (can be overriden by command line)

Anomaly is defined if a measurement is 3 standard deviations away from the 1 minute average.

## How to execute:
1. install Flink
2. build the project as jar artifact 
3. execute the job as follows:
   at the flink folder:
   bin/flink run -c TemperatureAnomalyDetection TemperatureAnomalyDetection.jar 
   optional params:
   Number of Devices (default: 200): --numOfDevices 100
   Measurements per minute (default: 100): --measurementsPerMinute 100
   Window Size in seconds (default 60): --windowSize 60
   
   for the out put type the follow command in a different terminal:
   tail -f log/flink-*-jobmanager-*.out
   
