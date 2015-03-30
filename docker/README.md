# Docker support

The files in this directory allow you to build a docker image.  The soda fountain assembly must be 
copied to `soda-fountain-assembly.jar` in this directory before building.

## Required Runtime Variables

* `SODA_FOUNTAIN_DB_HOST` - Soda Fountain DB hostname
* `SODA_FOUNTAIN_DB_PASSWORD_LINE` - Full line of config for Soda Fountain DB password.  Designed to be either `password = "foo"` or `include /path/to/file`.
* `ZOOKEEPER_ENSEMBLE` - The zookeeper cluster to talk to, in the form of `["10.0.0.1:2181", "10.0.0.2:2818"]`

## Optional Runtime Variables

See the Dockerfile for defaults.

* `ARK_HOST` - The IP address of the host of the docker container, used for service advertisements.
* `BALBOA_ACTIVEMQ_URI` - URI for Balboa metrics activemq, eg. `failover:tcp://10.1.0.28:61616,tcp://10.1.0.29:61616`
* `BALBOA_JMS_QUEUE` - Queue name for Balboa metrics
* `ENABLE_GRAPHITE` - Should various metrics information be reported to graphite
* `GRAPHITE_HOST` - The hostname or IP of the graphite server, if enabled
* `GRAPHITE_PORT` - The port number for the graphite server, if enabled
* `JAVA_XMX` - Sets the -Xmx and -Xms parameters to control the JVM heap size
* `LOG_METRICS` - Should various metrics information be logged to the log
* `SODA_FOUNTAIN_DB_NAME` - Soda Fountain DB database name
* `SODA_FOUNTAIN_DB_PORT` - Soda Fountain DB port number
* `SODA_FOUNTAIN_DB_USER` - Soda Fountain DB user name
* `SPANDEX_HOST` - Spandex (Suggest) host
* `SPANDEX_PORT` - Spandex (Suggest) port
