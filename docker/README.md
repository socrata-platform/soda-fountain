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
* `ENABLE_GRAPHITE` - Should various metrics information be reported to graphite
* `GRAPHITE_HOST` - The hostname or IP of the graphite server, if enabled
* `GRAPHITE_PORT` - The port number for the graphite server, if enabled
* `JAVA_XMX` - Sets the -Xmx and -Xms parameters to control the JVM heap size
* `LOG_METRICS` - Should various metrics information be logged to the log
* `MAX_THREADS` - Sets the total number of threads available to Soda Fountain
* `SODA_FOUNTAIN_DB_NAME` - Soda Fountain DB database name
* `SODA_FOUNTAIN_DB_PORT` - Soda Fountain DB port number
* `SODA_FOUNTAIN_DB_USER` - Soda Fountain DB user name
* `THREAD_LIMIT_RATIO` - Sets the max ratio of threads available to be used by QC or DC. Default: 0.5
