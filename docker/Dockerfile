FROM socrata/java8-focal

EXPOSE 6010
EXPOSE 6012/udp
ENV JMX_PORT 6019
#EXPOSE 6039

RUN apt-get -y update && apt-get -y install jq

ENV SERVER_ROOT /srv/soda-fountain/
# TODO sbt task to copy latest jar into docker/
ENV SERVER_ARTIFACT soda-fountain-jetty-assembly.jar
ENV SERVER_CONFIG soda-fountain.conf

# defaults
ENV ENABLE_GRAPHITE false
ENV GRAPHITE_HOST 0.0.0.0
ENV GRAPHITE_PORT 0
ENV JAVA_XMX 512m
ENV JAVA_MAX_METASPACE 64m
ENV JAVA_GC_LOG_BEHAVIOR="-Xloggc:/tmp/gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintTenuringDistribution -XX:+UseGCLogFileRotation -XX:NumberOfGCLogFiles=10 -XX:GCLogFileSize=50M -XX:+TraceClassLoading -XX:+PrintGCCause -XX:+TraceClassUnloading"
ENV LOG_METRICS false
ENV MAX_THREADS 100
ENV QUERY_TIMEOUT 10m
ENV SODA_FOUNTAIN_DB_NAME metasoda
ENV SODA_FOUNTAIN_DB_PORT 5432
ENV SODA_FOUNTAIN_DB_USER soda_fountain
ENV THREAD_LIMIT_RATIO 0.5

WORKDIR $SERVER_ROOT

COPY ship.d /etc/ship.d
COPY ${SERVER_CONFIG}.j2 $SERVER_ROOT/
COPY $SERVER_ARTIFACT $SERVER_ROOT/
