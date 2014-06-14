#!/bin/bash
# Run soda fountain migrations
BASEDIR=$(dirname $0)/..
CONFIG=${SODA_CONFIG:-$BASEDIR/../onramp/services/soda2.conf}
JARFILE=$BASEDIR/soda-fountain-jetty/target/scala-2.10/soda-fountain-jetty-assembly-*.jar
if [ ! -e $JARFILE ]; then
  cd $BASEDIR && sbt assembly
fi
java -Dconfig.file=$CONFIG -cp $JARFILE com.socrata.soda.server.MigrateSchema migrate
