#!/bin/bash
# Run soda fountain migrations.
# See README file for options - but command can be migrate / undo / redo
BASEDIR=$(dirname "$0")/..
if [ -z "$SODA_CONFIG" ]; then
    if [ -e "$BASEDIR/../docs/onramp/services/soda2.conf" ]; then
        echo "Using soda2.conf from docs directory"
        CONFIG="$BASEDIR/../docs/onramp/services/soda2.conf"
    else
        echo "Failed to access sample soda2.conf in $BASEDIR/../docs/onramp/services/. Have you cloned the docs repo?"; exit;
    fi
else
    echo "Using soda2.conf specified in $SODA_CONFIG environment variable"
    CONFIG=$SODA_CONFIG
fi
JARS=( $BASEDIR/soda-fountain-jetty/target/scala-2.10/soda-fountain-jetty-assembly-*.jar )
# shellcheck disable=SC2012
JARFILE=$(ls -t "${JARS[@]}" | head -n 1)

if [ ! -e "$JARFILE" ]; then
    cd "$BASEDIR" && sbt assembly
    JARS=( $BASEDIR/soda-fountain-jetty/target/scala-2.10/soda-fountain-jetty-assembly-*.jar )
    # shellcheck disable=SC2012
    JARFILE=$(ls -t "${JARS[@]}" | head -n 1)
fi

COMMAND=${1:-migrate}
echo Running soda.server.MigrateSchema "$COMMAND" "$2"...
ARGS=($COMMAND $2)
java -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.soda.server.MigrateSchema "${ARGS[@]}"
