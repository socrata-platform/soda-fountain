#!/bin/bash
# Run soda fountain migrations.
# See README file for options - but command can be migrate / undo / redo
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG="${SODA_CONFIG:-/etc/soda2.conf}" # TODO: Don't depend on soda2.conf.
JARFILE=$("$BINDIR"/build.sh "$@")

COMMAND=${1:-migrate}
echo Running soda.server.MigrateSchema "$COMMAND" "$2"...
ARGS=( $COMMAND $2 )
java  -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -cp "$JARFILE" com.socrata.soda.server.MigrateSchema "${ARGS[@]}"
