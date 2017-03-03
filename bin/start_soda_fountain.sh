#!/bin/bash
# Start Soda Fountain main server
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BINDIR=$(dirname "$REALPATH")

CONFIG="${SODA_CONFIG:-/etc/soda2.conf}" # TODO: Don't depend on soda2.conf.
JARFILE=$("$BINDIR"/build.sh "$@")

"$BINDIR"/run_migrations.sh

java -Djava.net.preferIPv4Stack=true -Dconfig.file="$CONFIG" -jar "$JARFILE"
