#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

cd "$BASEDIR"
JARFILE="$(ls -rt soda-fountain-jetty/target/scala-*/soda-fountain-jetty-assembly-*.jar 2>/dev/null | tail -n 1)"
SRC_PATHS=($(find .  -maxdepth 2 -name 'src' -o -name '*.sbt' -o -name '*.scala'))
if [ -z "$JARFILE" ] || find "${SRC_PATHS[@]}" -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    if [ "$1" == '--fast' ]; then
        echo 'Assembly is out of date.  Fast start is enabled so skipping rebuild anyway...' >&2
    else
        nice -n 19 sbt assembly >&2
        JARFILE="$(ls -rt soda-fountain-jetty/target/scala-*/soda-fountain-jetty-assembly-*.jar 2>/dev/null | tail -n 1)"
        touch "$JARFILE"
    fi
fi

python -c "import os; print(os.path.realpath('$JARFILE'))"
