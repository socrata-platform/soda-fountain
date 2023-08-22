#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

cd "$BASEDIR"
JARFILE="soda-fountain-jetty/target/soda-fountain-jetty-assembly.jar"
SRC_PATHS=($(find .  -maxdepth 2 -name 'src' -o -name '*.sbt' -o -name '*.scala'))
if [ ! -f "$JARFILE" ] || find "${SRC_PATHS[@]}" -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    if [ "$1" == '--fast' ]; then
        echo 'Assembly is out of date.  Fast start is enabled so skipping rebuild anyway...' >&2
    else
        nice -n 19 sbt assembly >&2
        touch "$JARFILE"
    fi
fi

python -c "import os; print(os.path.realpath('$JARFILE'))"
