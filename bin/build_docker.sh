#!/usr/bin/env bash
set -e

sbt soda-fountain-jetty/assembly
jarfile="$(ls -t soda-fountain-jetty/target/scala-2.10/soda-fountain-jetty-assembly-*.jar | head -1)"
cp "$jarfile" docker/soda-fountain-jetty-assembly.jar
docker build -t soda-fountain docker/
