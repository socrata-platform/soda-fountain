#!/usr/bin/env bash
set -e

sbt soda-fountain-jetty/assembly
jarfile="soda-fountain-jetty/target/soda-fountain-jetty-assembly.jar"
cp "$jarfile" docker/soda-fountain-jetty-assembly.jar
docker build -t soda-fountain docker/
