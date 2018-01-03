#!/usr/bin/env bash

image="$1"
if [ -z "$image" ]; then
  echo "ERR: must specify an image name as the first argument to this script!"
  exit 1
fi

AWS_PROFILE=infrastructure docker run \
           -p 6010:6010 \
           -d -t "$image"


