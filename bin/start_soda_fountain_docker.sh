#!/usr/bin/env bash

image="$1"
if [ -z "$image" ]; then
  echo "ERR: must specify an image name as the first argument to this script!"
  exit 1
fi

local_config_dir="$(dirname "$(realpath "$0")")/../configs"

AWS_PROFILE=infrastructure docker run \
           -e SERVER_CONFIG=/etc/configs/application.conf \
           -v "$local_config_dir":/etc/configs \
           -p 6010:6010 \
           -t "$image"


