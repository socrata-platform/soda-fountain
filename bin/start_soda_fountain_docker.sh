#!/usr/bin/env bash

image_tag="$1"
if [ -z "$image_tag" ]; then
  echo "ERR>> must specify an image tag or `local` if you want to build your own container"
  exit 1
elif [ "$image_tag" == "local" ]; then
  image="soda-fountain"
else
  image="649617362025.dkr.ecr.us-west-2.amazonaws.com/internal/soda-fountain:$image_tag"
fi

AWS_PROFILE=infrastructure docker run \
           -p 6010:6010 \
           -d -t "$image"


