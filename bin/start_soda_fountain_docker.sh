#!/usr/bin/env bash

image_tag="$1"
if [ "$image_tag" == "local" ]; then
  image="soda-fountain"
else
  image="649617362025.dkr.ecr.us-west-2.amazonaws.com/internal/soda-fountain:$image_tag"
fi

AWS_PROFILE=infrastructure docker run \
           -p 6010:6010 \
           -e ZOOKEEPER_ENSEMBLE="[ \"local.dev.socrata.net\" ]" \
           -e ARK_HOST="local.dev.socrata.net" \
           -e SODA_FOUNTAIN_DB_HOST="local.dev.socrata.net" \
           -e SODA_FOUNTAIN_DB_PORT=5432 \
           -e SODA_FOUNTAIN_DB_NAME="sodafountain" \
           -e SODA_FOUNTAIN_DB_USER="blist" \
           -e SODA_FOUNTAIN_DB_PASSWORD_LINE="password = \"blist\"" \
           -e DATA_COORDINATORS_FOR_NEW_DATASETS="primus" \
           -e SPANDEX_HOST="local.dev.socrata.net" \
           -e SPANDEX_PORT=8042 \
           -t "$image"


