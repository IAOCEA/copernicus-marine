#!/usr/bin/env bash

until pg_isready; do
    echo "waiting for postgresql..."
    sleep 2
done

hypercorn stac_fastapi.pgstac.app:app \
    -b "$APP_HOST:$APP_PORT" \
    $STAC_SERVER_OPTIONS
