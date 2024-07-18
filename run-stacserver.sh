#!/usr/bin/env bash

until pg_isready; do
    echo "waiting for postgresql..."
    sleep 2
done

stac-fastapi-pgstac
