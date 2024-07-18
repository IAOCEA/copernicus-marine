#!/usr/bin/env bash

# start and configure postgresql
echo "-- starting postgresql";
service postgresql start
pg_isready || exit 1

# setup database access
echo "-- initialize database";
echo "CREATE USER pgstac WITH LOGIN PASSWORD 'pgstac' SUPERUSER;" | sudo -u postgres psql
sudo -u postgres createdb pgstac

# intialize the database
echo "-- initializing database";
echo "CREATE DATABASE postgis;" | sudo -u postgres psql
dsn="postgresql://${POSTGRES_USER}:${POSTGRES_PASS}@127.0.0.1:5432/postgis"
pypgstac migrate --dsn="$dsn"

# fetch data
echo "-- fetching data";
python fetch_collections.py

echo "-- ingesting data";
directories=$(find /app/MDS -name "collection.json" | xargs dirname)
python ingest.py --method upsert $directories

echo "-- removing data files"
rm -rf /app/MDS

# stop postgresql
service postgresql stop
