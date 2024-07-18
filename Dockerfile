FROM python:3.11-slim as base

# install postgres and postgis
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get install -y sudo postgresql postgis supervisor

# add a dedicated user
RUN useradd --uid 1000 -U -G ssl-cert,postgres pgstac

# setup the python environment
COPY ./requirements.txt requirements.txt
RUN pip install --root-user-action ignore --upgrade pip
RUN pip install --root-user-action ignore --no-cache-dir --upgrade -r requirements.txt

# setup environment variables
# - pgstac settings needed to connect to the database
ENV PGUSER pgstac
ENV PGPASSWORD pgstac
ENV PGDATABASE postgis
ENV PGHOST 127.0.0.1
ENV PGPORT 5432

# - postgres settings needed to connect to the database
ENV POSTGRES_USER pgstac
ENV POSTGRES_PASS pgstac
ENV POSTGRES_DBNAME postgis
ENV POSTGRES_DB postgis
ENV POSTGRES_HOST 0.0.0.0
ENV POSTGRES_PORT 5432
ENV POSTGRES_HOST_READER 0.0.0.0
ENV POSTGRES_HOST_WRITER 0.0.0.0

# - application settings
ENV APP_HOST 0.0.0.0
ENV APP_PORT 9588

# configure and populate the database
RUN mkdir /app; chown pgstac:pgstac /app
COPY --chown=pgstac ingest.py /app/
COPY --chown=pgstac fetch_collections.py /app/
COPY --chown=pgstac --chmod=0755 setup-database.sh /app/
WORKDIR /app
RUN ./setup-database.sh

# run the postgresql database and the stac server
COPY --chown=pgstac supervisord.conf /etc/supervisor/supervisord.conf
COPY --chown=pgstac --chmod=0755 run-supervisor.sh /app/
COPY --chown=pgstac --chmod=0755 run-postgresql.sh /app/
COPY --chown=pgstac --chmod=0755 run-stacserver.sh /app/
RUN chown -R pgstac:pgstac /var/run/postgresql \
    && chown -R pgstac:pgstac /var/lib/postgresql/15/main

USER pgstac
ENTRYPOINT ["/app/run-supervisor.sh"]
