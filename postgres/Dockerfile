FROM postgres:9.6

ENV PG_MAJOR 9.6
ENV POSTGIS_MAJOR 2.4

RUN apt-get update -yqq && \
    apt-get install -yqq postgresql-$PG_MAJOR-postgis-$POSTGIS_MAJOR-scripts

RUN mkdir -p /docker-entrypoint-initdb.d
COPY scripts/init-db.sh /docker-entrypoint-initdb.d