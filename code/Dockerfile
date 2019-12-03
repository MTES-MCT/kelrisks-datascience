FROM python:3.7-slim-stretch

LABEL maintainer="benoitguigal"

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ENV AIRFLOW_HOME=/usr/local/airflow
ENV EMBULK_DIR=$AIRFLOW_HOME/embulk
ENV DATA_DIR=$AIRFLOW_HOME/data
ENV SQL_DIR=$AIRFLOW_HOME/sql


# https://stackoverflow.com/questions/51033689/how-to-fix-error-on-postgres-install-ubuntu
RUN mkdir -p /usr/share/man/man1 /usr/share/man/man7

RUN apt-get update -yqq \
  && apt-get install -yqq \
    openjdk-8-jdk \
    build-essential \
    libpq-dev \
    curl \
    netcat \
    vim \
    wget \
    unzip \
    postgresql-client \
    postgis \
    net-tools

# Embulk
ENV EMBULK_BIN /usr/local/bin/embulk
RUN curl -o $EMBULK_BIN -L "https://dl.embulk.org/embulk-0.9.9.jar"
RUN chmod +x $EMBULK_BIN
ENV EMBULK_BIN /usr/local/bin/embulk

# Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt \
  && rm /tmp/requirements.txt

RUN useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow

RUN chown -R airflow: ${AIRFLOW_HOME}

USER airflow

RUN $EMBULK_BIN gem install embulk-output-postgresql

COPY ./scripts/entrypoint.sh /entrypoint.sh

COPY dags ${AIRFLOW_HOME}/dags
COPY embulk ${EMBULK_DIR}
COPY sql ${SQL_DIR}
COPY templates ${AIRFLOW_HOME}/templates

EXPOSE 8080

WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT /entrypoint.sh


