#!/bin/bash

# This script is use to execute airflow command
# inside the container

container=$(docker ps -aqf "name=webserver")

# create test database
docker exec $container airflow "$@"