#!/bin/bash

# This script is use to execute airflow command
# inside the container
container=$(docker ps -qf "name=webserver")

# create test database
docker exec $container airflow "$@"