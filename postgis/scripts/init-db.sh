# Ce script est copié dans le répertoire /docker-entrypoint-initdb.d
# de l'image Docker postgis et est executé lors de la première
# création du container

# create schema etl and kelrisks
psql --username "$POSTGRES_USER" << EOF
  CREATE SCHEMA kelrisks;
  CREATE SCHEMA etl;
EOF

# create airflow database
psql --username "$POSTGRES_USER" << EOF
  CREATE DATABASE $AIRFLOW_POSTGRES_DB;
  CREATE USER $AIRFLOW_POSTGRES_USER WITH PASSWORD '$AIRFLOW_POSTGRES_PASSWORD';
  GRANT ALL PRIVILEGES ON DATABASE $AIRFLOW_POSTGRES_DB to $AIRFLOW_POSTGRES_USER;
EOF


