#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE DATABASE superset;
    CREATE USER superset WITH PASSWORD 'superset';
    GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
    ALTER DATABASE superset OWNER TO superset;
EOSQL