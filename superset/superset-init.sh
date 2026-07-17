#!/bin/bash
set -e

echo "Waiting for Postgres..."
until python -c "
import sqlalchemy
engine = sqlalchemy.create_engine('postgresql+psycopg2://superset:superset@postgres:5432/superset')
engine.connect().close()
" 2>/dev/null; do
  sleep 2
done

echo "Initializing Superset database..."
superset db upgrade

echo "Creating admin user..."
superset fab create-admin \
  --username "${SUPERSET_ADMIN_USER:-admin}" \
  --firstname Admin \
  --lastname User \
  --email "${SUPERSET_ADMIN_EMAIL:-admin@marketrisk.local}" \
  --password "${SUPERSET_ADMIN_PASSWORD:-admin}"

echo "Initializing roles and permissions..."
superset init

echo "Superset initialization complete."