#!/bin/bash
set -e

# First boot? → create admin + upgrade DB
if [ ! -f /home/superset/.superset_initialized ]; then
  echo "First boot → creating admin & initializing Superset..."
  superset fab create-admin \
    --username "$ADMIN_USERNAME" \
    --firstname Superset \
    --lastname Admin \
    --email "$ADMIN_EMAIL" \
    --password "$ADMIN_PASSWORD"

  superset db upgrade
  superset init

  touch /home/superset/.superset_initialized
  echo "Superset ready!"
fi

# Normal start
exec "$@"