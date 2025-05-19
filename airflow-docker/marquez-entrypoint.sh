#!/bin/bash

set -e

# Đợi PostgreSQL khởi động
echo "Waiting for PostgreSQL..."
while ! nc -z postgres 5432; do
  sleep 1
done
echo "PostgreSQL started"

# Chạy Marquez
exec /usr/src/app/entrypoint.sh
