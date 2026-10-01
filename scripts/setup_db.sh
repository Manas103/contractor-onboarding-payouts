#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
sudo -n -u postgres psql -v ON_ERROR_STOP=1 -f scripts/setup_db.sql
EXISTS=$(sudo -n -u postgres psql -tAc "SELECT 1 FROM pg_database WHERE datname='contractor_payouts'")
if [ "$EXISTS" != "1" ]; then
  sudo -n -u postgres psql -v ON_ERROR_STOP=1 -c "CREATE DATABASE contractor_payouts OWNER contractor_app;"
fi
PGPASSWORD=contractor_app_pw psql -h 127.0.0.1 -U contractor_app -d contractor_payouts -c "SELECT 'connection ok', current_user, current_database();"
