#!/bin/bash
set -e
cd "/mnt/c/Users/Manas/Downloads/Total Job Application process flow/projects/contractor-onboarding-payouts"
export PATH=/usr/local/go/bin:$PATH
export GOFLAGS=-mod=mod
go mod tidy
go build -o bin/onboard ./cmd/onboard
go build -o bin/intake ./cmd/intake
go build -o bin/payout ./cmd/payout
go build -o bin/stripemockserver ./cmd/stripemockserver
go build -o bin/reconcile ./cmd/reconcile
go build -o bin/killtest ./cmd/killtest
echo "BUILD_OK"
PGPASSWORD=contractor_app_pw psql -h 127.0.0.1 -U contractor_app -d contractor_payouts -f db/schema.sql
go test ./... 2>&1
echo "TEST_EXIT=$?"
