#!/bin/bash
set -e
cd "/mnt/c/Users/Manas/Downloads/Total Job Application process flow/projects/contractor-onboarding-payouts"
mkdir -p docs
exec > docs/benchmark_output.txt 2>&1
PGPASSWORD=contractor_app_pw psql -h 127.0.0.1 -U contractor_app -d contractor_payouts -c "TRUNCATE contractor_events, payout_queue, payouts RESTART IDENTITY" 2>&1 | tail -3
./bin/onboard --n 3000 --batches 2 --txn-batch-size 250
./bin/intake --idle-timeout 5s
curl -s "http://127.0.0.1:8899/v1/mock/reset_faultrate?rate=0.35" ; echo
./bin/payout --batch 1
curl -s http://127.0.0.1:8899/v1/mock/stats; echo
curl -s "http://127.0.0.1:8899/v1/mock/reset_faultrate?rate=0.0" ; echo
./bin/killtest --batch 2 --kills 200 --slow-ms 2 --payout-bin ./bin/payout 2>&1 | tail -20
./bin/reconcile
mkdir -p docs
