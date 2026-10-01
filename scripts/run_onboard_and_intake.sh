#!/bin/bash
set -e
cd "/mnt/c/Users/Manas/Downloads/Total Job Application process flow/projects/contractor-onboarding-payouts"
time ./bin/onboard --n 25000 --batches 5
echo "---intake---"
time ./bin/intake --idle-timeout 8s
