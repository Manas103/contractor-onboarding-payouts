#!/bin/bash
cd "/mnt/c/Users/Manas/Downloads/Total Job Application process flow/projects/contractor-onboarding-payouts"
nohup ./bin/stripemockserver --addr 127.0.0.1:8899 --fault-rate 0.0 > /tmp/stripemock.log 2>&1 &
echo $! > /tmp/stripemock.pid
sleep 1
cat /tmp/stripemock.pid
curl -s http://127.0.0.1:8899/v1/mock/stats
