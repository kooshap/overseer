#!/bin/sh

cd YCSB/

echo "Starting workloada with $1 threads"

bsub -q high_priority -m "compute013" -o bsub-output.txt "java -cp build/ycsb.jar:$CLASSPATH com.yahoo.ycsb.Client -t -db com.yahoo.ycsb.db.overseer_client -P workloads/workloada -P large.dat -s -threads $1 > ~/workloada$1.dat"

cd -
