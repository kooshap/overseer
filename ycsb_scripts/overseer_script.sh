#!/bin/sh

NTHREADS=$1
sed -i -e "s/\(NUM_OF_WORKER \).*/\1$1/" \
-e "s/\(NUM_NETWORK_READER \).*/\1$1/" coding/overseer/parameters.h

cd coding/overseer/

make clean
make 

bsub -q high_priority -m "compute012" -o bsub-output.txt ./overseer

OVERSEERPID=$!


sleep 5

cd ~/YCSB/

bsub -q high_priority -m "compute013" -o bsub-output.txt "java -cp build/ycsb.jar:$CLASSPATH com.yahoo.ycsb.Client -load -db com.yahoo.ycsb.db.overseer_client -P workloads/workloada -P large.dat -s -threads $1 > ~/load$1.dat"

sleep 5

while [[ "$(bjobs | grep compute013)" != "" ]]
do
	#echo "Loading overseer..."
	sleep 1
done

echo "Load finished."

sleep 5

cd ~ 
source wa.sh $1
 
sleep 5

while [[ "$(bjobs | grep compute013)" != "" ]]
do
	sleep 1
done

echo "Workloada finished."

sleep 5

source wc.sh $1
 
sleep 5

while [[ "$(bjobs | grep compute013)" != "" ]]
do
	sleep 1
done

echo "Workloadc finished."

sleep 10
bkill -u all

while [[ "$(bjobs | grep compute012)" != "" ]]
do
	sleep 1
done

echo "Overseer stopped."

