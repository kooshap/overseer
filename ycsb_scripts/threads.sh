#!/bin/sh

for NTHREADS in 1 2 4 6 8 10 12 14 16 18 20 22 24
do
	source overseer_script.sh $NTHREADS
	sleep 10
done

