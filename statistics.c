#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pthread.h>
#include "statistics.h"

const int interval=500000000L;
int *stats;
int num_workers_;
int stopped=0;


void *resetter();

void init_stats(int num_workers)
{
	num_workers_=num_workers;
	stats=(int *)calloc(num_workers,sizeof(int));

	// Start the resetter 
	pthread_t resetter_thread;
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
	pthread_create(&resetter_thread,&attr,resetter,NULL);
}

void task_completed(int worker_id)
{
	stats[worker_id]+=OFFLOAD_FREQ;
}

int *get_throughput()
{
	return stats;
}

void reset_stats()
{
	memset(stats,0,num_workers_*sizeof(int));
}

void free_stats()
{
	free(stats);
}

void stop_stats()
{
	stopped=1;
}

void print_stats()
{
	int i=0;
	for (;i<num_workers_;i++)
		printf("%d,",stats[i]);
	printf("\n");
}

void *resetter()
{
	printf("Stat started\n");
	struct timespec tim, tim2;
	tim.tv_sec = 0;
	tim.tv_nsec = interval;

	while (!stopped) {
		// Sleep 
		nanosleep(&tim , &tim2);
		reset_stats();
	}
	free_stats();
	printf("Stat stopped and freed\n");
}

