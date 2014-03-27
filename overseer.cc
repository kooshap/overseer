#include <stdio.h>
#include <stdlib.h>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>
#include <math.h>
#include <signal.h>
#include <unistd.h>
#include <string.h>
#include "worker.h"
#include "overseer.h"
#include "socket_server.h"
extern "C" {
#include "overseer_server.h"
}
#include <string>
#include "parameters.h"

using namespace std;

const int NREAD=500000;
//const int MAXKEY=500000;
const size_t MAXKEY=4294967294;// Unsinged 32 bit integer

typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;

//string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

int binary_search(size_t *arr,size_t key,int minIdx,int maxIdx){
	if (arr[maxIdx]<= key)
		return maxIdx;
	if (maxIdx - minIdx == 1) 
		return minIdx;
	int idx = (minIdx + maxIdx) >> 1;
	if (arr[idx] > key) 
		return binary_search(arr, key, minIdx, idx);
	return binary_search(arr, key, idx, maxIdx);
}

int find_container(size_t key,size_t *routingArr) {
	return binary_search(routingArr, key, 0, NUM_OF_WORKER-1);
}

bool belongTo(int workerNum, size_t key, size_t *arr) {
	if (workerNum==(sizeof(arr)/sizeof(*arr))-1) return (arr[workerNum]<=key);
	return (arr[workerNum]<=key) && (arr[workerNum+1]>key);
}

void route_task(task t) {
	tq[find_container(t.key, write_router)].put(t);
}

void overseer_write(size_t key,char *val) {
	task t;
	t.key=key;
	t.value=val;
	t.opCode=WRITE_OP;
	route_task(t);
}

void overseer_delete(size_t key) {
	task t;
	t.key=key;
	//t->value.clear();
	t.opCode=DELETE_OP;
	route_task(t);
} 

void worker_exit(int id) {
	task t;
	t.opCode=EXIT_OP;
	tq[id].put(t);
}

char *overseer_read(size_t k){
	node *croot=root[find_container(k,read_router)];
	record *rec=0;
	rec=find(croot,k,0);
	if (rec){
		return rec->value;
	}else{
		return (char *)("");
	}
}

static void sighandler(int signal) {
	fprintf(stdout, "Received signal %d: %s.  Shutting down.\n", signal, strsignal(signal));
	killServer();
}

int main(){
	// The counter for active workers.
	std::atomic<int> active_threads;
	active_threads=NUM_OF_WORKER;
	thread worker_thread[NUM_OF_WORKER];
	
	for (size_t i=0;i<NUM_OF_WORKER;i++){
		write_router[i]=round(MAXKEY/NUM_OF_WORKER)*i;
		read_router[i]=round(MAXKEY/NUM_OF_WORKER)*i;
		printf("write_router[%zd]=%zd\n",i,write_router[i]);
	}
	
	/*
	for (int i=0;i<MAXKEY/2;i++){
		overseer_write(i,(char *)"Hello!");
		overseer_write(MAXKEY/2+i,(char *)"Hello2!");
	}	
	*/

	//printf("write queue filled\n");

	// Start the worker stats
	init_stats(NUM_OF_WORKER);

	// Start the garbage collector
	thread gc_thread = thread(empty_garbage);
	gc_thread.detach();


	Clock::time_point t0 = Clock::now();
	for (int i=0;i<NUM_OF_WORKER;i++){
		worker_thread[i] = thread(run,i,&active_threads,ref(tq));
		worker_thread[i].detach();
	}

	//this_thread::sleep_for (std::chrono::milliseconds(1000));

	/*
	int miss_count=0;
	int result1,result2;
	for (int i=0;i<NREAD/2;i++){
		//printf("find_container(%d)=%d\n",i,find_container(i,write_router));
		result1=overseer_read(i);
		result2=overseer_read(NREAD/2+i);
		if (result1==-1) miss_count++;
		if (result2==-1) miss_count++;
	}
	
	printf("%d reads missed\n",miss_count);
	*/	
	//thread server_thread=thread(runServer);
	//server_thread.join();
	//
	
	// Setting the signal handler.
	struct sigaction sigIntHandler;

	sigIntHandler.sa_handler = sighandler;
	sigemptyset(&sigIntHandler.sa_mask);
	sigIntHandler.sa_flags = 0;

	sigaction(SIGINT, &sigIntHandler, NULL);

	runServer();

	/*	
	for (int i=0;i<MAXKEY;i++){
		overseer_delete(i);
	}
	*/
	
	this_thread::sleep_for (std::chrono::milliseconds(1000));

	for (int i=0;i<NUM_OF_WORKER;i++){
		worker_exit(i);
	}
	
	printf("Exit commands sent, active overseer workers:%d\n",(int)active_threads);

	while (active_threads)
		this_thread::sleep_for (std::chrono::milliseconds(100));

	printf("Overseer workers stopped.\n");
	
	Clock::time_point t1 = Clock::now();
	//printf("All thread finished in: %f\n", sec(t1-t0).count());

	// Stop the worker stats
	stop_stats();

	// Terminate the garbage collector thread
	stop_gc();
	//force_empty_garbage();

	// Wait for GC and Stats to end
	//this_thread::sleep_for (std::chrono::milliseconds(500));
	t0 = Clock::now();
	t1 = Clock::now();
	while (sec(t1-t0).count()<1){
		t1 = Clock::now();
	}
	
	// Free whatever object left in the garbage collector
	//force_empty_garbage();

	delete[] tq;
	delete[] read_router;
	delete[] write_router;
	free(root);
	return 0;	

}
