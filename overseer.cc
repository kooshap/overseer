#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>
#include <math.h>
#include "overseer.h"
#include "worker.h"

const int NREAD=50000;
const int MAXKEY=50000;

using namespace std;

typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;

//string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

int binarySearch(int *arr,int key,int minIdx,int maxIdx){
	if (arr[maxIdx]<= key)
		return maxIdx;
	if (maxIdx - minIdx == 1) 
		return minIdx;
	int idx = (minIdx + maxIdx) >> 1;
	if (arr[idx] > key) 
		return binarySearch(arr, key, minIdx, idx);
	return binarySearch(arr, key, idx, maxIdx);
}

int findContainer(int key,int *routingArr) {
	return binarySearch(routingArr, key, 0, NUM_OF_WORKER-1);
}

bool belongTo(int workerNum, int key, int *arr) {
	if (workerNum==(sizeof(arr)/sizeof(*arr))-1) return (arr[workerNum]<=key);
	return (arr[workerNum]<=key) && (arr[workerNum+1]>key);
}

void routeTask(task t) {
	tq[findContainer(t.key, write_router)].put(t);
}

void overseer_write(int key,string val) {
	task t;
	t.key=key;
	t.value=val;
	t.opCode=WRITE_OP;
	routeTask(t);
}

void overseer_delete(int key) {
	task t;
	t.key=key;
	//t->value.clear();
	t.opCode=DELETE_OP;
	routeTask(t);
} 

void worker_exit(int id) {
	task t;
	t.opCode=EXIT_OP;
	tq[id].put(t);
}

int overseer_read(int k,node *root){
	record *rec=0;
	rec=find(root,k,0);
	if (rec){
		return rec->value;
	}else{
		return -1;
	}
}

int main(){
	std::atomic<int> activeThreads;
	activeThreads=NUM_OF_WORKER;
	thread workerThread[NUM_OF_WORKER];
	
	for (int i=0;i<NUM_OF_WORKER;i++){
		write_router[i]=round(MAXKEY/NUM_OF_WORKER)*i;
		read_router[i]=round(MAXKEY/NUM_OF_WORKER)*i;
		printf("write_router[%d]=%d\n",i,write_router[i]);
	}

	for (int i=0;i<MAXKEY/2;i++){
		overseer_write(i,"");
		overseer_write(MAXKEY/2+i,"");
	}	

	//printf("write queue filled\n");

	// Start the worker stats
	init_stats(NUM_OF_WORKER);

	// Start the garbage collector
	thread gcThread = thread(empty_garbage);
	gcThread.detach();


	Clock::time_point t0 = Clock::now();
	for (int i=0;i<NUM_OF_WORKER;i++){
		workerThread[i] = thread(run,i,&activeThreads,ref(tq));
		workerThread[i].detach();
	}
	this_thread::sleep_for (std::chrono::milliseconds(1000));

	int missCount=0;
	node *croot;
	int result1,result2;
	for (int i=0;i<NREAD/2;i++){
		//printf("findContainer(%d)=%d\n",i,findContainer(i,write_router));
		croot=root[findContainer(i,read_router)];
		if (croot) {
			result1=overseer_read(i,croot);
		}
		croot=root[findContainer(NREAD/2+i,read_router)];
		if (croot) {
			result2=overseer_read(NREAD/2+i,croot);
		}
		if (result1==-1) missCount++;
		if (result2==-1) missCount++;
	}
	
	printf("%d reads missed\n",missCount);
	
	
	for (int i=0;i<MAXKEY;i++){
		overseer_delete(i);
	}
	
	for (int i=0;i<NUM_OF_WORKER;i++){
		worker_exit(i);
	}
	
	printf("Exit commands sent, active threads:%d\n",(int)activeThreads);

	while (activeThreads)
		this_thread::sleep_for (std::chrono::milliseconds(100));

	
	Clock::time_point t1 = Clock::now();
	//printf("All thread finished in: %f\n", sec(t1-t0).count());

	// Terminate the garbage collector thread
	gcThread.~thread();

	// Stop the worker stats
	stop_stats();

	// Wait for GC and Stats to end
	this_thread::sleep_for (std::chrono::milliseconds(500));
	
	// Free whatever object left in the garbage collector
	force_empty_garbage();

	delete[] tq;
	delete[] write_router;
	return 0;	

}
