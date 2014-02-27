#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>
#include <math.h>
#include "worker.h"

#define NUM_OF_WORKER 2
#define NREAD 1000000
#define MAXKEY 1000000

using namespace std;

typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;

taskQueue *tq=new taskQueue[NUM_OF_WORKER];
string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};
int *writerRouter=new int[NUM_OF_WORKER];

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
	tq[findContainer(t.key, writerRouter)].put(t);
}

void overseer_write(int key,string val) {
	task t;
	t.key=key;
	t.value=val;
	t.opCode=WRITE_OP;
	routeTask(t);
	//routeTask(*(new task(key,val,WRITE_OP)));
}

void overseer_delete(int key) {
	task t;
	t.key=key;
	t.value.clear();
	t.opCode=DELETE_OP;
	routeTask(t);
} 

void worker_exit(int id) {
	task t;
	t.key=0;
	t.value.clear();
	t.opCode=EXIT_OP;
	tq[id].put(t);
}

int overseer_read(int k,node *root){
	record *rec=(record *)malloc(sizeof(record));
	rec=find(root,k,false);
	if (rec){
		return rec->value;
	}else{
		return -1;
	}
}

int main(){
	std::atomic<int> activeThreads;
	activeThreads=NUM_OF_WORKER;
	node *root[NUM_OF_WORKER]={0};
	thread workerThread[NUM_OF_WORKER];
	
	for (int i=0;i<NUM_OF_WORKER;i++){
		writerRouter[i]=round(MAXKEY/NUM_OF_WORKER)*i;
		printf("writerRouter[%d]=%d\n",i,writerRouter[i]);
	}

	for (int i=0;i<MAXKEY;i++){
		overseer_write(i,list[i%10]);
	}	

	//printf("write queue filled\n");

	Clock::time_point t0 = Clock::now();
	for (int i=0;i<NUM_OF_WORKER;i++){
		workerThread[i] = thread(run,i,&activeThreads,ref(tq[i]),ref(root[i]));
		workerThread[i].detach();
	}
	this_thread::sleep_for (std::chrono::milliseconds(3000));

	int missCount=0;
	for (int i=0;i<NREAD;i++){
		//printf("findContainer(%d)=%d\n",i,findContainer(i,writerRouter));
		int result=overseer_read(i,root[findContainer(i,writerRouter)]);
		if (result!=-1) {
			//printf("%d, %d\n",i,result);
		}
		else {
			missCount++;
			//printf("%d Not found\n",i);
		}
	}
	
	printf("%d reads missed\n",missCount);
	
	
	for (int i=0;i<MAXKEY;i++){
		overseer_delete(i);
	}
	
	for (int i=0;i<NUM_OF_WORKER;i++){
		worker_exit(i);
	}
	
	while (activeThreads)
		this_thread::sleep_for (std::chrono::milliseconds(100));

	Clock::time_point t1 = Clock::now();
	//printf("All thread finished in: %f\n", sec(t1-t0).count());
	
	return 0;	

}
