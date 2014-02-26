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
#define NREAD 100
#define MAXKEY 100

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

char *read(int k,IdxState *myIdxs){
	Record *record=(Record *)malloc(sizeof(Record));
	//record->key.keyval.intkey=randomKey();
	record->key.keyval.intkey=k;
	ErrCode er=get(myIdxs,NULL,record);
	if (!er){
		return record->payload;
	}else{
		return NULL;
	}
}

int main(){
	std::atomic<int> activeThreads;
	activeThreads=NUM_OF_WORKER;
	IdxState *idxs[NUM_OF_WORKER];
	thread workerThread[NUM_OF_WORKER];
	
	//int arr[]={1,5,11,23,100,124};
	//int res=findContainer(4,arr);
	//printf("Container: %d\n", res);
	//printf("BelongTo: %s\n", belongTo(1,4,arr)?"True":"False");

	KeyType keyType=INT;
	char name[10];
	string s;	
	for (int i=0;i<NUM_OF_WORKER;i++){ 
		s = std::to_string(i);
		strcpy(name,s.c_str());
		//printf("%s\n", name);
		ErrCode er=create(keyType, name);
		if (!er)
			printf("Index %s Created Succesfully, errcode:%d\n", name , er);

		er=openIndex(name, &idxs[i]);
		//if (!er)
		//printf("Index Opened Succesfully, errcode: %d\n", er);
	}

	//int wId;

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
		workerThread[i] = thread(run,i,&activeThreads,ref(tq[i]),ref(*idxs[i]));
		workerThread[i].detach();
	}

	this_thread::sleep_for (std::chrono::milliseconds(3000));

	int missCount=0;
	for (int i=0;i<NREAD;i++){
		//printf("findContainer(%d)=%d\n",i,findContainer(i,writerRouter));
		char *result=read(i,idxs[findContainer(i,writerRouter)]);
		if (result!=NULL) {
			//printf("%d, %s\n",i,result);
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