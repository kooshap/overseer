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
#define MAXKEY 100000

using namespace std;

typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;

string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

int main(){
	std::atomic<int> activeThreads;
	activeThreads=NUM_OF_WORKER;
	IdxState *idxs[NUM_OF_WORKER];
	thread workerThread[NUM_OF_WORKER];
	
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

	taskQueue *tq=new taskQueue[NUM_OF_WORKER];
	int wId;

	for (int i=0;i<MAXKEY;i++){
		wId=i%NUM_OF_WORKER;	
		task t;
		t.key=i;
		t.value=list[i%10];

		tq[wId].put(t);
	}	
	
	Clock::time_point t0 = Clock::now();
	for (int i=0;i<NUM_OF_WORKER;i++){
		workerThread[i] = thread(run,i,&activeThreads,round(NREAD/NUM_OF_WORKER),&tq[i],idxs[i]);
		workerThread[i].detach();
	}
	while (activeThreads)
		this_thread::sleep_for (std::chrono::milliseconds(100));

	Clock::time_point t1 = Clock::now();
	//printf("All thread finished in: %f\n", sec(t1-t0).count());
	
	return 0;	

}
