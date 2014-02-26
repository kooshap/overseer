#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <cstring>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>
#include "worker.h"

#define MAXKEY 100000
using namespace std;

typedef minstd_rand G;
typedef uniform_int_distribution<> D;
typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;
//G g;
//D d(0, MAXKEY);
//IdxState *myIdxs;
//taskQueue *myTq;


/*int randomKey(){
	return d(g);
}*/

int worker_write(int k, string v,IdxState *myIdxs){
	Key *key=(Key *)malloc(sizeof(Key));
	key->type=INT;
	key->keyval.intkey=k;
	char *payload=new char[v.size() + 1];
	copy(v.begin(), v.end(), payload);
	payload[v.size()] = '\0';
	ErrCode er=insertRecord(myIdxs,NULL,key,payload);
	//if (!er)
	//	printf("Wrote %d:%s succesfully\n", k, v.c_str());
	return (int)er;
}

char *worker_read(int k,IdxState *myIdxs){
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

int worker_delete(int k,IdxState *myIdxs){
	Record *record=(Record *)malloc(sizeof(Record));
	record->key.keyval.intkey=k;
	ErrCode er=deleteRecord(myIdxs,NULL,record);
	return (int)er;
}

/*void setTaskQueue(taskQueue *imyTq){
	myTq=imyTq;
}*/

void run(int id,std::atomic<int> *activeThreads,taskQueue &itq, IdxState &iidxs){
	string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

	//myTq=itq;
	//myIdxs=&iidxs;

	Clock::time_point t0 = Clock::now();
	
	task t=itq.get();

	while (t.opCode!=EXIT_OP) {
		switch (t.opCode) {
			case WRITE_OP:
				worker_write(t.key, t.value,&iidxs);
				//printf("Thread #%d wrote key #%d, %s\n",id,t.key,t.value.c_str());
				break;
			case DELETE_OP:
				if (!worker_delete(t.key, &iidxs)) {
					//printf("Thread #%d deleted key #%d\n",id,t.key);
				}
				break;
			default:
				printf("Unknown opCode\n",id,t.key);
				//this_thread::sleep_for (std::chrono::milliseconds(10));

		}
		t=itq.get();
	}

	/*for (int i=0;i<nread;i+=2){
		char *result=worker_read(i);
		if (result!=NULL){ 
			printf("%d :%s\n", i,result);
		}
		if (result==NULL){ 
			printf("Key not found\n");
		}
	}*/
	Clock::time_point t1 = Clock::now();
	printf("Time: %f\n", sec(t1-t0).count());
	
	--*activeThreads;
}

