#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <cstring>
#include <random>
#include <chrono>
#include <atomic>
#include "worker.h"

#define MAXKEY 100000
using namespace std;

typedef minstd_rand G;
typedef uniform_int_distribution<> D;
typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;
G g;
D d(0, MAXKEY);
IdxState *myIdxs;
taskQueue *myTq;


int randomKey(){
	return d(g);
}

int write(int k, string v){
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

char *read(int k){
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

void setTaskQueue(taskQueue *imyTq){
	myTq=imyTq;
}

void run(int id,std::atomic<int> *activeThreads,int nread,taskQueue *itq, IdxState *iidxs){
	string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

	myTq=itq;
	myIdxs=iidxs;

	task t=myTq->get();
	while (t.key!=-1) { 	
		write(t.key, t.value);
		t=myTq->get();
	}

	Clock::time_point t0 = Clock::now();
	for (int i=0;i<nread;i+=2){
		char *result=read(i);
		//if (result!=NULL){ 
			//printf("%d :%s\n", i,result);
		//}
		//if (result==NULL){ 
			//printf("Key not found\n");
		//}
	}
	Clock::time_point t1 = Clock::now();
	printf("Time: %f\n", sec(t1-t0).count());
	
	*activeThreads-=1;
}

