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

using namespace std;

typedef minstd_rand G;
typedef uniform_int_distribution<> D;
typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;
//G g;
//D d(0, MAXKEY);
//taskQueue *myTq;


/*int randomKey(){
	return d(g);
}*/

int worker_write(node *&root,int k, int v){
	/*
	Key *key=(Key *)malloc(sizeof(Key));
	key->type=INT;
	key->keyval.intkey=k;
	char *payload=new char[v.size() + 1];
	copy(v.begin(), v.end(), payload);
	payload[v.size()] = '\0';
	*/
	if (root) {
		//printf("root: %p\n",bptinsert(root,k,k));
		root=bptinsert(root,k,k);
	}
	else {
		//printf("root: %p\n",bptinsert(root,k,k));
		root=bptinsert(root,k,k);
	}
	//if (!er)
	//	printf("Wrote %d:%s succesfully\n", k, v.c_str());
	return 0;
}
/*
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
*/
int worker_delete(node *&root,int k){
	root=bptdelete(root,k);
	return 0;
}

/*void setTaskQueue(taskQueue *imyTq){
	myTq=imyTq;
}*/

void run(int id,std::atomic<int> *activeThreads,taskQueue &itq, node *&root, int *&writerRouter){
	string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

	//myTq=itq;

	int leftMostKey=-1;
	int rightMostKey=-1;

	Clock::time_point t0 = Clock::now();
	
	task t=itq.get();

	while (t.opCode!=EXIT_OP) {
		switch (t.opCode) {
			case WRITE_OP:
				worker_write(root,t.key, t.key);	
				//printf("root: %p\n",root);
				//printf("Thread #%d wrote key #%d, %s\n",id,t.key,t.value.c_str());
				//printf("Thread #%d smallest key is %d\n",id,find_smallest_key(root, false)->value);
				break;
			case DELETE_OP:
				if (!worker_delete(root,t.key)) {
					//printf("Thread #%d deleted key #%d\n",id,t.key);
					record *rec=find_smallest_key(root, false);
					if (rec) {
						//printf("Thread #%d smallest key is %d\n",id,rec->value);
					}
				}
				break;
			case PUSH_CHUNK_OP:
				worker_write(root,t.key, t.key);	
				// TODO update router, put REMOVE_CHUNK_OP in neighbors taskQueue
				break;
			case REMOVE_CHUNK_OP:
				if (!worker_delete(root,t.key)) {
					// TODO release offload lock
				}
				break;
			default:
				printf("Unknown opCode\n",id,t.key);
				//this_thread::sleep_for (std::chrono::milliseconds(10));

		}
		t=itq.get();
	}
	if (root) {
		destroy_tree(root);
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

