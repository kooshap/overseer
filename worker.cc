#include "worker.h"
#include "overseer.h"


typedef minstd_rand G;
typedef uniform_int_distribution<> D;
typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;

taskQueue *tq=new taskQueue[NUM_OF_WORKER];

// Offloading locks
mutex offload_mutex[NUM_OF_WORKER-1];

int worker_write(node *&root,int k, int v){
	if (root) {
		//printf("root: %p\n",bptinsert(root,k,k));
		root=bptinsert(root,k,k);
	}
	else {
		//printf("root: %p\n",bptinsert(root,k,k));
		root=bptinsert(root,k,k);
	}
	return 0;
}

int worker_delete(node *&root,int k){
	root=bptdelete(root,k);
	return 0;
}

void perform_load_balancing_if_needed(int id, int &completed_tasks) 
{
	completed_tasks++;
	if (completed_tasks<OFFLOAD_FREQ)
		return;
	completed_tasks=0;

	task_completed(id);	
	int offset=need_to_push(id,NUM_OF_WORKER);
	// If it needs to offlaod to a neighbor
	if (offset) {
		// Mutex id is for worker #n is:
		// n-1: if we push to the left neighbor
		// n: if we push to the right neighbor
		int mutex_id=(offset>0)?id:id-1;
		// Try to acquire the lock
		if (!offload_mutex[mutex_id].try_lock()) {
			// Abort if another offload operation is taking place
			return;
		}

		//tq[id+offset].put();
	}
}

void release_offload_lock(int id,int victim_id)
{
	offload_mutex[(id<victim_id)?id:victim_id].unlock();
}

void run(int id,std::atomic<int> *activeThreads,taskQueue *&itq, node *&root, int *&writerRouter){
	string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

	int leftMostKey=-1;
	int rightMostKey=-1;

	Clock::time_point t0 = Clock::now();

	int completed_tasks=0;
	task t=tq[id].get();
	
	while (t.opCode!=EXIT_OP) {
		switch (t.opCode) {
			case WRITE_OP:
				worker_write(root,t.key, t.key);	
				//printf("root: %p\n",root);
				//printf("Thread #%d wrote key #%d, %s\n",id,t.key,t.value.c_str());
				//printf("Thread #%d biggest key is %d\n",id,find_biggest_key(root, false)->value);
				break;
			case DELETE_OP:
				if (!worker_delete(root,t.key)) {
					//printf("Thread #%d deleted key #%d\n",id,t.key);
					//record *rec=find_smallest_key(root, false);
					//if (rec) {
						//printf("Thread #%d smallest key is %d\n",id,rec->value);
					//}
				}
				break;
			case PUSH_CHUNK_OP:
				worker_write(root,t.key, t.key);
				writerRouter[id+1]+=1;
				printf("writerRouter[%d]=%d\n", id+1, writerRouter[id+1]);
				// TODO update router, put REMOVE_CHUNK_OP in neighbors taskQueue
				break;
			case REMOVE_CHUNK_OP:
				if (!worker_delete(root,t.key)) {
					printf("Unknown opCode\n");
					release_offload_lock(id,0);
				}
				break;
			default:
				printf("Unknown opCode\n",id,t.key);
				//this_thread::sleep_for (std::chrono::milliseconds(10));

		}
		perform_load_balancing_if_needed(id,completed_tasks);
		t=tq[id].get();
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

