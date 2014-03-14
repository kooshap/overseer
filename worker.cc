#include "worker.h"
#include "overseer.h"

typedef minstd_rand G;
typedef uniform_int_distribution<> D;
typedef chrono::high_resolution_clock Clock;
typedef chrono::duration<double> sec;

// Task queues for workers
taskQueue *tq=new taskQueue[NUM_OF_WORKER];

// Write router
int *write_router=new int[NUM_OF_WORKER];
int *read_router=new int[NUM_OF_WORKER];

// Root nodes of each worker's tree
node **root=(node **)calloc(NUM_OF_WORKER,sizeof(node *));

// Offloading locks
mutex offload_mutex[NUM_OF_WORKER-1];

int worker_write(int id,int k, int v){
	if (root[id]) {
		//printf("root: %p\n",bptinsert(root[id],k,k));
		root[id]=bptinsert(root[id],k,k);
	}
	else {
		//printf("root: %p\n",bptinsert(root[id],k,k));
		root[id]=bptinsert(root[id],k,k);
	}
	return 0;
}

int worker_delete(int id,int k){
	root[id]=bptdelete(root[id],k);
	return 0;
}

void update_write_router(task t)
{
	if (t.offloader_id<t.victim_id) {
		write_router[t.victim_id]=((chunk *)t.offload_chunk)->key[0];	
		printf("WriterRouter[%d]=%d\n",t.victim_id,write_router[t.victim_id]);
	} 
	else {
		int i=0;
		chunk *offload_chunk=(chunk *)t.offload_chunk;
		while (offload_chunk->value[i]!=NULL) i++;
		write_router[t.offloader_id]=offload_chunk->key[i-1]+1;
		printf("WriterRouter[%d]=%d\n",t.offloader_id,write_router[t.offloader_id]);
	}
}

void update_read_router(task t)
{
	if (t.offloader_id<t.victim_id) {
		read_router[t.victim_id]=((chunk *)t.offload_chunk)->key[0];	
		printf("read_router[%d]=%d\n",t.victim_id,read_router[t.victim_id]);
	} 
	else {
		int i=0;
		chunk *offload_chunk=(chunk *)t.offload_chunk;
		while (offload_chunk->value[i]!=NULL) i++;
		read_router[t.offloader_id]=offload_chunk->key[i-1]+1;
		printf("read_router[%d]=%d\n",t.offloader_id,read_router[t.offloader_id]);
	}
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
		printf("Got lock #%d\n",mutex_id);
		task t;
		t.opCode=PUSH_CHUNK_OP;
		t.offloader_id=id;
		t.victim_id=id+offset;
		chunk * offchunk;
		if (offset<0) {
			offchunk=(chunk *)get_left_most_leaf(root[id],false);
		} else {
			offchunk=(chunk *)get_right_most_leaf(root[id],false);
		}		
		t.offload_chunk=offchunk;
		/*
		int i=0;
		while (offchunk->value[i]!=NULL) {
			printf("%d/%d ",offchunk->key[i], offchunk->value[i]->value);
			i++;
		}
		printf("\n");
		*/
		tq[id+offset].put(t);
		update_write_router(t);
	}
}

void push_chunk(int id,task t)
{
	chunk *offload_chunk=(chunk *)t.offload_chunk;
	int i=0;
	while (offload_chunk->value[i]!=NULL)
	{
		worker_write(id,offload_chunk->key[i],offload_chunk->value[i]->value);
		printf("Pushed %d/%d \n",offload_chunk->key[i], offload_chunk->value[i]->value);
		i++;
	}
}

void remove_chunk(int id,task t)
{
	chunk *offload_chunk=(chunk *)t.offload_chunk;
	int i=0;
	while (offload_chunk->value[i]!=NULL)
	{
		worker_delete(id,offload_chunk->key[i]);
		printf("Removed %d/%d \n",offload_chunk->key[i], offload_chunk->value[i]->value);
		i++;
	}
	free(offload_chunk->key);
	free(offload_chunk->value);
	free(offload_chunk);
}

void release_offload_lock(int offloader_id,int victim_id)
{
	offload_mutex[(offloader_id<victim_id)?offloader_id:victim_id].unlock();
	printf("Released lock #%d\n",(offloader_id<victim_id)?offloader_id:victim_id);
}

void run(int id,std::atomic<int> *active_threads,taskQueue *&itq){
	//string list[] = {"zero","one", "two","three","four","five","six","seven","eight","nine"};

	Clock::time_point t0 = Clock::now();

	int wid;
	int completed_tasks=0;
	task t=tq[id].get();
	
	while (t.opCode!=EXIT_OP) {
		switch (t.opCode) {
			case WRITE_OP:
				wid=find_container(t.key,write_router);
				if (wid!=id) {
					tq[wid].put(t);
					t=tq[id].get();
					continue;
				}
				worker_write(id,t.key, t.key);	
				break;
			case DELETE_OP:
				wid=find_container(t.key,write_router);
				if (wid!=id) {
					tq[wid].put(t);
					t=tq[id].get();
					continue;
				}
				worker_write(id,t.key, t.key);	
				if (!worker_delete(id,t.key)) {
				}
				break;
			case PUSH_CHUNK_OP:
				printf("PUSH Chunk from node %d\n",t.offloader_id);
				push_chunk(id,t);
				// Update read router
				update_read_router(t);
				t.opCode=REMOVE_CHUNK_OP;
				tq[t.offloader_id].put(t);
				break;
			case REMOVE_CHUNK_OP:
				remove_chunk(id,t);
				release_offload_lock(t.offloader_id,t.victim_id);
				break;
			default:
				printf("Unknown opCode\n",id,t.key);
				//this_thread::sleep_for (std::chrono::milliseconds(10));

		}
		perform_load_balancing_if_needed(id,completed_tasks);
		t=tq[id].get();
	}
	if (root[id]) {
		//printf("root: %p\n",root[id]);
		destroy_tree(root[id]);
	}

	Clock::time_point t1 = Clock::now();
	printf("Time: %f\n", sec(t1-t0).count());
	
	--*active_threads;
}

