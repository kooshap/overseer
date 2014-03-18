#include <pthread.h>
#include <time.h>
#include "gc.h"
#include "parameters.h"

// The id of the last read operation for every reading thread
// Used by the garbage collector to check which objects are safe to delete
unsigned long long last_reader_version[MAX_READERS];

struct garbage_item *garbage_head[NUM_OF_WORKER] = {0};
struct garbage_item *garbage_curr[NUM_OF_WORKER] = {0};
struct garbage_item *candidate_head[NUM_OF_WORKER] = {0};
struct garbage_item *candidate_curr[NUM_OF_WORKER] = {0};

pthread_mutex_t swap_mutex;
int stop=0;

struct garbage_item* add_garbage(void *victim, unsigned long long curr_version, int worker_id)
{
	//printf("Garbage added %p\n",victim);
	struct garbage_item *ptr;
	ptr = (struct garbage_item *)malloc(sizeof(struct garbage_item));	
	/*if(NULL == ptr)
	{
		//printf("\n Node creation failed \n");
		return 0;
	}*/
	ptr->victim = victim;
	ptr->version = curr_version;
	ptr->next = 0;

	//pthread_mutex_lock(&swap_mutex);

	if(0 == candidate_head[worker_id])
	{
		candidate_head[worker_id] = candidate_curr[worker_id] = ptr;
	}	
	else
	{
		candidate_curr[worker_id]->next = ptr;
		candidate_curr[worker_id] = ptr;
	}
	
	//pthread_mutex_unlock(&swap_mutex);

	return candidate_curr[worker_id];
}

struct garbage_item* search_in_list(void *victim, struct garbage_item **prev, int worker_id)
{
	struct garbage_item *ptr = candidate_head[worker_id];
	struct garbage_item *tmp = 0;
	bool found = false;

	//printf("\n Searching the list for value [%d] \n",victim);

	while(ptr != 0)
	{
		if(ptr->victim == victim)
		{
			found = true;
			break;
		}
		else
		{
			tmp = ptr;
			ptr = ptr->next;
		}
	}

	if(true == found)
	{
		if(prev)
			*prev = tmp;
		return ptr;
	}
	else
	{
		return 0;
	}
}

// Sqaps the garbage list and the candidate list
void swap_lists(int worker_id)
{
	struct garbage_item *tmp = 0;

	tmp = candidate_head[worker_id];
	candidate_head[worker_id] = garbage_head[worker_id];
	garbage_head[worker_id] = tmp;

	tmp = candidate_curr[worker_id];
	candidate_curr[worker_id] = garbage_curr[worker_id];
	garbage_curr[worker_id] = tmp;
}

// Initialize the last_reader_version array to -1
void init_last_read_versions() 
{
	int i;
	for (i=0;i<MAX_READERS;i++) {
		last_reader_version[i]=0;
	}
}

// Returns the minimum value in the last_reader_version array
unsigned long long get_min_version() 
{
	unsigned long long min_version = last_reader_version[0];
	int i;
	for (i=1;i<MAX_READERS;i++) {
		if (last_reader_version[i]==0) {
			break;
		}
		if (min_version<last_reader_version[i]) {
			min_version = last_reader_version[i];
		}
	}
	return min_version;
}

// empties the garbage list
void empty_garbage()
{
	struct garbage_item *ptr;
	struct garbage_item *tmp;

	unsigned long long min_version = 0;

	init_last_read_versions();

	struct timespec tim, tim2;
	tim.tv_sec = 0;
	tim.tv_nsec = 100000000L;

	while (!stop) {
		ptr = 0;
		tmp = 0;

		// Sleep 
		nanosleep(&tim , &tim2);

		if (stop) break;	

		//printf("Start emptying\n");

		// Get the minimum version among the completed reads	
		min_version = get_min_version();
		//printf("Min version: %llu\n", min_version);

		// get the lock	
		//pthread_mutex_lock(&swap_mutex);

		int worker_id;
		for (worker_id=0;worker_id<NUM_OF_WORKER;worker_id++) {
			ptr = garbage_head[worker_id];

			// empty the garbage list
			while (ptr != 0)
			{
				// free the garbage object
				//printf("Free %p %p\n", ptr->victim, ptr);

				//printf(".");
				if (ptr->next==0) {
					garbage_curr[worker_id]=ptr;
				}

				if (ptr->version>min_version) {
					//printf("%p ",ptr->next);
					ptr = ptr->next;
					continue;
				}

				free(ptr->victim);
				ptr->victim = 0;

				tmp = ptr;
				ptr = ptr->next;
				// Move the head pointer forward if the first item is deleted
				if (garbage_head[worker_id]==tmp) {
					garbage_head[worker_id]=ptr;
				}
				// free the linkedList object
				//printf("-");
				free(tmp);
				tmp = 0;
			}
			// If the list is empty
			if (garbage_head[worker_id]==0) {	
				garbage_curr[worker_id] = 0;
			}

			swap_lists(worker_id);
		}

		// release the lock
		//pthread_mutex_unlock(&swap_mutex);
	}
	force_empty_garbage();
}

// empties the garbage list without checking the versions
void force_empty_garbage()
{
	struct garbage_item *ptr;
	struct garbage_item *tmp;

	// get the lock	
	pthread_mutex_lock(&swap_mutex);
	int worker_id;
	for (worker_id=0; worker_id<NUM_OF_WORKER;worker_id++) {	
		ptr = garbage_head[worker_id];
		// empty the garbage list
		while (ptr != 0)
		{
			// free the garbage object
			free(ptr->victim);
			ptr->victim = 0;

			tmp = ptr;
			ptr = ptr->next;
			// free the linkedList object

			//printf("%p\n",tmp);
			free(tmp);
			tmp = 0;
		}

		ptr = candidate_head[worker_id];
		// empty the candidate list
		while (ptr != 0)
		{
			// free the garbage object
			free(ptr->victim);
			ptr->victim = 0;

			tmp = ptr;
			ptr = ptr->next;
			// free the linkedList object
			free(tmp);
			tmp = 0;
		}
	}

	// release the lock
	pthread_mutex_unlock(&swap_mutex);
}

void stop_gc()
{
	stop=1;
}

int delete_from_list(void *victim,int worker_id)
{
	struct garbage_item *prev = 0;
	struct garbage_item *del = 0;

	//printf("\n Deleting value [%d] from list\n",victim);

	del = search_in_list(victim,&prev,worker_id);
	if(del == 0)
	{
		return -1;
	}
	else
	{
		if(prev != 0)
			prev->next = del->next;

		if(del == candidate_curr[worker_id])
		{
			candidate_curr[worker_id] = prev;
		}
		else if(del == candidate_head[worker_id])
		{
			candidate_head[worker_id] = del->next;
		}
	}

	free(del);
	del = 0;

	return 0;
}

void print_list(int worker_id)
{
	struct garbage_item *ptr = candidate_head[worker_id];

	//printf("\n -------Printing list Start------- \n");
	while(ptr != 0)
	{
		//printf("\n [%d] \n",ptr->victim);
		ptr = ptr->next;
	}
	//printf("\n -------Printing list End------- \n");

	return;
}

