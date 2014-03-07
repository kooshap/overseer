#include <pthread.h>
#include <time.h>
#include "gc.h"

// The id of the last read operation for every reading thread
// Used by the garbage collector to check which objects are safe to delete
unsigned long long last_reader_version[MAX_READERS];

struct garbage_item *garbage_head = NULL;
struct garbage_item *garbage_curr = NULL;
struct garbage_item *candidate_head = NULL;
struct garbage_item *candidate_curr = NULL;

pthread_mutex_t swap_mutex;

struct garbage_item* add_garbage(void *victim, unsigned long long curr_version)
{
	struct garbage_item *ptr = (struct garbage_item*)malloc(sizeof(struct garbage_item));
	if(NULL == ptr)
	{
		//printf("\n Node creation failed \n");
		return NULL;
	}
	ptr->victim = victim;
	ptr->version = curr_version;
	ptr->next = NULL;

	pthread_mutex_lock(&swap_mutex);

	if(NULL == candidate_head)
	{
		candidate_head = candidate_curr = ptr;
	}	
	else
	{
		candidate_curr->next = ptr;
		candidate_curr = ptr;
	}
	
	pthread_mutex_unlock(&swap_mutex);

	return ptr;
}

struct garbage_item* search_in_list(void *victim, struct garbage_item **prev)
{
	struct garbage_item *ptr = candidate_head;
	struct garbage_item *tmp = NULL;
	bool found = false;

	//printf("\n Searching the list for value [%d] \n",victim);

	while(ptr != NULL)
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
		return NULL;
	}
}

// Sqaps the garbage list and the candidate list
void swap_lists()
{
	struct garbage_item *tmp = NULL;

	tmp = candidate_head;
	candidate_head = garbage_head;
	garbage_head = tmp;

	tmp = candidate_curr;
	candidate_curr = garbage_curr;
	garbage_curr = tmp;
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
	tim.tv_nsec = 1000000L;

	while (1) {
		ptr = NULL;
		tmp = NULL;

		// Sleep 
		nanosleep(&tim , &tim2);
	
		// Get the minimum version among the completed reads	
		min_version = get_min_version();
		//printf("Min version: %llu\n", min_version);

		// get the lock	
		pthread_mutex_lock(&swap_mutex);

		ptr = garbage_head;
				
		// empty the garbage list
		while (ptr != NULL)
		{
			// free the garbage object
			//printf("Free %p %p\n", ptr->victim, ptr);

			//printf(".");
			if (ptr->next==NULL) {
				garbage_curr=ptr;
			}

			if (ptr->version>min_version) {
				ptr = ptr->next;
				continue;
			}
			
			free(ptr->victim);
			ptr->victim = NULL;

			tmp = ptr;
			ptr = ptr->next;
			// Move the head pointer forward if the first item is deleted
			if (garbage_head==tmp) {
				garbage_head=ptr;
			}
			// free the linkedList object
			free(tmp);
			tmp = NULL;
		}
		// If the list is empty
		if (garbage_head==NULL) {	
			garbage_curr = NULL;
		}
		
		swap_lists();

		// release the lock
		pthread_mutex_unlock(&swap_mutex);
	}
}

int delete_from_list(void *victim)
{
	struct garbage_item *prev = NULL;
	struct garbage_item *del = NULL;

	//printf("\n Deleting value [%d] from list\n",victim);

	del = search_in_list(victim,&prev);
	if(del == NULL)
	{
		return -1;
	}
	else
	{
		if(prev != NULL)
			prev->next = del->next;

		if(del == candidate_curr)
		{
			candidate_curr = prev;
		}
		else if(del == candidate_head)
		{
			candidate_head = del->next;
		}
	}

	free(del);
	del = NULL;

	return 0;
}

void print_list(void)
{
	struct garbage_item *ptr = candidate_head;

	//printf("\n -------Printing list Start------- \n");
	while(ptr != NULL)
	{
		//printf("\n [%d] \n",ptr->victim);
		ptr = ptr->next;
	}
	//printf("\n -------Printing list End------- \n");

	return;
}

