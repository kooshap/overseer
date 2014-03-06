#include <pthread.h>
#include <time.h>
#include "gc.h"

struct garbage_item *garbage_head = NULL;
struct garbage_item *garbage_curr = NULL;
struct garbage_item *candidate_head = NULL;
struct garbage_item *candidate_curr = NULL;

pthread_mutex_t swap_mutex;

struct garbage_item* add_garbage(void *victim)
{
	struct garbage_item *ptr = (struct garbage_item*)malloc(sizeof(struct garbage_item));
	if(NULL == ptr)
	{
		//printf("\n Node creation failed \n");
		return NULL;
	}
	ptr->victim = victim;
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

// empties the garbage list
void empty_garbage()
{
	struct garbage_item *ptr;
	struct garbage_item *tmp;
	
	struct timespec tim, tim2;
	tim.tv_sec = 0;
	tim.tv_nsec = 1000000L;

	while (1) {
		ptr = NULL;
		tmp = NULL;

		nanosleep(&tim , &tim2);
		
		// get the lock	
		pthread_mutex_lock(&swap_mutex);

		ptr = garbage_head;
		
		// empty the garbage list
		while (ptr != NULL)
		{
			// free the garbage object
			//printf("Free %p %p\n", ptr->victim, ptr);
			//printf(".");
			free(ptr->victim);
			ptr->victim = NULL;
			tmp = ptr;
			ptr = ptr->next;
			// free the linkedList object
			free(tmp);
			tmp = NULL;
		}
		garbage_curr = NULL;
		garbage_head = NULL;	
		
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

