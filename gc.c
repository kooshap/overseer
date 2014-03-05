#include "gc.h"

struct garbage_item *head = NULL;
struct garbage_item *curr = NULL;

struct garbage_item* create_garbage_list(void *victim)
{
	//printf("\n creating list with headnode as [%d]\n",victim);
	struct garbage_item *ptr = (struct garbage_item*)malloc(sizeof(struct garbage_item));
	if(NULL == ptr)
	{
		//printf("\n Node creation failed \n");
		return NULL;
	}
	ptr->victim = victim;
	ptr->next = NULL;

	head = curr = ptr;
	return ptr;
}

struct garbage_item* add_garbage(void *victim, bool add_to_end)
{
	if(NULL == head)
	{
		return (create_garbage_list(victim));
	}

	if(add_to_end) {
		//printf("\n Adding node to end of list with victimue [%d]\n",victim);
	}
	else {
		//printf("\n Adding node to beginning of list with value [%d]\n",victim);
	}

	struct garbage_item *ptr = (struct garbage_item*)malloc(sizeof(struct garbage_item));
	if(NULL == ptr)
	{
		//printf("\n Node creation failed \n");
		return NULL;
	}
	ptr->victim = victim;
	ptr->next = NULL;

	if(add_to_end)
	{
		curr->next = ptr;
		curr = ptr;
	}
	else
	{
		ptr->next = head;
		head = ptr;
	}
	return ptr;
}

struct garbage_item* search_in_list(void *victim, struct garbage_item **prev)
{
	struct garbage_item *ptr = head;
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

// Added by Koosha
int empty_garbage()
{
	struct garbage_item *ptr = head;
	struct garbage_item *tmp = NULL;

	while (ptr != NULL)
	{
		// free the garbage object
		free(ptr->victim);
		ptr->victim = NULL;
		tmp = ptr;
		ptr = ptr->next;
		free(tmp);
		tmp = NULL;
	}
	curr = NULL;
	head = NULL;	
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

		if(del == curr)
		{
			curr = prev;
		}
		else if(del == head)
		{
			head = del->next;
		}
	}

	free(del);
	del = NULL;

	return 0;
}

void print_list(void)
{
	struct garbage_item *ptr = head;

	//printf("\n -------Printing list Start------- \n");
	while(ptr != NULL)
	{
		//printf("\n [%d] \n",ptr->victim);
		ptr = ptr->next;
	}
	//printf("\n -------Printing list End------- \n");

	return;
}

