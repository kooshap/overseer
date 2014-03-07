#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>

// Maximum number of readign threads
#define MAX_READERS 64

// The id of the last read operation for every reading thread
// Used by the garbage collector to check which objects are safe to delete
extern unsigned long long last_reader_version[MAX_READERS];

struct garbage_item
{
	void* victim;
	unsigned long long version;
	struct garbage_item *next;
};

struct garbage_item* add_garbage(void *victim, unsigned long long reader_id);

void empty_garbage();

