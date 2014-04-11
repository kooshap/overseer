#include <math.h>
#include <stdio.h>
#include <chrono>
#include <thread>
#include "taskQueue.h"

const int ARRAY_LENGTH=1<<20;
const int ARRAY_MASK=(1<<20)-1;
const int MAX_INSERTERS=64;
const int BOUND=ARRAY_LENGTH-MAX_INSERTERS;

taskQueue::taskQueue()
{
	producerIdx=0;
	consumerIdx=0;
	taskArr=new task[ARRAY_LENGTH];
} 
taskQueue::~taskQueue()
{
	delete[] taskArr;
} 
/*taskQueue(taskQueue&&) {}
  taskQueue& operator=(taskQueue&&) {}

  taskQueue(const taskQueue&) =delete;
  taskQueue& operator=(const taskQueue&) =delete;    
  */
void taskQueue::put(task t){
	while(((producerIdx-consumerIdx)&ARRAY_MASK)>BOUND) {
		//printf("pId=%d, cId=%d, waiting..\n",(int)producerIdx,consumerIdx);	
		struct timespec tim, tim2;
		tim.tv_sec = 0;
		tim.tv_nsec = 10000;
		nanosleep(&tim, &tim2);
	}

	taskArr[producerIdx]=t;
	producerIdx++;
	producerIdx&=ARRAY_MASK;
	//producerIdx.fetch_and(ARRAY_MASK);
	//producerIdx=producerIdx%ARRAY_LENGTH;
}
task taskQueue::get(){
	struct timespec tim, tim2;
	tim.tv_sec = 0;
	tim.tv_nsec = 1000000;
	while (consumerIdx==producerIdx) {
		//printf("pId=%d, cId=%d, waiting..\n",(int)producerIdx,consumerIdx);	
		//this_thread::sleep_for (std::chrono::milliseconds(1));
		nanosleep(&tim, &tim2);
		continue;
	}
	task t=taskArr[consumerIdx];
	consumerIdx++;
	consumerIdx&=ARRAY_MASK;
	return t;
}
