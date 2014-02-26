#include <math.h>
#include <stdio.h>
#include <atomic>
#include "task.cc"

const int ARRAY_LENGTH=1<<19;
const int ARRAY_MASK=(1<<19)-1;
const int MAX_INSERTERS=64;
const int BOUND=ARRAY_LENGTH-MAX_INSERTERS;

class taskQueue{
	private:
		task *taskArr;
		std::atomic<int> producerIdx;
		int consumerIdx;	
		
	public: 
		taskQueue()
		{
			producerIdx=0;
			consumerIdx=0;
			taskArr=new task[ARRAY_LENGTH];
		} 
		/*taskQueue(taskQueue&&) {}
		taskQueue& operator=(taskQueue&&) {}

		taskQueue(const taskQueue&) =delete;
		taskQueue& operator=(const taskQueue&) =delete;    
		*/
		void put(task t){
			while(((producerIdx-consumerIdx)&ARRAY_MASK)>BOUND) {
				printf("pId=%d, cId=%d, waiting..\n",(int)producerIdx,consumerIdx);	
			}
			
			taskArr[producerIdx]=t;
			producerIdx++;
			producerIdx.fetch_and(ARRAY_MASK);
			//producerIdx=producerIdx%ARRAY_LENGTH;
		}
		task get(){
			task t=taskArr[consumerIdx];
			if (consumerIdx==producerIdx) {
				t.key=-1;
				return t;	
			}

			consumerIdx++;
			consumerIdx&=ARRAY_MASK;
			return t;
		}
};
