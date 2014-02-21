#include <math.h>
#include <atomic>
#include "task.cc"

#define ARRAY_LENGTH 100000

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
		
		void put(task t){
			taskArr[producerIdx]=t;
			producerIdx++;
			producerIdx=producerIdx%ARRAY_LENGTH;
		}
		task get(){
			task t=taskArr[consumerIdx];
			if (consumerIdx==producerIdx) {
				t.key=-1;
				return t;	
			}

			consumerIdx+=1;
			consumerIdx=consumerIdx%ARRAY_LENGTH;
			return t;
		}
};
