#include "task.cc"

class taskQueue{
	private:
		task *taskArr;
		int producerIdx;
		int consumerIdx;	
		
	public: 
		taskQueue();
		~taskQueue();
		void put(task t);
		task get();
};
