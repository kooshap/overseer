#include "task.cc"

class taskQueue{
	private:
		task *taskArr;
		std::atomic<int> producerIdx;
		int consumerIdx;	
		
	public: 
		taskQueue();
		~taskQueue();
		void put(task t);
		task get();
};
