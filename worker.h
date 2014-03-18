#include "taskQueue.h"
extern "C"{
#include "bpt.h"
#include "statistics.h"
#include "offloading_policy.h"
}

extern taskQueue *tq;
extern int *write_router;
extern int *read_router;
extern node **root;

//int randomKey();

//int write(int k, string v,IdxState *myIdxs);

//char *read(int k,IdxState *myIdxs);

//void setTaskQueue(taskQueue *itq);

void run(int id,std::atomic<int> *activeThreads,taskQueue *&itq);

