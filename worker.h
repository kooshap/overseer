#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <cstring>
#include <random>
#include <chrono>
#include <atomic>
#include <thread>
//#include "server.h"
#include "taskQueue.cc"
extern "C"{
#include "bpt.h"
}
extern "C"{
#include "statistics.h"
#include "offloading_policy.h"
}

using namespace std;

extern taskQueue *tq;
extern int *write_router;
extern int *read_router;
extern node **root;

//int randomKey();

//int write(int k, string v,IdxState *myIdxs);

//char *read(int k,IdxState *myIdxs);

//void setTaskQueue(taskQueue *itq);

void run(int id,std::atomic<int> *activeThreads,taskQueue *&itq);

