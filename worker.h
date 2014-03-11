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

//int randomKey();

//int write(int k, string v,IdxState *myIdxs);

//char *read(int k,IdxState *myIdxs);

//void setTaskQueue(taskQueue *itq);

void run(int id,std::atomic<int> *activeThreads,taskQueue *&itq, node *&root, int *&writerRouter);

