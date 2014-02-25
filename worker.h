#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <cstring>
#include <random>
#include <chrono>
#include <atomic>
#include "server.h"
#include "taskQueue.cc"

using namespace std;
//int randomKey();

int write(int k, string v,IdxState *myIdxs);

char *read(int k,IdxState *myIdxs);

//void setTaskQueue(taskQueue *itq);

void run(int id,std::atomic<int> *activeThreads,taskQueue &itq, IdxState &iidxs);

