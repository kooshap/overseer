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

#define MAXKEY 100000
using namespace std;

int randomKey();

int write(int k, string v);

char *read(int k);

void setTaskQueue(taskQueue *itq);

void run(int id,std::atomic<int> *activeThreads,int nread,taskQueue *itq, IdxState *iidxs);

