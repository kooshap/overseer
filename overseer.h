#include <string>
#include "parameters.h"

using namespace std;

int find_container(int key,int *routingArr);
char *overseer_read(int k);
void overseer_write(int key,string val);
void overseer_delete(int key);
