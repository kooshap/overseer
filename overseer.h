#include "parameters.h"

#ifdef __cplusplus
extern "C" {
#endif
int find_container(int key,int *routingArr);
char *overseer_read(int k);
void overseer_write(int key,char *val);
void overseer_delete(int key);
#ifdef __cplusplus
}
#endif
