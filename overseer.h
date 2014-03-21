#include "parameters.h"

#ifdef __cplusplus
extern "C" {
#endif
int find_container(size_t key,size_t *routingArr);
char *overseer_read(size_t k);
void overseer_write(size_t key,char *val);
void overseer_delete(size_t key);
#ifdef __cplusplus
}
#endif
