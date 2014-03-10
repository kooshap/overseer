const int OFFLOAD_FREQ=50;

void init_stats(int num_workers);
void task_completed(int worker_id);
int *get_throughput();
void free_stats();
void stop_stats();
