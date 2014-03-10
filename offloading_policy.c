#include "offloading_policy.h"

const float ratio_threshold=2;

int need_to_push(int worker_id,int num_workers)
{
	int *stats=get_throughput();

	int self=stats[worker_id];
	if (self<1000) return 0; // not enough statistics anyway
	int left=(worker_id==0) ? INT_MAX : stats[worker_id-1];
	int right=(worker_id==num_workers-1) ? INT_MAX : stats[worker_id+1];

	float leftRatio=(left==0) ? FLT_MAX : ((float)self)/left;
	float rightRatio=(right==0) ? FLT_MAX : ((float)self)/right;
	if (leftRatio>ratio_threshold || rightRatio>ratio_threshold) {
		return (leftRatio>rightRatio) ? -1 : 1;
	} else {
		return 0;
	}
}
