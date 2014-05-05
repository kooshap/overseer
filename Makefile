CFLAGS=-ggdb -ljemalloc
#CFLAGS="-pg"

overseer: overseer.o worker.o task.o taskQueue.o bpt.o gc.o statistics.o offloading_policy.o overseer_server.o readqueue.o
	g++ $(CFLAGS) -O3 -pthread -o $@ $^ -levent -lpthread

overseer_server2.o: overseer_server2.c
	gcc $(CFLAGS) -O3 -c overseer_server2.c -levent

overseer_server.o: overseer_server.c readqueue.c readqueue.h
	gcc $(CFLAGS) -O3 -c overseer_server.c -levent -lpthread

readqueue.o: readqueue.c
	gcc $(CFLAGS) -O3 -c readqueue.c -levent -lpthread

overseer.o: overseer.cc overseer.h worker.o
	g++ $(CFLAGS) -std=gnu++0x -O3 -c overseer.cc 

taskQueue.o: taskQueue.cc taskQueue.h task.cc
	g++ $(CFLAGS) -std=gnu++0x -O3 -c taskQueue.cc -lm

task.o: task.cc
	g++ $(CFLAGS) -std=gnu++0x -O3 -c task.cc 

worker.o: worker.cc worker.h bpt.h task.cc taskQueue.cc	statistics.h offloading_policy.h overseer.h
	g++ $(CFLAGS) -std=gnu++0x -O3 -c worker.cc 

gc.o: gc.c gc.h
	gcc $(CFLAGS) -O3 -c gc.c

statistics.o: statistics.c statistics.h
	gcc $(CFLAGS) -O3 -c statistics.c

offloading_policy.o: offloading_policy.c 
	gcc $(CFLAGS) -O3 -c offloading_policy.c
	
socket_server.o: socket_server.cc overseer.h 
	gcc $(CFLAGS) -std=gnu++0x -O3 -c socket_server.cc

nomap: nomap.cc
	g++ $(CFLAGS) -Wall -O3 -fPIC -shared -pthread -o $@ nomap.cc

nomap64: nomap.cc
	g++ $(CFLAGS) -Wall -O3 -fPIC -shared -pthread -m64 -L/usr/lib64 -o $@ nomap.cc

bptree.so: bptree.cc
	g++ $(CFLAGS) -Wall -O3 -fPIC -shared -pthread -o $@ bptree.cc
	
bptree64.o: bptree.cc
	g++ $(CFLAGS) -O3 -m64 -L/usr/lib64 -c -o $@ bptree.cc

bpt.o: bpt.c bpt.h gc.h
	gcc $(CFLAGS) -O3 -c bpt.c

.PHONY: clean
clean:
	-rm -f overseer.o taskQueue.o task.o worker.o statistics.o offloading_policy.o gc.o *.o bpt*.o overseer core
	

