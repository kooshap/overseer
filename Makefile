overseer: overseer.o worker.o task.o taskQueue.o bptree64.o bpt.o gc.o statistics.o
	g++ -O2 -pthread -o $@ $^ 

overseer.o: overseer.cc worker.h
	g++ -std=gnu++0x -O2 -c overseer.cc 

taskQueue.o: taskQueue.cc
	g++ -std=gnu++0x -O2 -c taskQueue.cc -lm

task.o: task.cc
	g++ -std=gnu++0x -O2 -c task.cc 

worker.o: worker.cc worker.h bpt.h
	g++ -std=gnu++0x -O2 -c worker.cc 

gc.o: gc.c gc.h
	gcc -O2 -c gc.c

statistics.o: statistics.c statistics.h
	gcc -O2 -c statistics.c

nomap: nomap.cc
	g++ -Wall -O3 -fPIC -shared -pthread -o $@ nomap.cc

nomap64: nomap.cc
	g++ -Wall -O3 -fPIC -shared -pthread -m64 -L/usr/lib64 -o $@ nomap.cc

bptree.so: bptree.cc
	g++ -Wall -O3 -fPIC -shared -pthread -o $@ bptree.cc
	
bptree64.o: bptree.cc
	g++ -O3 -m64 -L/usr/lib64 -c -o $@ bptree.cc

bpt.o: bpt.c bpt.h gc.h
	gcc -O2 -c bpt.c

.PHONY: clean
clean:
	-rm -f overseer.o taskQueue.o task.o worker.o statistics.o bpt*.o overseer
	

