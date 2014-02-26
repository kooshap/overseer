taskQueue.o: taskQueue.cc
	g++ -std=gnu++0x -c taskQueue.cc -lm

task.o: task.cc
	g++ -std=gnu++0x -c task.cc 

worker.o: worker.cc
	g++ -std=gnu++0x -c worker.cc 

overseer: overseer.cc worker.o task.o taskQueue.o bptree64
	g++ -std=gnu++0x -L. -lpthread -o overseer worker.o task.o taskQueue.o overseer.cc lib.so 

all: nomap.cc
	g++ -Wall -O3 -fPIC -shared -pthread -o lib.so nomap.cc

lib64: nomap.cc
	g++ -Wall -O3 -fPIC -shared -pthread -m64 -L/usr/lib64 -o lib.so nomap.cc

bptree: bptree.cc
	g++ -Wall -O3 -fPIC -shared -pthread -o lib.so bptree.cc
	
bptree64: bptree.cc
	g++ -Wall -O3 -fPIC -shared -pthread -m64 -L/usr/lib64 -o lib.so bptree.cc


.PHONY: clean
clean:
	-rm lib.so *.o
	

