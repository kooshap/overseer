overseer: overseer.o worker.o task.o taskQueue.o bptree64.o bpt.o 
	g++ -O2 -pthread -o $@ $^ 

overseer.o: overseer.cc
	g++ -std=gnu++0x -O2 -c overseer.cc 

taskQueue.o: taskQueue.cc
	g++ -std=gnu++0x -O2 -c taskQueue.cc -lm

task.o: task.cc
	g++ -std=gnu++0x -O2 -c task.cc 

worker.o: worker.cc
	g++ -std=gnu++0x -O2 -c worker.cc 

nomap: nomap.cc
	g++ -Wall -O3 -fPIC -shared -pthread -o $@ nomap.cc

nomap64: nomap.cc
	g++ -Wall -O3 -fPIC -shared -pthread -m64 -L/usr/lib64 -o $@ nomap.cc

bptree.so: bptree.cc
	g++ -Wall -O3 -fPIC -shared -pthread -o $@ bptree.cc
	
bptree64.o: bptree.cc
	g++ -O3 -m64 -L/usr/lib64 -c -o $@ bptree.cc

bpt.o: bpt.c
	gcc -c -o $@ bpt.c

.PHONY: clean
clean:
	-rm -f overseer.o taskQueue.o task.o worker.o bpt*.o overseer
	

