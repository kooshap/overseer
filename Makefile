overseer: overseer.o worker.o task.o taskQueue.o bptree64.so
	g++ -L. -O2 -o $@ $^ 

overseer.o: overseer.cc
	g++ -std=gnu++0x -O2 -lpthread -c overseer.cc 

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
	
bptree64.so: bptree.cc
	g++ -Wall -O3 -fPIC -shared -pthread -m64 -L/usr/lib64 -o $@ bptree.cc


.PHONY: clean
clean:
	-rm *.so *.o
	

