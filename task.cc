#include <string>
using namespace std;

typedef enum opCodeType {
	WRITE_OP,
	DELETE_OP,
	PUSH_CHUNK_OP,
	REMOVE_CHUNK_OP,
	EXIT_OP
} opCodeType;

struct task {
	//task(): key(0), value(""), opCode(DELETE_OP), victim_id(0), offloader_id(0) {}
	int key;
	string value;
	opCodeType opCode;
	int offloader_id;
	int victim_id;
	task *chunk;
	/*
	task(){
		//key=0;
		//value="";
		//opCode=DELETE_OP;
		//victim_id=0;
		//offloader_id=0;
	}
	task(int init_key,string init_value,opCodeType init_opCode){
		key=init_key;
		value=init_value;
		opCode=init_opCode;
	}
	*/
};
