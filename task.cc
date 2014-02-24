#include <string>
using namespace std;

typedef enum opCodeType {
	WRITE_OP,
	DELETE_OP,
} opCodeType;

struct task {
	int key;
	string value;
	opCodeType opCode;

	task(){
		key=0;
		value="";
		opCode=DELETE_OP;
	}

	task(int init_key,string init_value,opCodeType init_opCode){
		key=init_key;
		value=init_value;
		opCode=init_opCode;
	}
};
