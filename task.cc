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
};
