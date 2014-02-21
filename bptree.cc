
#define __STDC_LIMIT_MACROS

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "server.h"

#define NODE_SIZE 512
#define MAX_BF 40
//#define LEAF_SIZE 512

#define mb() asm volatile("sfence":::"memory")

// can only handle up to 13 revisions atm
struct Revisions
{
	//////////////0
	uint32_t num;
	uint32_t revs[15];
	//////////////64
};

struct Payload
{
	//////////////0
	Revisions revs;
	//////////////64
	Payload *more;
	//////////////72
	Payload *nextUpdate;
	/////////////80
	char payload[MAX_PAYLOAD_LEN + 11];
	bool updated;
	//////////////192

	bool exist(uint32_t ver) const;
	void revise(uint32_t ver);
	void abort();
	void commit(uint32_t ver);
};


struct NodeEntry
{
	uint16_t offset;   // offset to NodeHead of the key in byte
	uint8_t next;   // index of the next entry; 0xff means null
	uint8_t num;    // [0]: number of entries in use
};

struct NodeHead
{
	///////////////0
	NodeHead *null; // null indicates internal node
	///////////////8
	char *children; // ptr to children
	///////////////16
	NodeEntry entries[1]; // entry array


	char *getKey(uint8_t pos)
	{
		return ((char *)this) + entries[pos].offset;
	};

	NodeHead *getChild(uint8_t pos)
	{
		return (NodeHead *)(children + (pos - 1) * NODE_SIZE);
	};

	uint8_t nextPos(uint8_t pos) const
	{
		return entries[pos].next;
	};

	NodeHead *makeNewRoot() const;

	bool insert(const char *key, uint8_t pos2, KeyType type);

	NodeHead *split(NodeHead *node1, uint8_t pivpos, 
			uint8_t &pos1, KeyType type);

	uint8_t pivot(KeyType type);

	uint16_t &offset()
	{
		return entries[0].offset;
	};
	uint8_t &first()
	{
		return entries[0].next;
	};
	uint8_t &num()
	{
		return entries[0].num;
	};

	void activate(uint8_t pos)
	{
		mb();
		entries[pos].next = num() - 1;
	};
};

struct LeafEntry
{
	//////////////0
	Payload *payload; // first payload
	//////////////8
	uint32_t ver;   // disappeared since this version
	uint16_t offset;   // offset to Leafhead of the key in byte
	uint8_t next;   // index of the next entry; 0xff means null
	uint8_t num;    // similar to NodeEntry
	//////////////16
};

struct Iter;

struct LeafHead
{
	//////////////0
	LeafHead *prev; // ptr to previous leaf
	//////////////8
	LeafHead *next; // ptr to next leaf
	//////////////16
	LeafEntry entries[1];


	char *getKey(uint8_t pos)
	{
		return ((char *)this) + entries[pos].offset;
	};

	uint8_t nextPos(uint8_t pos) const
	{
		return entries[pos].next;
	};

	bool insert(const Key &key, uint8_t pos2);

	uint8_t pivotKey(Key &pivot);

	LeafHead *split(LeafHead *leaf1, uint8_t pivpos, 
			uint8_t &pos1, KeyType type, Iter *iter);

	uint16_t &offset()
	{
		return entries[0].offset;
	};
	uint8_t &first()
	{
		return entries[0].next;
	};
	uint8_t &num()
	{
		return entries[0].num;
	};

	void activate(uint8_t pos)
	{
		mb();
		entries[pos].next = num();
	};
};

struct Iter
{
	///////////0
	LeafHead *leaf;
	///////////8
	Payload *payload;
	///////////16
	uint8_t pos;
	char pad[7];
	///////////24
};

struct Index
{
	/////////////0
	pthread_mutex_t mutex; // write lock
	/////////////40
	LeafHead dummy;       // dummy used to located the first leaf
	/////////////64
	NodeHead *root;        // ptr to root
	/////////////72
	uint32_t ver;         // latest version
	KeyType type;         // type of the index
	/////////////80
	char name[48];        // name of this index
	/////////////128

	Payload *insert(const Key &key, const char *payload, 
			Iter *iter = NULL);
	ErrCode del(const Key &key, const char *payload);
	ErrCode get(Record &record);

	ErrCode getNext(Record &record, Iter &iter, uint32_t ver);
	void get(const Key &key, Iter &iter); 
};


struct IdxState
{
	///////////0
	IdxState *next;
	///////////8
	Index *index;
	///////////16
	Iter iter;
	///////////40
	Key key;
	bool registered;
	bool locked;
	bool closed;
	char pad[16];
	///////////192

	void reset();
	ErrCode get(TxnState &txn, Record &record);
	ErrCode insert(TxnState &txn, const Key &key, const char *payload);
	ErrCode del(TxnState &txn, const Key &key, const char *payload);
};

struct TxnState
{
	///////////0
	Payload *updates;
	///////////8
	IdxState *idxStates;
	///////////16
	Index *last;
	///////////24
	uint32_t ver;
	char pad[36];
	///////////64

	void registerIdx(IdxState *index);
	ErrCode lock(IdxState *index);
};


////////////////0
pthread_mutex_t mutex 
__attribute__((aligned(64))) = PTHREAD_MUTEX_INITIALIZER;
////////////////40
char pad[24];
////////////////64
Index *indices = NULL;
////////////////72
uint32_t idxNum = 0;
uint32_t version = 0;
////////////////80


void *aalloc(size_t size)
{
	char *p = (char *)malloc(size + 64);
	char *p2 = p + 64 - ((uint64_t)p) % 64;
	p2[-1] = p2 - p;
	return p2;
}

void afree(void *p)
{
	char *p2 = (char *)p;
	free(p2 - p2[-1]);
}

Key &Key::operator=(const Key &k)
{
	type = k.type;
	if(INT == type)
		keyval.intkey = k.keyval.intkey;
	else if(SHORT == type)
		keyval.shortkey = k.keyval.shortkey;
	else
		strcpy(keyval.charkey, k.keyval.charkey);
	return *this;
}

bool keyLE(const Key &key0, const char *key1)
{
	if(key0.type == INT)
		return key0.keyval.intkey <= *(const int64_t *)key1;
	else if(key0.type == SHORT)
		return key0.keyval.shortkey <= *(const int32_t *)key1;
	return strcmp(key0.keyval.charkey, key1) <= 0;
}

bool keyLT(const Key &key0, const char *key1)
{
	if(key0.type == INT)
		return key0.keyval.intkey < *(const int64_t *)key1;
	else if(key0.type == SHORT)
		return key0.keyval.shortkey < *(const int32_t *)key1;
	return strcmp(key0.keyval.charkey, key1) < 0;
}

bool keyEq(const Key &key0, const char *key1)
{
	if(key0.type == INT)
		return key0.keyval.intkey == *(const int64_t *)key1;
	else if(key0.type == SHORT)
		return key0.keyval.shortkey == *(const int32_t *)key1;
	return strcmp(key0.keyval.charkey, key1) == 0;
}

uint16_t keySize(const Key &key)
{
	if(key.type == INT)return sizeof(int64_t);
	if(key.type == SHORT)return sizeof(int32_t);
	return strlen(key.keyval.charkey) + 1;
}

uint16_t keySize(const char *key, KeyType type)
{
	if(type == INT)return sizeof(int64_t);
	if(type == SHORT)return sizeof(int32_t);
	return strlen(key) + 1;
}

void keyCpy(char *key0, const Key &key)
{
	if(key.type == INT)
		*(int64_t *)key0 = key.keyval.intkey;
	else if (key.type == SHORT)
		*(int32_t *)key0 = key.keyval.shortkey;
	else
		strcpy(key0, key.keyval.charkey);
}

void keyCpy(Key &key, const char *key1)
{
	if(key.type == INT)
		key.keyval.intkey = *(int64_t *)key1;
	else if(key.type == SHORT)
		key.keyval.shortkey = *(int32_t *)key1;
	else
		strcpy(key.keyval.charkey, key1);
}

void keyCpy(char *key0, const char *key1, KeyType type)
{
	if(type == INT)
		*(int64_t *)key0 = *(int64_t *)key1;
	else if (type == SHORT)
		*(int32_t *)key0 = *(int32_t *)key1;
	else
		strcpy(key0, key1);
}

Payload *findPayload(Payload *p, const char *payload)
{
	while(p)
	{
		if(strcmp(p->payload, payload) == 0)
			return p;
		p = p->more;
	}
	return NULL;
}

ErrCode create(KeyType type, char *name)
{
	pthread_mutex_lock(&mutex);
	if(indices == NULL)indices = (Index *)aalloc(sizeof(Index) * 1024);
	uint32_t i;
	for(i = 0; i < idxNum; i++)
		if(strcmp(indices[i].name, name) == 0)break;
	if(i == idxNum)
	{
		strcpy(indices[i].name, name);
		indices[i].type = type;
		indices[i].ver = 0;
		pthread_mutex_init(&indices[i].mutex, NULL);
		// create an empty b+tree
		indices[i].root = (NodeHead *)aalloc(NODE_SIZE * MAX_BF * 2);
		LeafHead *root = (LeafHead *)indices[i].root;
		root->prev = &indices[i].dummy;
		root->next = &indices[i].dummy;
		root->offset() = NODE_SIZE;
		root->first() = 0;
		root->num() = 0;

		// setup dummy
		indices[i].dummy.prev = root;
		indices[i].dummy.next = root;
		indices[i].dummy.offset() = NODE_SIZE;
		indices[i].dummy.first() = 0;
		indices[i].dummy.num() = 0;
		mb();
		idxNum++;
		i++;
	}
	pthread_mutex_unlock(&mutex);
	return i == idxNum ? SUCCESS : DB_EXISTS;
}

bool Payload::exist(uint32_t ver) const
{
	int i = revs.num - 1;
	// locate version
	while(i >= 0 && revs.revs[i] > ver)
		i--;
	// check existence
	if (i >= 0 && i % 2 == 0)
		return true;
	return false;
}

void Payload::revise(uint32_t ver)
{
	if (revs.num > 0 && revs.revs[revs.num - 1] == ver)
	{
		revs.num --;
	}
	else
	{
		revs.revs[revs.num] = ver;
		mb();
		revs.num ++;

		if(revs.num >= 15)
		{
			printf("too many revisions\n");
			exit(1);
		}
	}
}

void Payload::abort()
{
	if(revs.num && revs.revs[revs.num - 1] == UINT32_MAX)
		revs.num--;
}

void Payload::commit(uint32_t ver)
{
	if(revs.num && revs.revs[revs.num - 1] == UINT32_MAX)
		revs.revs[revs.num - 1] = ver;
}

void IdxState::reset()
{
	if(locked)
	{
		locked = false;
		pthread_mutex_unlock(&index->mutex);
	}
	iter.leaf = &index->dummy;
	iter.payload = NULL;
	iter.pos = 0;
	registered = false;
}

ErrCode IdxState::get(TxnState &txn, Record &record)
{
	// register index
	txn.registerIdx(this);

	uint32_t ver = locked ? UINT32_MAX : txn.ver;
	// key = record.key;

	// get p, update pos and leaf
	index->get(record.key, iter);
	Payload *p = iter.payload;
	while(p)
	{
		if(p->exist(ver))
		{
			strcpy(record.payload, p->payload);
			iter.payload = p->more;
			return SUCCESS;
		}
		p = p->more;
	}
	// keep the key, in case getNext is called after a failed get
	key = record.key;
	iter.leaf = NULL; // failed
	iter.pos = 0;
	iter.payload = NULL;
	return KEY_NOTFOUND;
}

ErrCode IdxState::del(TxnState &txn, const Key &key, const char *payload)
{
	if(txn.lock(this))return DEADLOCK;
	txn.registerIdx(this);

	bool found = false;
	Iter iter2;
	index->get(key, iter2);
	Payload *p = iter2.payload;

	if(!p) goto DEL_END2;
	p = payload[0] ? findPayload(p, payload) : p;

	while(p)
	{
		if(p->exist(UINT32_MAX))
		{
			found = true;
			p->revise(UINT32_MAX);
			if(p->updated == false)
			{
				p->updated = true;
				p->nextUpdate = txn.updates;
				txn.updates = p;
			}
		}
		if (payload[0]) break;
		p = p->more;
	}

DEL_END2:
	if(!found)
		return payload[0] ? ENTRY_DNE : KEY_NOTFOUND;
	return SUCCESS;
}

// inner func for get
void Index::get(const Key &key, Iter &iter)
{
	NodeHead *node = root;

	// go down internal nodes
	while(!node->null)
	{
		iter.pos = node->first();
		while(node->nextPos(iter.pos)
				&& keyLT(key, node->getKey(iter.pos)) == false)
			iter.pos = node->nextPos(iter.pos);
		node = node->getChild(iter.pos);
	}
	iter.leaf = (LeafHead *)node;
	// search in leaf
	iter.pos = iter.leaf->first();

	while(iter.pos && keyLE(key, iter.leaf->getKey(iter.pos)) == false)
		iter.pos = iter.leaf->nextPos(iter.pos);

	iter.payload = (iter.pos && keyEq(key, iter.leaf->getKey(iter.pos)))
		? iter.leaf->entries[iter.pos].payload : NULL;
}

// inner func of getNext
ErrCode Index::getNext(Record &record, Iter &iter, uint32_t ver)
{
	if(iter.leaf == NULL)
	{      
		get(record.key, iter);
		if(iter.payload)
			iter.pos = iter.leaf->nextPos(iter.pos);
		if(iter.pos)
			iter.payload = iter.leaf->entries[iter.pos].payload;
		else iter.payload = NULL;
	}

	do
	{
		while(iter.pos)
		{
			while(iter.payload)
			{
				if(iter.payload->exist(ver))
				{
					strcpy(record.payload, iter.payload->payload);
					keyCpy(record.key, iter.leaf->getKey(iter.pos));
					iter.payload = iter.payload->more;
					return SUCCESS;
				}
				iter.payload = iter.payload->more;
			}
			iter.pos = iter.leaf->nextPos(iter.pos);
			if(iter.pos)
				iter.payload = iter.leaf->entries[iter.pos].payload;
		}
		iter.leaf = iter.leaf->next;
		iter.pos = iter.leaf->first();
		if(iter.pos)
			iter.payload = iter.leaf->entries[iter.pos].payload;
	}while(iter.leaf != &dummy);
	return DB_END;
}

ErrCode Index::get(Record &record)
{
	uint32_t ver = version;
	Iter iter;
	get(record.key, iter);
	Payload *p = iter.payload;
	while(p)
	{
		if(p->exist(ver))
		{
			strcpy(record.payload, p->payload);
			return SUCCESS;
		}
		p = p->more;
	}
	return KEY_NOTFOUND;
}

ErrCode Index::del(const Key &key, const char *payload)
{
	bool found = false;

	Iter iter;
	get(key, iter);
	Payload *p = iter.payload;

	// lock index update
	pthread_mutex_lock(&mutex);
	if(p == NULL) goto DEL_END2;
	p = payload[0] ? findPayload(p, payload) : p;

	while(p)
	{
		if(p->exist(UINT32_MAX))
		{
			if(!found)
			{
				found = true;
				pthread_mutex_lock(&::mutex);
				ver = version + 1;
			}
			p->revise(ver);
		}
		// done with the one
		if (payload[0]) break;
		// del all
		p = p->more;
	}

DEL_END2:
	if(!found)
	{
		pthread_mutex_unlock(&mutex);
		return payload[0] ? ENTRY_DNE : KEY_NOTFOUND;
	}
	mb();
	version = ver;
	pthread_mutex_unlock(&::mutex);
	pthread_mutex_unlock(&mutex);
	return SUCCESS;
}

Payload *Index::insert(const Key &key, const char *payload, Iter *iter)
{
	// start from root
	// the following arrays track the stack
	NodeHead *path[32];
	// pos of the entry prior to the selected one
	uint8_t pos[32];
	int depth = 0;
	path[depth] = root;
	pos[depth] = 0;

	// go down internal node
	while(path[depth]->null == NULL)
	{
		NodeHead *node = path[depth];
		uint8_t pos2 = node->nextPos(0);
		while(node->nextPos(pos2)
				&& keyLT(key, node->getKey(pos2)) == false)
		{
			pos[depth] = pos2;
			pos2 = node->nextPos(pos2);
		}
		depth++;
		path[depth] = node->getChild(pos2);
		pos[depth] = 0;
	}

	// search in leaf
	LeafHead *leaf = (LeafHead *)path[depth];
	uint8_t pos2 = leaf->first();
	while(pos2 && keyLE(key, leaf->getKey(pos2)) == false)
	{
		pos[depth] = pos2;
		pos2 = leaf->nextPos(pos2);
	}

	// key found
	if(pos2 && keyEq(key, leaf->getKey(pos2)) == true)
	{
		// search for payload
		Payload *pl = leaf->entries[pos2].payload;
		while(pl && strcmp(pl->payload, payload))pl = pl->more;
		// payload exists
		if(pl && pl->exist(UINT32_MAX))return NULL;
		// payload was deleted, revise it
		if(pl)
		{
			pl->revise(UINT32_MAX);
			return pl;
		}
		// payload not found, prepend a new payload
		pl = (Payload *)aalloc(sizeof(Payload));
		pl->revs.num = 1;
		pl->revs.revs[0] = UINT32_MAX;
		pl->updated = false;
		strcpy(pl->payload, payload);
		pl->more = leaf->entries[pos2].payload;
		mb();
		leaf->entries[pos2].payload = pl;
		return pl;
	}

	// not found, insert(split)
	if(leaf->insert(key, pos2) == false)
	{
		// pivot used to split the leaf
		Key pivot;
		pivot.type = key.type;
		// position of the split point
		uint8_t pivpos[32];

		// try to insert bottom up
		// get the pivot of the leaf and move up
		pivpos[depth] = leaf->pivotKey(pivot);
		depth--;
		const char *pkey = pivot.keyval.charkey;
		while(depth >= 0 && path[depth]->insert(pkey, 
					path[depth]->nextPos(pos[depth]), key.type) == false)
		{
			pivpos[depth] = path[depth]->pivot(key.type);
			pkey = path[depth]->getKey(pivpos[depth]);
			depth--;
		}

		int minDepth = depth;
		NodeHead *newRoot = NULL;
		int newPos;

		// create a new root here
		NodeHead *newNode1;
		if(depth < 0)
		{
			newRoot = path[0]->makeNewRoot();
			newRoot->insert(pkey, 1, key.type);
			newRoot->activate(0);
			newNode1 = newRoot->getChild(2);
			newPos = 2;
		}
		else 
		{
			newNode1 = path[depth]->getChild(path[depth]->num() - 1);
			newPos = path[depth]->num() - 1;
		}

		depth++;
		// insert/split down the path
		while(path[depth]->null == NULL)
		{
			//            printf("%u ", newPos);
			//            fprintf(stderr, "splitting %u: %x; pos %u\n", depth, path[depth], pos[depth]), 
			path[depth] = path[depth]->split(newNode1, pivpos[depth], pos[depth], key.type);
			//            fprintf(stderr, "select %u: %x; pos %u\n", path[depth] == newNode1 ? 1 : 2, path[depth], pos[depth]);
			// next lvl also needs to be updated
			path[depth + 1] = path[depth]->getChild(path[depth]->nextPos(pos[depth]));
			if(path[depth + 1]->null == NULL)
				pkey = path[depth + 1]->getKey(pivpos[depth + 1]);
			else pkey = pivot.keyval.charkey;
			path[depth]->insert(pkey, path[depth]->nextPos(pos[depth]), key.type);
			path[depth]->activate(pos[depth]);
			newNode1 = path[depth]->getChild(path[depth]->num() - 1);
			newPos = path[depth]->num() - 1;
			depth++;
			if(depth > 30)
			{
				printf("depth too large\n");
				exit(1);
			}
		}

		// split leaf and insert the key
		leaf = (LeafHead *)path[depth];
		leaf = leaf->split((LeafHead *)newNode1, pivpos[depth], 
				pos[depth], key.type, iter);
		pos2 = leaf->nextPos(pos[depth]);
		leaf->insert(key, pos2);

		// activate the splitted part of the tree
		if(minDepth < 0)
		{
			mb();
			root = newRoot;
		}
		else path[minDepth]->activate(pos[minDepth]);
	}

	// key has just been inserted, make a payload now
	// and activate the leaf entry
	pos2 = leaf->num();
	Payload *pl = (Payload *)aalloc(sizeof(Payload));
	pl->revs.num = 1;
	pl->revs.revs[0] = UINT32_MAX;
	pl->updated = false;
	strcpy(pl->payload, payload);
	pl->more = NULL;
	leaf->entries[pos2].payload = pl;
	mb();
	leaf->activate(pos[depth]);

	return pl;
}



NodeHead *NodeHead::makeNewRoot() const
{
	NodeHead *newRoot = (NodeHead *)aalloc(NODE_SIZE * MAX_BF * 2);
	newRoot->null = NULL;
	newRoot->children = (char *)this;
	newRoot->offset() = NODE_SIZE;
	newRoot->first() = 1;
	newRoot->num() = 1;
	newRoot->entries[1].offset = NODE_SIZE;
	newRoot->entries[1].next = 0;
	return newRoot;
}

bool NodeHead::insert(const char *key, uint8_t pos2, KeyType type)
{
	uint16_t keysize = keySize(key, type);
	if(num() + 2 < MAX_BF * 2 
			&& sizeof(NodeHead) + (num() + 2) * sizeof(NodeEntry) 
			+ keysize <= offset())
	{
		offset() -= keysize;
		keyCpy(((char *)this) + offset(), key, type);
		entries[num() + 1].offset = offset();
		entries[num() + 1].next = num() + 2;
		entries[num() + 2] = entries[pos2];
		num() += 2;
		if(num() > MAX_BF * 2)
		{
			printf("toooooo many inserts\n");
			exit(1);
		}
		return true;
	}
	return false;
}

// split this into node1 and node2(immediate next), 
// pivpos is the last entry on this that should go into node1
// pos1 is the original position on this to be inserted
// when the func returns pos1 is the corresponding position on one of the new node
// returns the new node that has pos1
NodeHead *NodeHead::split(NodeHead *node1, uint8_t pivpos, uint8_t &pos1, KeyType type)
{
	NodeHead *node = NULL;
	NodeHead *node2 = (NodeHead *)((char *)node1 + NODE_SIZE);
	// init node 1
	node1->null = NULL;
	node1->children = (char *)aalloc(NODE_SIZE * MAX_BF * 2);
	node1->offset() = NODE_SIZE;
	node1->first() = 1;
	node1->num() = 1;
	// init node 2
	node2->null = NULL;
	node2->children = (char *)aalloc(NODE_SIZE * MAX_BF * 2);
	node2->offset() = NODE_SIZE;
	node2->first() = 1;
	node2->num() = 1;
	// traverse this
	uint8_t p = first();
	// entries that go into node 1
	const char *key;
	while(p != pivpos)
	{
		// check pos1
		if(node == NULL && pos1 == p)
		{
			node = node1;
			pos1 = node1->num();
		}
		// copy child
		//        fprintf(stderr, "[(%u)%u[%u]] ", num(), p, node1->num());
		memcpy(node1->getChild(node1->num()), getChild(p), NODE_SIZE);
		// copy key and entry
		key = getKey(p);
		node1->offset() -= keySize(key, type);
		keyCpy(((char *)node1) + node1->offset(), key, type);
		node1->entries[node1->num()].offset = node1->offset();
		node1->entries[node1->num()].next = node1->num() + 1;

		node1->num()++;
		p = nextPos(p);
	}
	// last entry that goes into node 1
	// check pos1
	if(node == NULL && pos1 == p)
	{
		node = node2;
		pos1 = 0;
	}
	// copy child
	memcpy(node1->getChild(node1->num()), getChild(p), NODE_SIZE);
	// copy  entry
	node1->entries[node1->num()].offset = node1->offset();
	node1->entries[node1->num()].next = 0;

	p = nextPos(p);
	//    fprintf(stderr, "<(%u)%u[%u]> ", num(), p, node2->num());
	// entries that go into node 2
	while(nextPos(p))
	{
		// check pos1
		if(node == NULL && pos1 == p)
		{
			node = node2;
			pos1 = node2->num();
		}
		// copy child
		//        fprintf(stderr, "(%u)%u[%u] ", num(), p, node2->num());
		memcpy(node2->getChild(node2->num()), getChild(p), NODE_SIZE);

		// copy key and entry
		key = getKey(p);
		node2->offset() -= keySize(key, type);
		keyCpy(((char *)node2) + node2->offset(), key, type);
		node2->entries[node2->num()].offset = node2->offset();
		node2->entries[node2->num()].next = node2->num() + 1;

		node2->num()++;
		p = nextPos(p);
	}
	// last entry that goes into node 2
	// check pos1
	if(node == NULL && pos1 == p)
	{
		fprintf(stderr, "impossible\n");
		node = node2;
		pos1 = node2->num();
	}
	// copy child
	memcpy(node2->getChild(node2->num()), getChild(p), NODE_SIZE);
	// copy  entry
	node2->entries[node2->num()].offset = node2->offset();
	node2->entries[node2->num()].next = 0;

	if(node == NULL)node = node1;
	return node;
}

uint8_t NodeHead::pivot(KeyType type)
{
	uint8_t pos = 0;
	uint8_t pos2 = first();
	uint16_t size = 0;
	do
	{
		size += sizeof(NodeEntry) + keySize(getKey(pos2), type);
		pos = pos2;
		pos2 = nextPos(pos2);
	}while(size < (NODE_SIZE - offset() + sizeof(NodeEntry) * num()) / 2);
	return pos;
}

bool LeafHead::insert(const Key &key, uint8_t pos2)
{
	uint16_t keysize = keySize(key);
	if(num() + 1 < MAX_BF * 2 
			&& sizeof(LeafHead) + (num() + 1) * sizeof(LeafEntry) 
			+ keysize <= offset())
	{
		offset() -= keysize;
		keyCpy(((char *)this) + offset(), key);
		entries[num() + 1].offset = offset();
		entries[num() + 1].next = pos2;
		entries[num() + 1].ver = UINT32_MAX;
		entries[num() + 1].payload = NULL;
		num()++;
		if(num() > MAX_BF * 2)
		{
			printf("toooooo many inserts\n");
			exit(1);
		}
		return true;
	}
	return false;
}

uint8_t LeafHead::pivotKey(Key &pivot)
{
	uint8_t pos = 0;
	uint8_t pos2 = first();
	uint16_t size = 0;
	do
	{
		size += sizeof(LeafEntry) + keySize(getKey(pos2), pivot.type);
		pos = pos2;
		pos2 = nextPos(pos2);
	}while(size < (NODE_SIZE - offset() + sizeof(LeafEntry) * num()) / 2);
	keyCpy(pivot, getKey(pos2));
	// truncate string key
	if(pivot.type == VARCHAR)
	{
		const char *p = getKey(pos);
		char *p2 = pivot.keyval.charkey;
		while(*p == *p2)
		{
			p++; p2++;
		}
		p2[1] = 0;
	}
	return pos;
}

// split this into leaf1 and leaf2(immediate next), 
// pivpos is the last entry on this that should go into leaf1
// pos1 is the original position on this to be inserted
// when the func returns pos1 is the corresponding position on one of the new leaf
// returns the new leaf that has pos1
LeafHead *LeafHead::split(LeafHead *leaf1, 
		uint8_t pivpos, uint8_t &pos1, KeyType type, Iter *iter)
{
	LeafHead *leaf = NULL;
	LeafHead *leaf2 = (LeafHead *)((char *)leaf1 + NODE_SIZE);
	// init leaf 1
	leaf1->prev = prev;
	leaf1->next = leaf2;
	leaf1->offset() = NODE_SIZE;
	leaf1->first() = 1;
	leaf1->num() = 1;
	// init leaf 2
	leaf2->prev = leaf1;
	leaf2->next = next;
	leaf2->offset() = NODE_SIZE;
	leaf2->first() = 1;
	leaf2->num() = 1;
	// traverse this
	uint8_t p = first();
	// entries that go into leaf 1
	const char *key;
	while(1)
	{
		// check pos1
		if(leaf == NULL && pos1 == p)
		{
			if(p != pivpos)
			{
				leaf = leaf1;
				pos1 = leaf1->num();
			}
			else
			{
				leaf = leaf2;
				pos1 = 0;
			}
		}
		// check iter
		if(iter && iter->leaf == this && iter->pos == p)
		{
			iter->leaf = leaf1;
			iter->pos = leaf1->num();
		}
		// copy key and entry
		key = getKey(p);
		leaf1->offset() -= keySize(key, type);
		keyCpy(((char *)leaf1) + leaf1->offset(), key, type);
		leaf1->entries[leaf1->num()] = entries[p];
		leaf1->entries[leaf1->num()].offset = leaf1->offset();
		leaf1->entries[leaf1->num()].next = leaf1->num() + 1;

		if(p == pivpos)break;
		leaf1->num()++;
		p = nextPos(p);
	}
	leaf1->entries[leaf1->num()].next = 0;

	p = nextPos(p);
	// entries that go into leaf 2
	while(p)
	{
		// check pos1
		if(leaf == NULL && pos1 == p)
		{
			leaf = leaf2;
			pos1 = leaf2->num();
		}
		// check iter
		if(iter && iter->leaf == this && iter->pos == p)
		{
			iter->leaf = leaf2;
			iter->pos = leaf2->num();
		}
		// copy key and entry
		key = getKey(p);
		leaf2->offset() -= keySize(key, type);
		keyCpy(((char *)leaf2) + leaf2->offset(), key, type);
		leaf2->entries[leaf2->num()] = entries[p];
		leaf2->entries[leaf2->num()].offset = leaf2->offset();
		leaf2->entries[leaf2->num()].next = leaf2->num() + 1;

		leaf2->num()++;
		p = nextPos(p);
	}
	leaf2->num()--;
	leaf2->entries[leaf2->num()].next = 0;

	// hook leaf1 and leaf2 up to the linked list
	mb();
	next->prev = leaf2;
	prev->next = leaf1;
	if(leaf == NULL)leaf = leaf1;
	return leaf;
}

ErrCode TxnState::lock(IdxState *index)
{
	if(index->locked)return SUCCESS;

	int r;
	if(index->index > last)
		r = pthread_mutex_lock(&index->index->mutex);
	else
		r = pthread_mutex_trylock(&index->index->mutex);
	if(!r && index->index->ver > ver)
	{
		pthread_mutex_unlock(&index->index->mutex);
		return DEADLOCK;
	}
	if(r)return DEADLOCK;

	// obtain the latest version
	if(ver == UINT32_MAX)
		ver = version;

	index->locked = true;
	last = index->index;
	return SUCCESS;
}


void TxnState::registerIdx(IdxState *idx)
{
	if(!idx->registered)
	{
		idx->next = idxStates;
		idxStates = idx;
		idx->registered = true;

		// obtain the latest version
		if(ver == UINT32_MAX)
			ver = version;
	}
}

ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record)
{
	record->key.type = idxState->index->type;
	if(txn)
	{
		txn->registerIdx(idxState);
		uint32_t ver = idxState->locked ? UINT32_MAX : txn->ver;
		if(idxState->iter.leaf == NULL)
			record->key = idxState->key;
		return idxState->index->getNext(*record, idxState->iter, ver);
	}
	else
	{
		idxState->iter.leaf = &idxState->index->dummy;
		idxState->iter.pos = 0;
		idxState->iter.payload = NULL;
		return idxState->index->getNext(*record, 
				idxState->iter, version);
	}
}

ErrCode insertRecord(IdxState *idxState, TxnState *txn, 
		Key *k, const char *payload)
{
	k->type = idxState->index->type;
	if(txn)
	{
		// lock index
		if(txn->lock(idxState))return DEADLOCK;

		// register index
		txn->registerIdx(idxState);


		// insert
		Payload *pl = idxState->index->insert(*k, payload, 
				&idxState->iter);
		if(pl == NULL)return ENTRY_EXISTS;
		if(pl->updated == false)
		{
			pl->updated = true;
			pl->nextUpdate = txn->updates;
			txn->updates = pl;
		}
		return SUCCESS;
	}

	// index lock
	pthread_mutex_lock(&idxState->index->mutex);
	// insert
	Payload *pl = idxState->index->insert(*k, payload);
	// write version
	if(pl)
	{
		// global lock
		pthread_mutex_lock(&mutex);
		pl->revise(UINT32_MAX);
		pl->revise(version + 1);
		mb();
		version++;
		// global unlock
		pthread_mutex_unlock(&mutex);
	}
	// index unlock
	pthread_mutex_unlock(&idxState->index->mutex);
	return pl ? SUCCESS : ENTRY_EXISTS;
}


ErrCode openIndex(const char *name, IdxState **idxState)
{
	uint32_t i;
	for(i = 0; i < idxNum; i++)
		if(strcmp(indices[i].name, name) == 0)break;

	if(i < idxNum)
	{
		*idxState = (IdxState *)aalloc(sizeof(IdxState));
		(*idxState)->next = NULL;
		(*idxState)->index = indices + i;
		(*idxState)->iter.leaf = &indices[i].dummy;
		(*idxState)->iter.payload = NULL;
		(*idxState)->iter.pos = 0;
		(*idxState)->registered = false;
		(*idxState)->locked = false;
		(*idxState)->closed = false;

		return SUCCESS;
	}
	return DB_DNE;
}


ErrCode closeIndex(IdxState *idxState)
{
	if(idxState->closed)
		return DB_DNE;

	if(idxState->registered)
		idxState->closed = true;
	else afree(idxState);

	return SUCCESS;
}


ErrCode beginTransaction(TxnState **txn)
{
	*txn = (TxnState *)aalloc(sizeof(TxnState));
	(*txn)->updates = NULL;
	(*txn)->idxStates = NULL;
	(*txn)->last = NULL;
	(*txn)->ver = UINT32_MAX;
	return SUCCESS;
}

ErrCode abortTransaction(TxnState *txn)
{
	// abort updates
	Payload *pl = txn->updates;
	while(pl)
	{
		pl->abort();
		pl->updated = false;
		pl = pl->nextUpdate;
	}

	// reset and unlock (delayed afree if closed)
	IdxState *idx = txn->idxStates;
	while(idx)
	{
		idx->reset();
		IdxState *idx2 = idx->next;
		if(idx->closed)
			afree(idx);
		idx = idx2;
	}
	afree(txn);
	return SUCCESS;
}


ErrCode commitTransaction(TxnState *txn) 
{
	// readonly
	if(txn->updates == NULL)
		return abortTransaction(txn);

	// lock all indices and validate
	IdxState *idx = txn->idxStates;
	while(idx)
	{
		if(txn->lock(idx))
		{
			abortTransaction(txn);
			return DEADLOCK;
		}
		idx = idx->next;
	}

	// lock version and commit
	pthread_mutex_lock(&mutex);
	txn->ver = version + 1;
	Payload *pl = txn->updates;
	while(pl)
	{
		pl->commit(txn->ver);
		pl->updated = false;
		pl = pl->nextUpdate;
	}
	// set version to index
	idx = txn->idxStates;
	while(idx)
	{
		idx->index->ver = txn->ver;
		idx = idx->next;
	}
	// make version available
	mb();
	version = txn->ver;
	pthread_mutex_unlock(&mutex);

	// clean up
	return abortTransaction(txn);
}


ErrCode deleteRecord(IdxState *idxState, TxnState *txn, Record *record)
{
	record->key.type = idxState->index->type;
	if(txn)return idxState->del(*txn, record->key, record->payload);
	return idxState->index->del(record->key, record->payload);
}


ErrCode get(IdxState *idxState, TxnState *txn, Record *record)
{
	record->key.type = idxState->index->type;
	if(txn)return idxState->get(*txn, *record);
	return idxState->index->get(*record);
}

