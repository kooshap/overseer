
#define __STDC_LIMIT_MACROS

#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <vector>
#include <string>
#include <tr1/unordered_map>
#include "server.h"

using namespace std;
using namespace tr1;

#define smp_mb() asm volatile("sfence":::"memory")


#define ALIGN
#define REV_BUF_SIZE 4

struct Revision
{
    Revision *prev;
    uint32_t ver;
    bool exist;
};

struct Payload
{
    ALIGN Payload *more;
    ALIGN Revision *revs;
    ALIGN Revision buf[REV_BUF_SIZE];
    char payload[MAX_PAYLOAD_LEN + 1];
    uint32_t size;

    bool exist(uint32_t ver = UINT32_MAX) const;
    void revise(uint32_t ver);
};

struct Group
{
    void *leaf;
    ALIGN Group *next;
    Group *prev;
    ALIGN Payload data;
    Key key;

    void init(uint32_t ver, const Key &key, const char *payload);
    Group(uint32_t ver, const Key &key, const char *payload);
    Payload *find(const char *payload);
    Payload *add(uint32_t ver, const char *payload);
};

struct TrieNode
{
    void *leaf;
    ALIGN TrieNode *table[64];
    unsigned char level;
    unsigned char min;
    unsigned char max;
};

struct TrieIter
{
    TrieNode *node;
    unsigned char index;
    Group *g;
};

struct Index
{
    ALIGN TrieNode root;
    Group *min;
    uint32_t ver;
    pthread_mutex_t mutex;
    
    Group *buf;
    uint32_t size;
    TrieNode *buf2;
    uint32_t size2;

    Index(KeyType type);
    void get(TrieIter &itr, const Key &key);
    void set(TrieIter &itr, Group *g);
    void getNext(TrieIter &itr, const Key &key);

    ErrCode get(Record &record);
    ErrCode getNext(Record &record);
    ErrCode insert(const Key &key, const char *payload);
    ErrCode del(const Key &key, const char *payload);

    static unsigned char keyIndex(const Key &key, unsigned char level);
};

struct IdxState
{
    Index *index;

    TrieIter itr;
    const Payload *cursor;
    Key key;

    bool registered;
    bool locked;
    bool closed;

    IdxState(Index *index);
    void reset();
    ErrCode get(TxnState &txn, Record &record);
    ErrCode getNext(TxnState &txn, Record &record);
    ErrCode insert(TxnState &txn, const Key &key, const char *payload);
    ErrCode del(TxnState &txn, const Key &key, const char *payload);
};

struct TxnState
{
    uint32_t ver;
    vector<Revision *> updates;
    vector<IdxState *> indices;

    Index *last;

    TxnState(): updates(64) {updates.clear();};
    ErrCode lock(IdxState *index);
};

ALIGN uint32_t version = 0;
pthread_mutex_t mutexVer = PTHREAD_MUTEX_INITIALIZER;

unordered_map<string, Index *> indices;
pthread_rwlock_t lockIdx = PTHREAD_RWLOCK_INITIALIZER;

const Key KEY_MIN_SHORT = {SHORT, {INT32_MIN}};
const Key KEY_MAX_SHORT = {SHORT, {INT32_MAX}};
const Key KEY_MIN_INT = {INT, {INT64_MIN}};
const Key KEY_MAX_INT = {INT, {INT64_MAX}};
const Key KEY_MIN_VARCHAR = {VARCHAR, {0x0}};
const Key KEY_MAX_VARCHAR = {VARCHAR, {0x7f}};


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

int Key::operator<(const Key &k) const
{
    if(INT == type)
        return keyval.intkey < k.keyval.intkey; 
    else if(SHORT == type)
        return keyval.shortkey < k.keyval.shortkey;
    else
        return strcmp(keyval.charkey, k.keyval.charkey) < 0;
}

int Key::operator==(const Key &k) const
{
    if(INT == type)
        return keyval.intkey == k.keyval.intkey; 
    else if(SHORT == type)
        return keyval.shortkey == k.keyval.shortkey;
    else
        return strcmp(keyval.charkey, k.keyval.charkey) == 0;
}

bool Payload::exist(uint32_t ver) const 
{
    const Revision *r = revs;
    // locate version
    while(r && r->ver > ver)
        r = r->prev;
    // check existence
    if(r && r->exist)
        return true;
    return false;
}

void Payload::revise(uint32_t ver)
{
    Revision *r;
    if(size < REV_BUF_SIZE)
        r = &buf[size++];
    else
        r = new Revision();
    r->ver = ver;
    r->prev = revs;
    r->exist = revs ? !revs->exist : true;

    smp_mb();
    revs = r;
}

void Group::init(uint32_t ver, const Key &key, const char *payload)
{
    leaf = this;
    next = NULL;
    prev = NULL;
    data.more = NULL;
    data.revs = NULL;
    data.size = 0;
    
    this->key = key;
    strcpy(data.payload, payload);
    if(ver)data.revise(ver);
}

Group::Group(uint32_t ver, const Key &key, const char *payload)
{
    init(ver, key, payload);
}

Payload *Group::find(const char *payload)
{
    Payload *pl = &data;
    while(pl)
    {
        if(strcmp(pl->payload, payload) == 0)
            return pl;
        pl = pl->more;
    }
    return NULL;
}

Payload *Group::add(uint32_t ver, const char *payload)
{
    Payload *pl = new Payload();
    pl->size = 0;
    pl->more = data.more;
    strcpy(pl->payload, payload);
    pl->revs = NULL;
    pl->revise(ver);

    smp_mb();
    data.more = pl;
    return pl;
}

Index::Index(KeyType type)
{
    ver = 0;
    pthread_mutex_init(&mutex, NULL);

    if(INT == type)
    {
        min = new Group(0, KEY_MIN_INT, "");
        min->next = new Group(0, KEY_MAX_INT, "");
    }
    else if(SHORT == type)
    {
        min = new Group(0, KEY_MIN_SHORT, "");
        min->next = new Group(0, KEY_MAX_SHORT, "");
    }
    else
    {
        min = new Group(0, KEY_MIN_VARCHAR, "");
        min->next = new Group(0, KEY_MAX_VARCHAR, "");
    }
    min->next->prev = min;
    // fixed another bug here...
    memset(&root, 0, sizeof(TrieNode));
    root.table[0] = (TrieNode *)min;
    root.table[63] = (TrieNode *)min->next;
    root.level = 0;
    root.min = 0;
    root.max = 63;

    buf = NULL;
    size = 0;
    buf2 = NULL;
    size2 = 0;
}


unsigned char Index::keyIndex(const Key &key, unsigned char level)
{
    if(INT == key.type)
    {
        int shift = 58 - level * 6;
        if(level == 0)
            return ((key.keyval.intkey >> shift) & 0x3f) ^ 0x30;
        else if(shift < 0)
            return key.keyval.intkey & 0xf;
        return (key.keyval.intkey >> shift) & 0x3f;
    }
    else if(SHORT == key.type)
    {
        int shift = 26 - level * 6;
        if(level == 0)
            return ((key.keyval.shortkey >> shift) & 0x3f) ^ 0x30;
        else if(shift < 0)
            return key.keyval.shortkey & 0x3;
        return (key.keyval.shortkey >> shift) & 0x3f;
    }
    return key.keyval.charkey[level] & 0x3f;
}

void Index::get(TrieIter &itr, const Key &key)
{
    TrieNode *node = &root;
    do
    {
        itr.node = node;
        itr.index = keyIndex(key, node->level);
        node = node->table[itr.index];
    }while(node && !node->leaf);

    itr.g = (Group *)node;
    if(itr.g && !(itr.g->key == key))
        itr.g = NULL;
}

void Index::set(TrieIter &itr, Group *g)
{
    itr.g = (Group *)itr.node->table[itr.index];
    if(itr.index > itr.node->max)
        itr.node->max = itr.index;
    else if(itr.index < itr.node->min)
        itr.node->min = itr.index;

    if(!itr.g)
    {
        smp_mb();
        itr.node->table[itr.index] = (TrieNode *)g;
        itr.g = g;
    }
    else
    {
        if(!buf2 || 4000 == size2)
        {
            buf2 = (TrieNode *)malloc(4000 * sizeof(TrieNode));
            size2 = 0;
        }
        TrieNode *node = buf2 + size2++;
        memset(node, 0, sizeof(TrieNode));

        node->level = itr.node->level + 1;
        unsigned char index = keyIndex(itr.g->key, node->level);
        node->table[index] = (TrieNode *)itr.g;
        node->min = index;
        node->max = index;

        // recursively try to put g on node
        TrieNode *pnode = itr.node;
        unsigned char pindex = itr.index;
        itr.node = node;
        itr.index = keyIndex(g->key, node->level);
        set(itr, g);

/*
        index = keyIndex(g->key, node->level);
        node->table[index] = (TrieNode *)g;
        if(index > node->max)
            node->max = index;
        else node->min = index;
        //*/

        smp_mb();
        pnode->table[pindex] = node;
//        itr.node = node;
//        itr.index = index;
        itr.g = g;
    }
}

void Index::getNext(TrieIter &itr, const Key &key)
{
    TrieNode *node = itr.node->table[itr.index];
    while(node && !node->leaf)
    {
        itr.node = node;
        itr.index = keyIndex(key, node->level);
        node = node->table[itr.index];
    }

    itr.g = (Group *)node;
    if(itr.g)
    {
        // next key found in this slot
        if(key < itr.g->key)return;
        // previous key found in this slot, return the next one
        itr.g = itr.g->next;
        return;
    }

    char i;
    // search for the next key on the right side
    // dfs from left to right until a leaf is found
    for(i = (char)itr.index + 1; i < 64; i++)
        if((node = itr.node->table[(unsigned char)i]))
        {
            itr.index = (unsigned char)i;
            while(!node->leaf)
            {
                itr.node = node;
                itr.index = itr.node->min;
                node = itr.node->table[itr.index];
            }
            itr.g = (Group *)node;
            return;
        }

    // search for the previous key on the left side
    // dfs from right to left until a leaf is found
    for(i = (char)itr.index - 1; i >= 0; i--)
        if((node = itr.node->table[(unsigned char)i]))
        {
            itr.index = (unsigned char)i;
            while(!node->leaf)
            {
                itr.node = node;
                itr.index = itr.node->max;
                node = itr.node->table[itr.index];
            }
            itr.g = (Group *)node;
            itr.g = itr.g->next;
            return;
        }
}


ErrCode Index::get(Record &record)
{
    uint32_t ver = version;
    TrieIter itr;

    get(itr, record.key);

    Payload *p = itr.g ? &itr.g->data : NULL;
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

ErrCode Index::getNext(Record &record)
{
    uint32_t ver = version;
    Group *g = min;
    while(g)
    {
        Payload *p = &g->data;
        while(p)
        {
            if(p->exist(ver))
            {
                strcpy(record.payload, p->payload);
                record.key = g->key;
                return SUCCESS;
            }
            p = p->more;
        }
        g = g->next;
    }
    return DB_END;
}

ErrCode Index::insert(const Key &key, const char *payload)
{
    // lock index update
    pthread_mutex_lock(&mutex);
    TrieIter itr;
    get(itr, key);
    Payload *p = itr.g ? itr.g->find(payload) : NULL;
    // found
    if(p && p->exist())
    {
        pthread_mutex_unlock(&mutex);
        return ENTRY_EXISTS;
    }

    // not found
    // lock global version update
    pthread_mutex_lock(&mutexVer);
    ver = version + 1;
    if(!itr.g)
    {
        TrieIter itr2 = itr;
        getNext(itr2, key);

        if(!buf || 4000 == size)
        {
            buf = (Group *)malloc(4000 * sizeof(Group));
            size = 0;
        }
        Group *g = buf + size++;
        // bug here
        // was
//        g->init(UINT32_MAX, key, payload);
//        should be
        g->init(ver, key, payload);

        g->next = itr2.g;
        g->prev = itr2.g->prev;
        itr2.g->prev = g;
        smp_mb();
        g->prev->next = g;
        set(itr, g);
    }
    else if(!p)
    {
        itr.g->add(ver, payload);
    }
    else
        p->revise(ver);

    // make version available
    smp_mb();
    version = ver;
    pthread_mutex_unlock(&mutexVer);
    pthread_mutex_unlock(&mutex);
    return SUCCESS;
}

ErrCode Index::del(const Key &key, const char *payload)
{
    bool found = false;

    // lock index update
    pthread_mutex_lock(&mutex);

    // find the key and the/one payload
    TrieIter itr;

    get(itr, key);

    Payload *p;
    if(!itr.g)goto DEL_END2;
    p = payload[0] ? itr.g->find(payload) : &itr.g->data;

    while(p)
    {
        // exist? revise
        if(p->exist())
        {
            if(!found)
            {
                found = true;
                pthread_mutex_lock(&mutexVer);
                ver = version + 1;
            }
            p->revise(ver);
        }
        // done with the one
        if(payload[0])break;
        // continue with all
        p = p->more;
    }

DEL_END2:
    // nothing found
    if(!found)
    {
        pthread_mutex_unlock(&mutex);
        return payload[0] ? ENTRY_DNE : KEY_NOTFOUND;
    }
    // make version available
    smp_mb();
    version = ver;
    pthread_mutex_unlock(&mutexVer);
    pthread_mutex_unlock(&mutex);
    return SUCCESS;
}

IdxState::IdxState(Index *index)
{
    this->index = index;
    itr.node = NULL;
    registered = false;
    locked = false;
    closed = false;
}


void IdxState::reset()
{
    if(locked)
    {
        locked = false;
        pthread_mutex_unlock(&index->mutex);
    }
    itr.node = NULL;
    registered = false;
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

    index->locked = true;
    last = index->index;
    return SUCCESS;
}

ErrCode IdxState::get(TxnState &txn, Record &record)
{
    // register index
    if(!registered)
    {
        txn.indices.push_back(this);
        registered = true;
    }

    uint32_t ver = locked ? UINT32_MAX : txn.ver;

    index->get(itr, record.key);

    Payload *p = itr.g ? &itr.g->data : NULL;
    while(p)
    {
        if(p->exist(ver))
        {
            strcpy(record.payload, p->payload);
            cursor = p->more;
            return SUCCESS;
        }
        p = p->more;
    }
    if(!itr.g)
        key = record.key;
    cursor = NULL;
//    if(strcmp(record.key.keyval.charkey, "b_key") == 0)
//    {
//        printf("=");
//    }
    return KEY_NOTFOUND;
}

ErrCode IdxState::getNext(TxnState &txn, Record &record)
{
    // register index
    if(!registered)
    {
        txn.indices.push_back(this);
        registered = true;
    }

    uint32_t ver = locked ? UINT32_MAX : txn.ver;

    // never called get? setup scan context
    if(!itr.node)
    {
        itr.node = &index->root;
        itr.g = index->min;
        cursor = &itr.g->data;
    }

    // get did not find anything, use the next key
    if(!itr.g)
    {
        index->getNext(itr, key);
        cursor = &itr.g->data;
    }

    while(1)
    {
        while(cursor)
        {
            if(cursor->exist(ver))
            {
                record.key = itr.g->key;
                strcpy(record.payload, cursor->payload);
                cursor = cursor->more;
                return SUCCESS;
            }
            cursor = cursor->more;
        }
        if(!itr.g->next)return DB_END;
        itr.g = itr.g->next;
        cursor = &itr.g->data;
    }
}

ErrCode IdxState::insert(TxnState &txn, const Key &key, const char *payload)
{
    // register index
    if(!registered)
    {
        txn.indices.push_back(this);
        registered = true;
    }

    // lock index
    if(txn.lock(this))return DEADLOCK;

    TrieIter itr;
    index->get(itr, key);
    Payload *p = itr.g ? itr.g->find(payload) : NULL;
    // found
    if(p && p->exist())
        return ENTRY_EXISTS;

    // key not found
    if(!itr.g)
    {
        TrieIter itr2 = itr;

        index->getNext(itr2, key);

        if(!index->buf || 4000 == index->size)
        {
            index->buf = (Group *)malloc(4000 * sizeof(Group));
            index->size = 0;
        }
        Group *g = index->buf + index->size++;
        g->init(UINT32_MAX, key, payload);

        g->next = itr2.g;
        g->prev = itr2.g->prev;
        itr2.g->prev = g;
        smp_mb();

        g->prev->next = g;
        index->set(itr, g);
        p = &g->data;
    }
    // payload not found
    else if(!p)
    {
        p = itr.g->add(UINT32_MAX, payload);
    }
    // payload found, not modified yet
    else if(!p->revs || p->revs->ver != UINT32_MAX)
        p->revise(UINT32_MAX);
    // payload has been modified before
    else
    {
        p->revs->exist = true;
        p = NULL;
    }

    if(p)txn.updates.push_back(p->revs);
    return SUCCESS;
}

ErrCode IdxState::del(TxnState &txn, const Key &key, const char *payload)
{
    // register index
    if(!registered)
    {
        txn.indices.push_back(this);
        registered = true;
    }

    // lock index
    if(txn.lock(this))return DEADLOCK;

    bool found = false;
    // find the key and the/one payload
    TrieIter itr;
    index->get(itr, key);

    Payload *p;
    if(!itr.g)goto DEL_END2;
    p = payload[0] ? itr.g->find(payload) : &itr.g->data;

    while(p)
    {
        // exist? revise
        if(p->exist())
        {
            found = true;
            if(!p->revs || p->revs->ver != UINT32_MAX)
            {
                p->revise(UINT32_MAX);
                txn.updates.push_back(p->revs);
            }
            else p->revs->exist = false;
        }
        // done with the one
        if(payload[0])break;
        // continue with all
        p = p->more;
    }

DEL_END2:
    // nothing found
    if(!found)
        return payload[0] ? ENTRY_DNE : KEY_NOTFOUND;
    return SUCCESS;
}



ErrCode create(KeyType type, char *name)
{
    string str = name;
    ErrCode ec = DB_EXISTS;

    pthread_rwlock_wrlock(&lockIdx);
    if(!indices.count(str))
    {
        ec = SUCCESS;
        indices[str] = new Index(type);
    }
    pthread_rwlock_unlock(&lockIdx);
    return ec;
}

ErrCode openIndex(const char *name, IdxState **idxState)
{
    unordered_map<string, Index *>::const_iterator cit;
    Index *idx = NULL;

    pthread_rwlock_rdlock(&lockIdx);
    cit = indices.find(string(name));
    if(indices.end() != cit)
        idx = cit->second;
    pthread_rwlock_unlock(&lockIdx);

    if(!idx)return DB_DNE;

    *idxState = new IdxState(idx);
    return SUCCESS;
}

ErrCode closeIndex(IdxState *idxState)
{
    if(idxState->closed)
        return DB_DNE;

    if(idxState->registered)
        idxState->closed = true;
    else
        delete idxState;
    return SUCCESS;
}

ErrCode beginTransaction(TxnState **txn)
{
    *txn = new TxnState();
    (*txn)->ver = version;

    (*txn)->last = NULL;
    return SUCCESS;
}

ErrCode abortTransaction(TxnState *txn)
{
    uint32_t i;
    // abort changes
    for(i = 0; i < txn->updates.size(); i++)
    {
        Revision *prev = txn->updates[i]->prev;
        txn->updates[i]->exist = prev && prev->exist;
        smp_mb();
        txn->updates[i]->ver = prev ? prev->ver : 0;
    }

    // reset and unlock
    for(i = 0; i < txn->indices.size(); i++)
    {
        txn->indices[i]->reset();
        if(txn->indices[i]->closed)
            delete txn->indices[i];
    }
    delete txn;
    return SUCCESS;
}

ErrCode commitTransaction(TxnState *txn) 
{
    // readonly
    if(txn->updates.empty())
        return abortTransaction(txn);

    uint32_t i;
    // lock all indices and validate
    for(i = 0; i < txn->indices.size(); i++)
        if(txn->lock(txn->indices[i]))
        {
            abortTransaction(txn);
            return DEADLOCK;
        }

    // lock version and commit
    pthread_mutex_lock(&mutexVer);
    txn->ver = version + 1;

    for(i = 0; i < txn->updates.size(); i++)
        txn->updates[i]->ver = txn->ver;

    smp_mb();
    version = txn->ver;
    pthread_mutex_unlock(&mutexVer);

    // clean up and unlock
    txn->updates.clear();
    return abortTransaction(txn);
}

ErrCode get(IdxState *idxState, TxnState *txn, Record *record)
{
    if(txn)return idxState->get(*txn, *record);
    return idxState->index->get(*record);
}

ErrCode getNext(IdxState *idxState, TxnState *txn, Record *record)
{
    if(txn)return idxState->getNext(*txn, *record);
    return idxState->index->get(*record);
}

ErrCode insertRecord(IdxState *idxState, TxnState *txn, 
    Key *k, const char *payload)
{
    if(txn)return idxState->insert(*txn, *k, payload);
    return idxState->index->insert(*k, payload);
}

ErrCode deleteRecord(IdxState *idxState, TxnState *txn, Record *record)
{
    if(txn)return idxState->del(*txn, record->key, record->payload);
    return idxState->index->del(record->key, record->payload);
}

