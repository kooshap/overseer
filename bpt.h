// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <string.h>
#include "gc.h"
#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// Default order is 4.
#define DEFAULT_ORDER 128

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 200

// TYPES.

/* Type representing the record
 * to which a given key refers.
 * In a real B+ tree system, the
 * record would hold data (in a database)
 * or a file (in an operating system)
 * or some other information.
 * Users can rewrite this part of the code
 * to change the type and content
 * of the value field.
 */

#ifdef _cplusplus
extern "C" {
#endif
typedef struct record {
	char *value;
} record;

typedef struct chunk {
	size_t *key;
	record **value;
} chunk;

/* Type representing a node in the B+ tree.
 * This type is general enough to serve for both
 * the leaf and the internal node.
 * The heart of the node is the array
 * of keys and the array of corresponding
 * pointers.  The relation between keys
 * and pointers differs between leaves and
 * internal nodes.  In a leaf, the index
 * of each key equals the index of its corresponding
 * pointer, with a maximum of order - 1 key-pointer
 * pairs.  The last pointer points to the
 * leaf to the right (or NULL in the case
 * of the rightmost leaf).
 * In an internal node, the first pointer
 * refers to lower nodes with keys less than
 * the smallest key in the keys array.  Then,
 * with indices i starting at 0, the pointer
 * at i + 1 points to the subtree with keys
 * greater than or equal to the key in this
 * node at index i.
 * The num_keys field is used to keep
 * track of the number of valid keys.
 * In an internal node, the number of valid
 * pointers is always num_keys + 1.
 * In a leaf, the number of valid pointers
 * to data is always num_keys.  The
 * last leaf pointer points to the next leaf.
 */
typedef struct node {
	void ** pointers;
	size_t * keys;
	struct node * parent;
	bool is_leaf;
	int num_keys;
	struct node * next; // Used for queue.
} node;

// FUNCTION PROTOTYPES.

// Output and utility.

record * find( node * root, size_t key, int thread_id );
record * find_smallest_key( node * root, bool verbose );
record * find_biggest_key( node * root, bool verbose );
chunk *get_left_most_leaf( node * root, bool verbose );
chunk *get_right_most_leaf( node * root, bool verbose );

// Insertion.

node * bptinsert( node * root, size_t key, char *value, int worker_id );

// Deletion.

node * destroy_tree(node * root, int worker_id);
node * bptdelete( node * root, size_t key, int worker_id);

#ifdef _cplusplus
}
# endif

