/* 
 *  bpt.c  
 */
#define Version "1.12"
/*
 *
 *  bpt:  B+ Tree Implementation
 *  Copyright (C) 2010  Amittai Aviram  http://www.amittai.com
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 *  Author:  Amittai Aviram 
 *    http://www.amittai.com
 *    amittai.aviram@yale.edu or afa13@columbia.edu
 *    Department of Computer Science
 *    Yale University
 *    P. O. Box 208285
 *    New Haven, CT 06520-8285
 *  Date:  26 June 2010
 *  Last modified: 6 August 2011 
 *
 *  This implementation demonstrates the B+ tree data structure
 *  for educational purposes, includin insertion, deletion, search, and display
 *  of the search path, the leaves, or the whole tree.
 *  
 *  Must be compiled with a C99-compliant C compiler such as the latest GCC.
 *
 *  Usage:  bpt [order]
 *  where order is an optional argument
 *  (integer MIN_ORDER <= order <= MAX_ORDER)
 *  defined as the maximal number of pointers in any node.
 *
 */

#include "bpt.h"

// GLOBALS.

/* The order determines the maximum and minimum
 * number of entries (keys and pointers) in any
 * node.  Every node has at most order - 1 keys and
 * at least (roughly speaking) half that number.
 * Every leaf has as many pointers to data as keys,
 * and every internal node has one more pointer
 * to a subtree than the number of keys.
 * This global variable is initialized to the
 * default value.
 */
int order = DEFAULT_ORDER;

/* The queue is used to print the tree in
 * level order, starting from the root
 * printing each entire rank on a separate
 * line, finishing with the leaves.
 */
node * queue = NULL;

/* The user can toggle on and off the "verbose"
 * property, which causes the pointer addresses
 * to be printed out in hexadecimal notation
 * next to their corresponding keys.
 */
bool verbose_output = false;

// Global reader version is read by every thread at the beginning of 
// a read operation, and is passed with every object that goes
// to the garbage collector. Therefore, the garbage collector can
// check the version seen by the last completed read operation 
// for every thread, and free the objects that belong to older versions. 
unsigned long long global_version = 1;

// FUNCTION PROTOTYPES.

// Output and utility.

void enqueue( node * new_node );
node * dequeue( void );
int height( node * root );
size_t path_to_root( node * root, node * child );
void print_leaves( node * root );
void print_tree( node * root );
void find_and_print(node * root, size_t key, bool verbose); 
void find_and_print_range(node * root, size_t range1, size_t range2, bool verbose); 
size_t find_range( node * root, size_t key_start, size_t key_end, bool verbose,
		size_t returned_keys[], void * returned_pointers[]); 
node * find_leaf( node * root, size_t key, bool verbose );
size_t cut( int length );

// Insertion.

record * make_record(char *value);
node * make_node( void );
node * make_leaf( void );
size_t get_left_index(node * parent, node * left);
node * insert_into_leaf( node * leaf, size_t key, record * pointer, int worker_id );
node * insert_into_leaf_after_splitting(node * root, node * leaf, size_t key, record * pointer);
node * insert_into_node(node * root, node * parent, 
		size_t left_index, size_t key, node * right);
node * insert_into_node_after_splitting(node * root, node * parent, size_t left_index, 
		size_t key, node * right);
node * insert_into_parent(node * root, node * left, size_t key, node * right);
node * insert_into_new_root(node * left, size_t key, node * right);
node * start_new_tree(size_t key, record * pointer);

// Deletion.

size_t get_neighbor_index( node * n );
node * adjust_root(node * root,int worker_id);
node * coalesce_nodes(node * root, node * n, node * neighbor, size_t neighbor_index, size_t k_prime, int worker_id);
node * redistribute_nodes(node * root, node * n, node * neighbor, size_t neighbor_index, 
		size_t k_prime_index, size_t k_prime);
node * delete_entry( node * root, node * n, size_t key, void * pointer , int worker_id);


// FUNCTION DEFINITIONS.

/* Helper function for printing the
 * tree out.  See print_tree.
 */
void enqueue( node * new_node ) {
	node * c;
	if (queue == NULL) {
		queue = new_node;
		queue->next = NULL;
	}
	else {
		c = queue;
		while(c->next != NULL) {
			c = c->next;
		}
		c->next = new_node;
		new_node->next = NULL;
	}
}


/* Helper function for printing the
 * tree out.  See print_tree.
 */
node * dequeue( void ) {
	node * n = queue;
	queue = queue->next;
	n->next = NULL;
	return n;
}


/* Prints the bottom row of keys
 * of the tree (with their respective
 * pointers, if the verbose_output flag is set.
 */
void print_leaves( node * root ) {
	int i;
	node * c = root;
	if (root == NULL) {
		printf("Empty tree.\n");
		return;
	}
	while (!c->is_leaf)
		c = c->pointers[0];
	while (true) {
		for (i = 0; i < c->num_keys; i++) {
			if (verbose_output)
				printf("%lx ", (unsigned long)c->pointers[i]);
			printf("%zd ", c->keys[i]);
		}
		if (verbose_output)
			printf("%lx ", (unsigned long)c->pointers[order - 1]);
		if (c->pointers[order - 1] != NULL) {
			printf(" | ");
			c = c->pointers[order - 1];
		}
		else
			break;
	}
	printf("\n");
}


/* Utility function to give the height
 * of the tree, which length in number of edges
 * of the path from the root to any leaf.
 */
int height( node * root ) {
	int h = 0;
	node * c = root;
	while (!c->is_leaf) {
		c = c->pointers[0];
		h++;
	}
	return h;
}


/* Utility function to give the length in edges
 * of the path from any node to the root.
 */
size_t path_to_root( node * root, node * child ) {
	int length = 0;
	node * c = child;
	while (c != root) {
		c = c->parent;
		length++;
	}
	return length;
}


/* Prints the B+ tree in the command
 * line in level (rank) order, with the 
 * keys in each node and the '|' symbol
 * to separate nodes.
 * With the verbose_output flag set.
 * the values of the pointers corresponding
 * to the keys also appear next to their respective
 * keys, in hexadecimal notation.
 */
void print_tree( node * root ) {

	node * n = NULL;
	int i = 0;
	int rank = 0;
	int new_rank = 0;

	if (root == NULL) {
		printf("Empty tree.\n");
		return;
	}
	queue = NULL;
	enqueue(root);
	while( queue != NULL ) {
		n = dequeue();
		if (n->parent != NULL && n == n->parent->pointers[0]) {
			new_rank = path_to_root( root, n );
			if (new_rank != rank) {
				rank = new_rank;
				printf("\n");
			}
		}
		if (verbose_output) 
			printf("(%lx)", (unsigned long)n);
		for (i = 0; i < n->num_keys; i++) {
			if (verbose_output)
				printf("%lx ", (unsigned long)n->pointers[i]);
			printf("%zd ", n->keys[i]);
		}
		if (!n->is_leaf)
			for (i = 0; i <= n->num_keys; i++)
				enqueue(n->pointers[i]);
		if (verbose_output) {
			if (n->is_leaf) 
				printf("%lx ", (unsigned long)n->pointers[order - 1]);
			else
				printf("%lx ", (unsigned long)n->pointers[n->num_keys]);
		}
		printf("| ");
	}
	printf("\n");
}


/* Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print(node * root, size_t key, bool verbose) {
	record * r = find(root, key, -1);
	if (r == NULL)
		printf("Record not found under key %zd.\n", key);
	else 
		printf("Record at %lx -- key %zd, value %s.\n",
				(unsigned long)r, key, r->value);
}


/* Finds and prints the keys, pointers, and values within a range
 * of keys between key_start and key_end, including both bounds.
 */
void find_and_print_range( node * root, size_t key_start, size_t key_end,
		bool verbose ) {
	int i;
	size_t array_size = key_end - key_start + 1;
	size_t returned_keys[array_size];
	void * returned_pointers[array_size];
	size_t num_found = find_range( root, key_start, key_end, verbose,
			returned_keys, returned_pointers );
	if (!num_found)
		printf("None found.\n");
	else {
		for (i = 0; i < num_found; i++)
			printf("Key: %zd   Location: %lx  Value: %s\n",
					returned_keys[i],
					(unsigned long)returned_pointers[i],
					((record *)
					 returned_pointers[i])->value);
	}
}


/* Finds keys and their pointers, if present, in the range specified
 * by key_start and key_end, inclusive.  Places these in the arrays
 * returned_keys and returned_pointers, and returns the number of
 * entries found.
 */
size_t find_range( node * root, size_t key_start, size_t key_end, bool verbose,
		size_t returned_keys[], void * returned_pointers[]) {
	int i, num_found;
	num_found = 0;
	node * n = find_leaf( root, key_start, verbose );
	if (n == NULL) return 0;
	for (i = 0; i < n->num_keys && n->keys[i] < key_start; i++) ;
	if (i == n->num_keys) return 0;
	while (n != NULL) {
		for ( ; i < n->num_keys && n->keys[i] <= key_end; i++) {
			returned_keys[num_found] = n->keys[i];
			returned_pointers[num_found] = n->pointers[i];
			num_found++;
		}
		n = n->pointers[order - 1];
		i = 0;
	}
	return num_found;
}


/* Traces the path from the root to a leaf, searching
 * by key.  Displays information about the path
 * if the verbose flag is set.
 * Returns the leaf containing the given key.
 */
node * find_leaf( node * root, size_t key, bool verbose ) {
	int i = 0;
	node * c = root;
	if (c == NULL) {
		if (verbose) 
			printf("Empty tree.\n");
		return c;
	}
	while (!c->is_leaf) {
		if (verbose) {
			printf("[");
			for (i = 0; i < c->num_keys - 1; i++)
				printf("%zd ", c->keys[i]);
			printf("%zd] ", c->keys[i]);
		}
		i = 0;
		while (i < c->num_keys) {
			if (key >= c->keys[i]) i++;
			else break;
		}
		if (verbose)
			printf("%d ->\n", i);
		c = (node *)c->pointers[i];
	}
	if (verbose) {
		printf("Leaf [");
		for (i = 0; i < c->num_keys - 1; i++)
			printf("%zd ", c->keys[i]);
		printf("%zd] ->\n", c->keys[i]);
	}
	return c;
}

/* Traces the path from the root to the left-most leaf.
 * Displays information about the path
 * if the verbose flag is set.
 * Returns the left-most leaf.
 * Added by Koosha
 */
node * find_left_most_leaf( node * root, bool verbose ) {
	int i = 0;
	node * c = root;
	if (c == NULL) {
		if (verbose) 
			printf("Empty tree.\n");
		return c;
	}
	while (!c->is_leaf) {
		if (verbose) {
			printf("[");
			for (i = 0; i < c->num_keys - 1; i++)
				printf("%zd ", c->keys[i]);
			printf("%zd] ", c->keys[i]);
		}
		c = (node *)c->pointers[0];
	}
	if (verbose) {
		printf("Leaf [");
		for (i = 0; i < c->num_keys - 1; i++)
			printf("%zd ", c->keys[i]);
		printf("%zd] ->\n", c->keys[i]);
	}
	return c;
}

/* Traces the path from the root to the right-most leaf.
 * Displays information about the path
 * if the verbose flag is set.
 * Returns the left-most leaf.
 * Added by Koosha
 */
node * find_right_most_leaf( node * root, bool verbose ) {
	int i = 0;
	node * c = root;
	if (c == NULL) {
		if (verbose) 
			printf("Empty tree.\n");
		return c;
	}
	while (!c->is_leaf) {
		if (verbose) {
			printf("[");
			for (i = 0; i < c->num_keys - 1; i++)
				printf("%zd ", c->keys[i]);
			printf("%zd] ", c->keys[i]);
		}
		c = (node *)c->pointers[c->num_keys];
	}
	if (verbose) {
		printf("Leaf [");
		for (i = 0; i < c->num_keys - 1; i++)
			printf("%zd ", c->keys[i]);
		printf("%zd] ->\n", c->keys[i]);
	}
	return c;
}


/* Returns the records of the leftmost node 
 * Added by Koosha
 */
chunk * get_left_most_leaf( node * root, bool verbose ) {
	node * c = find_left_most_leaf( root, verbose );

	if (c == NULL) return NULL;
	if (0 == c->num_keys) 
		return NULL;
	
	// Improvement can be made be allocating only num_keys elements
	chunk *left_most_leaf=(chunk *)malloc(sizeof(chunk));
	left_most_leaf->value=(record **)calloc(order,sizeof(record *));
	left_most_leaf->key=(size_t *)malloc(order*sizeof(size_t));

	int i=0;
	for (i=0;i<c->num_keys;i++) {
		left_most_leaf->key[i]=c->keys[i];	
		left_most_leaf->value[i]=c->pointers[i];
	}
	left_most_leaf->value[i]=NULL;

	return (chunk *)left_most_leaf;
}

/* Returns the records of the rightmost node 
 * Added by Koosha
 */
chunk * get_right_most_leaf( node * root, bool verbose ) {
	node * c = find_right_most_leaf( root, verbose );

	if (c == NULL) return NULL;
	if (0 == c->num_keys) 
		return NULL;
	
	// Improvement can be made be allocating only num_keys elements
	chunk *right_most_leaf=(chunk *)malloc(sizeof(chunk));
	right_most_leaf->value=(record **)calloc(order,sizeof(record *));
	right_most_leaf->key=(size_t *)malloc(order*sizeof(size_t));

	int i=0;
	for (i=0;i<c->num_keys;i++) {
		right_most_leaf->key[i]=c->keys[i];
		right_most_leaf->value[i]=c->pointers[i];	
	}
	right_most_leaf->value[i]=NULL;

	return (chunk *)right_most_leaf;
}

/* Finds and returns the record to which
 * a key refers.
 */
record * find( node * root, size_t key, int thread_id ) {
	// Get the current version
	long long reader_version = global_version;

	int i = 0;
	node * c = find_leaf( root, key, false );
	if (c == NULL) return NULL;
	for (i = 0; i < c->num_keys; i++)
		if (c->keys[i] == key) break;
	if (i == c->num_keys) { 
		// update the last worked version for this thread
		if (thread_id>=0) {	
			last_reader_version[thread_id]=reader_version;
		}
		return NULL;
	}
	else {
		// update the last worked version for this thread
		if (thread_id>=0) {	
			last_reader_version[thread_id]=reader_version;
		}
		return (record *)c->pointers[i];
	}
}


/* Finds and returns the record 
 * with the smalles key
 * Added by Koosha
 */
record * find_smallest_key( node * root, bool verbose ) {
	node * c = find_left_most_leaf( root, verbose );
	if (c == NULL) return NULL;
	if (0 == c->num_keys) 
		return NULL;
	else
		return (record *)c->pointers[0];
}


/* Finds and returns the record 
 * with the smalles key
 * Added by Koosha
 */
record * find_biggest_key( node * root, bool verbose ) {
	node * c = find_right_most_leaf( root, verbose );
	if (c == NULL) return NULL;
	if (0 == c->num_keys) 
		return NULL;
	else
		return (record *)c->pointers[c->num_keys-1];
}

/* Finds the appropriate place to
 * split a node that is too big into two.
 */
size_t cut( int length ) {
	if (length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}


// INSERTION

/* Creates a new record to hold the value
 * to which a key refers.
 */
record * make_record(char *value) {
	record * new_record = (record *)malloc(sizeof(record));
	if (new_record == NULL) {
		perror("Record creation.");
		exit(EXIT_FAILURE);
	}
	else {
		new_record->value = value;
	}
	return new_record;
}


/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
node * make_node( void ) {
	node * new_node;
	new_node = malloc(sizeof(node));
	if (new_node == NULL) {
		perror("Node creation.");
		exit(EXIT_FAILURE);
	}
	new_node->keys = malloc( (order - 1) * sizeof(size_t) );
	if (new_node->keys == NULL) {
		perror("New node keys array.");
		exit(EXIT_FAILURE);
	}
	new_node->pointers = malloc( order * sizeof(void *) );
	if (new_node->pointers == NULL) {
		perror("New node pointers array.");
		exit(EXIT_FAILURE);
	}
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->next = NULL;
	return new_node;
}

/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
node * make_leaf( void ) {
	node * leaf = make_node();
	leaf->is_leaf = true;
	return leaf;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to 
 * the node to the left of the key to be inserted.
 */
size_t get_left_index(node * parent, node * left) {

	size_t left_index = 0;
	while (left_index <= parent->num_keys && 
			parent->pointers[left_index] != left)
		left_index++;
	return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
node * insert_into_leaf( node * leaf, size_t key, record * pointer, int worker_id ) {

	int i, insertion_point;

	insertion_point = 0;
	while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
		insertion_point++;

	// If the key exists, update it
	if (insertion_point < leaf->num_keys && leaf->keys[insertion_point]==key) {
		record * r = leaf->pointers[insertion_point];
		leaf->pointers[insertion_point] = pointer;
		//free(r->value);
		//free(r);
		add_garbage(r->value , global_version, worker_id);
		add_garbage(r, global_version, worker_id);
		++global_version;
	}
	// If the key doesn't exist, insert it
	else {
		for (i = leaf->num_keys; i > insertion_point; i--) {
			leaf->keys[i] = leaf->keys[i - 1];
			leaf->pointers[i] = leaf->pointers[i - 1];
		}
		leaf->keys[insertion_point] = key;
		leaf->pointers[insertion_point] = pointer;
		leaf->num_keys++;
	}
	return leaf;
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
node * insert_into_leaf_after_splitting(node * root, node * leaf, size_t key, record * pointer) {

	node * new_leaf;
	size_t * temp_keys;
	void ** temp_pointers;
	size_t insertion_index, split, new_key, i, j;

	new_leaf = make_leaf();

	temp_keys = malloc( order * sizeof(size_t) );
	if (temp_keys == NULL) {
		perror("Temporary keys array.");
		exit(EXIT_FAILURE);
	}

	temp_pointers = malloc( order * sizeof(void *) );
	if (temp_pointers == NULL) {
		perror("Temporary pointers array.");
		exit(EXIT_FAILURE);
	}

	insertion_index = 0;
	while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index) j++;
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = leaf->pointers[i];
	}

	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = pointer;

	leaf->num_keys = 0;

	split = cut(order - 1);

	for (i = 0; i < split; i++) {
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
	}

	for (i = split, j = 0; i < order; i++, j++) {
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
	}

	free(temp_pointers);
	free(temp_keys);
	//add_garbage(temp_pointers,global_version);
	//add_garbage(temp_keys,global_version);
	//++global_version;
	
	new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
	leaf->pointers[order - 1] = new_leaf;

	for (i = leaf->num_keys; i < order - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->pointers[i] = NULL;

	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];

	return insert_into_parent(root, leaf, new_key, new_leaf);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
node * insert_into_node(node * root, node * n, 
		size_t left_index, size_t key, node * right) {
	int i;

	for (i = n->num_keys; i > left_index; i--) {
		n->pointers[i + 1] = n->pointers[i];
		n->keys[i] = n->keys[i - 1];
	}
	n->pointers[left_index + 1] = right;
	n->keys[left_index] = key;
	n->num_keys++;
	return root;
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
node * insert_into_node_after_splitting(node * root, node * old_node, size_t left_index, 
		size_t key, node * right) {

	int i, j, split;
	size_t k_prime;
	node * new_node, * child;
	size_t * temp_keys;
	node ** temp_pointers;

	/* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */

	temp_pointers = malloc( (order + 1) * sizeof(node *) );
	if (temp_pointers == NULL) {
		perror("Temporary pointers array for splitting nodes.");
		exit(EXIT_FAILURE);
	}
	temp_keys = malloc( order * sizeof(size_t) );
	if (temp_keys == NULL) {
		perror("Temporary keys array for splitting nodes.");
		exit(EXIT_FAILURE);
	}

	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1) j++;
		temp_pointers[j] = old_node->pointers[i];
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index) j++;
		temp_keys[j] = old_node->keys[i];
	}

	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;

	/* Create the new node and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */  
	split = cut(order);
	new_node = make_node();
	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
	}
	old_node->pointers[i] = temp_pointers[i];
	k_prime = temp_keys[split - 1];
	for (++i, j = 0; i < order; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
		new_node->num_keys++;
	}
	new_node->pointers[j] = temp_pointers[i];
	free(temp_pointers);
	free(temp_keys);
	//add_garbage(temp_pointers,global_version);
	//add_garbage(temp_keys,global_version);
	//++global_version;

	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		child = new_node->pointers[i];
		child->parent = new_node;
	}

	/* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */

	return insert_into_parent(root, old_node, k_prime, new_node);
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
node * insert_into_parent(node * root, node * left, size_t key, node * right) {

	size_t left_index;
	node * parent;

	parent = left->parent;

	/* Case: new root. */

	if (parent == NULL)
		return insert_into_new_root(left, key, right);

	/* Case: leaf or node. (Remainder of
	 * function body.)  
	 */

	/* Find the parent's pointer to the left 
	 * node.
	 */

	left_index = get_left_index(parent, left);


	/* Simple case: the new key fits into the node. 
	 */

	if (parent->num_keys < order - 1)
		return insert_into_node(root, parent, left_index, key, right);

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */

	return insert_into_node_after_splitting(root, parent, left_index, key, right);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
node * insert_into_new_root(node * left, size_t key, node * right) {

	node * root = make_node();
	root->keys[0] = key;
	root->pointers[0] = left;
	root->pointers[1] = right;
	root->num_keys++;
	root->parent = NULL;
	left->parent = root;
	right->parent = root;
	return root;
}



/* First insertion:
 * start a new tree.
 */
node * start_new_tree(size_t key, record * pointer) {

	node * root = make_leaf();
	root->keys[0] = key;
	root->pointers[0] = pointer;
	root->pointers[order - 1] = NULL;
	root->parent = NULL;
	root->num_keys++;
	return root;
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
node * bptinsert( node * root, size_t key, char *value , int worker_id) {

	record * pointer;
	node * leaf;

	/* The current implementation ignores
	 * duplicates.
	 */

	//if (find(root, key, -1) != NULL)
	//	return root;

	/* Create a new record for the
	 * value.
	 */
	pointer = make_record(value);


	/* Case: the tree does not exist yet.
	 * Start a new tree.
	 */

	if (root == NULL) 
		return start_new_tree(key, pointer);


	/* Case: the tree already exists.
	 * (Rest of function body.)
	 */

	leaf = find_leaf(root, key, false);

	/* Case: leaf has room for key and pointer.
	 */

	if (leaf->num_keys < order - 1) {
		leaf = insert_into_leaf(leaf, key, pointer, worker_id);
		return root;
	}


	/* Case:  leaf must be split.
	 */

	return insert_into_leaf_after_splitting(root, leaf, key, pointer);
}




// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */
size_t get_neighbor_index( node * n ) {

	int i;

	/* Return the index of the key to the left
	 * of the pointer in the parent pointing
	 * to n.  
	 * If n is the leftmost child, this means
	 * return -1.
	 */
	for (i = 0; i <= n->parent->num_keys; i++)
		if (n->parent->pointers[i] == n)
			return i - 1;

	// Error state.
	printf("Search for nonexistent pointer to node in parent.\n");
	printf("Node:  %#lx\n", (unsigned long)n);
	exit(EXIT_FAILURE);
}


node * remove_entry_from_node(node * n, size_t key, node * pointer) {

	int i, num_pointers;

	// Remove the key and shift other keys accordingly.
	i = 0;
	while (n->keys[i] != key)
		i++;
	for (++i; i < n->num_keys; i++)
		n->keys[i - 1] = n->keys[i];

	// Remove the pointer and shift other pointers accordingly.
	// First determine number of pointers.
	num_pointers = n->is_leaf ? n->num_keys : n->num_keys + 1;
	i = 0;
	while (n->pointers[i] != pointer)
		i++;
	for (++i; i < num_pointers; i++)
		n->pointers[i - 1] = n->pointers[i];


	// One key fewer.
	n->num_keys--;

	// Set the other pointers to NULL for tidiness.
	// A leaf uses the last pointer to point to the next leaf.
	if (n->is_leaf)
		for (i = n->num_keys; i < order - 1; i++)
			n->pointers[i] = NULL;
	else
		for (i = n->num_keys + 1; i < order; i++)
			n->pointers[i] = NULL;

	return n;
}


node * adjust_root(node * root, int worker_id) {

	node * new_root;

	/* Case: nonempty root.
	 * Key and pointer have already been deleted,
	 * so nothing to be done.
	 */

	if (root->num_keys > 0)
		return root;

	/* Case: empty root. 
	 */

	// If it has a child, promote 
	// the first (only) child
	// as the new root.

	if (!root->is_leaf) {
		new_root = root->pointers[0];
		new_root->parent = NULL;
	}

	// If it is a leaf (has no children),
	// then the whole tree is empty.

	else
		new_root = NULL;

	//free(root->keys);
	//free(root->pointers);
	//free(root);
	add_garbage(root->keys,global_version,worker_id);
	add_garbage(root->pointers,global_version,worker_id);
	add_garbage(root,global_version,worker_id);
	++global_version;

	return new_root;
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */
node * coalesce_nodes(node * root, node * n, node * neighbor, size_t neighbor_index, size_t k_prime, int worker_id) {

	size_t i, j, neighbor_insertion_index, n_start, n_end;
   	size_t new_k_prime;
	node * tmp;
	bool split;

	/* Swap neighbor with node if node is on the
	 * extreme left and neighbor is to its right.
	 */

	if (neighbor_index == -1) {
		tmp = n;
		n = neighbor;
		neighbor = tmp;
	}

	/* Starting point in the neighbor for copying
	 * keys and pointers from n.
	 * Recall that n and neighbor have swapped places
	 * in the special case of n being a leftmost child.
	 */

	neighbor_insertion_index = neighbor->num_keys;

	/*
	 * Nonleaf nodes may sometimes need to remain split,
	 * if the insertion of k_prime would cause the resulting
	 * single coalesced node to exceed the limit order - 1.
	 * The variable split is always false for leaf nodes
	 * and only sometimes set to true for nonleaf nodes.
	 */

	split = false;

	/* Case:  nonleaf node.
	 * Append k_prime and the following pointer.
	 * If there is room in the neighbor, append
	 * all pointers and keys from the neighbor.
	 * Otherwise, append only cut(order) - 2 keys and
	 * cut(order) - 1 pointers.
	 */

	if (!n->is_leaf) {

		/* Append k_prime.
		 */

		neighbor->keys[neighbor_insertion_index] = k_prime;
		neighbor->num_keys++;


		/* Case (default):  there is room for all of n's keys and pointers
		 * in the neighbor after appending k_prime.
		 */

		n_end = n->num_keys;

		/* Case (special): k cannot fit with all the other keys and pointers
		 * into one coalesced node.
		 */
		n_start = 0; // Only used in this special case.
		if (n->num_keys + neighbor->num_keys >= order) {
			split = true;
			n_end = cut(order) - 2;
		}

		for (i = neighbor_insertion_index + 1, j = 0; j < n_end; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
			n->num_keys--;
			n_start++;
		}

		/* The number of pointers is always
		 * one more than the number of keys.
		 */

		neighbor->pointers[i] = n->pointers[j];

		/* If the nodes are still split, remove the first key from
		 * n.
		 */
		if (split) {
			new_k_prime = n->keys[n_start];
			for (i = 0, j = n_start + 1; i < n->num_keys; i++, j++) {
				n->keys[i] = n->keys[j];
				n->pointers[i] = n->pointers[j];
			}
			n->pointers[i] = n->pointers[j];
			n->num_keys--;
		}

		/* All children must now point up to the same parent.
		 */

		for (i = 0; i < neighbor->num_keys + 1; i++) {
			tmp = (node *)neighbor->pointers[i];
			tmp->parent = neighbor;
		}
	}

	/* In a leaf, append the keys and pointers of
	 * n to the neighbor.
	 * Set the neighbor's last pointer to point to
	 * what had been n's right neighbor.
	 */

	else {
		for (i = neighbor_insertion_index, j = 0; j < n->num_keys; i++, j++) {
			neighbor->keys[i] = n->keys[j];
			neighbor->pointers[i] = n->pointers[j];
			neighbor->num_keys++;
		}
		neighbor->pointers[order - 1] = n->pointers[order - 1];
	}

	if (!split) {
		root = delete_entry(root, n->parent, k_prime, n, worker_id);
		//free(n->keys);
		//free(n->pointers);
		//free(n); 
		add_garbage(n->keys,global_version,worker_id);
		add_garbage(n->pointers,global_version,worker_id);
		add_garbage(n,global_version,worker_id);
		++global_version;
	}
	else
		for (i = 0; i < n->parent->num_keys; i++)
			if (n->parent->pointers[i + 1] == n) {
				n->parent->keys[i] = new_k_prime;
				break;
			}

	return root;

}


/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
 */
node * redistribute_nodes(node * root, node * n, node * neighbor, size_t neighbor_index, 
		size_t k_prime_index, size_t k_prime) {  

	int i;
	node * tmp;

	/* Case: n has a neighbor to the left. 
	 * Pull the neighbor's last key-pointer pair over
	 * from the neighbor's right end to n's left end.
	 */

	if (neighbor_index != -1) {
		if (!n->is_leaf)
			n->pointers[n->num_keys + 1] = n->pointers[n->num_keys];
		for (i = n->num_keys; i > 0; i--) {
			n->keys[i] = n->keys[i - 1];
			n->pointers[i] = n->pointers[i - 1];
		}
		if (!n->is_leaf) {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys];
			tmp = (node *)n->pointers[0];
			tmp->parent = n;
			neighbor->pointers[neighbor->num_keys] = NULL;
			n->keys[0] = k_prime;
			n->parent->keys[k_prime_index] = neighbor->keys[neighbor->num_keys - 1];
		}
		else {
			n->pointers[0] = neighbor->pointers[neighbor->num_keys - 1];
			neighbor->pointers[neighbor->num_keys - 1] = NULL;
			n->keys[0] = neighbor->keys[neighbor->num_keys - 1];
			n->parent->keys[k_prime_index] = n->keys[0];
		}
	}

	/* Case: n is the leftmost child.
	 * Take a key-pointer pair from the neighbor to the right.
	 * Move the neighbor's leftmost key-pointer pair
	 * to n's rightmost position.
	 */

	else {  
		if (n->is_leaf) {
			n->keys[n->num_keys] = neighbor->keys[0];
			n->pointers[n->num_keys] = neighbor->pointers[0];
			n->parent->keys[k_prime_index] = neighbor->keys[1];
		}
		else {
			n->keys[n->num_keys] = k_prime;
			n->pointers[n->num_keys + 1] = neighbor->pointers[0];
			tmp = (node *)n->pointers[n->num_keys + 1];
			tmp->parent = n;
			n->parent->keys[k_prime_index] = neighbor->keys[0];
		}
		for (i = 0; i < neighbor->num_keys; i++) {
			neighbor->keys[i] = neighbor->keys[i + 1];
			neighbor->pointers[i] = neighbor->pointers[i + 1];
		}
		if (!n->is_leaf)
			neighbor->pointers[i] = neighbor->pointers[i + 1];
	}

	/* n now has one more key and one more pointer;
	 * the neighbor has one fewer of each.
	 */

	n->num_keys++;
	neighbor->num_keys--;

	return root;
}


/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate
 * changes to preserve the B+ tree properties.
 */
node * delete_entry( node * root, node * n, size_t key, void * pointer ,int worker_id) {

	int min_keys;
	node * neighbor;
	size_t neighbor_index;
	size_t k_prime_index;
	size_t k_prime;
	int capacity;

	// Remove key and pointer from node.

	n = remove_entry_from_node(n, key, pointer);

	/* Case:  deletion from the root. 
	 */

	if (n == root) 
		return adjust_root(root,worker_id);


	/* Case:  deletion from a node below the root.
	 * (Rest of function body.)
	 */

	/* Determine minimum allowable size of node,
	 * to be preserved after deletion.
	 */

	min_keys = n->is_leaf ? cut(order - 1) : cut(order) - 1;

	/* Case:  node stays at or above minimum.
	 * (The simple case.)
	 */

	if (n->num_keys >= min_keys)
		return root;

	/* Case:  node falls below minimum.
	 * Either coalescence or redistribution
	 * is needed.
	 */

	/* Find the appropriate neighbor node with which
	 * to coalesce.
	 * Also find the key (k_prime) in the parent
	 * between the pointer to node n and the pointer
	 * to the neighbor.
	 */

	neighbor_index = get_neighbor_index( n );
	k_prime_index = neighbor_index == -1 ? 0 : neighbor_index;
	k_prime = n->parent->keys[k_prime_index];
	neighbor = neighbor_index == -1 ? n->parent->pointers[1] : 
		n->parent->pointers[neighbor_index];

	capacity = n->is_leaf ? order : order - 1;

	/* Coalescence. */

	if (neighbor->num_keys + n->num_keys < capacity)
		return coalesce_nodes(root, n, neighbor, neighbor_index, k_prime, worker_id);

	/* Redistribution. */

	else
		return redistribute_nodes(root, n, neighbor, neighbor_index, k_prime_index, k_prime);
}



/* Master deletion function.
 */
node * bptdelete(node * root, size_t key, int worker_id) {

	node * key_leaf;
	record * key_record;

	key_record = find(root, key, -1);
	key_leaf = find_leaf(root, key, false);
	if (key_record != NULL && key_leaf != NULL) {
		root = delete_entry(root, key_leaf, key, key_record, worker_id);
		//free(key_record);
		add_garbage(key_record->value,global_version,worker_id);
		//add_garbage(key_record,global_version,worker_id);
		++global_version;
	}
	return root;
}


void destroy_tree_nodes(node * root, int worker_id) {
	int i;
	if (root->is_leaf)
		for (i = 0; i < root->num_keys; i++) {
			//free(root->pointers[i]);
			add_garbage(((record *)root->pointers[i])->value,global_version,worker_id);
			add_garbage(root->pointers[i],global_version,worker_id);
			++global_version;
		}			
	else
		for (i = 0; i < root->num_keys + 1; i++)
			destroy_tree_nodes(root->pointers[i], worker_id);
	//free(root->pointers);
	//free(root->keys);
	//free(root);
	add_garbage(root->pointers,global_version,worker_id);
	add_garbage(root->keys,global_version,worker_id);
	add_garbage(root,global_version,worker_id);
	++global_version;
}


node * destroy_tree(node * root, int worker_id) {
	destroy_tree_nodes(root, worker_id);
	return NULL;
}

