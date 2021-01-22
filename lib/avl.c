// disable assert() unless building test main
#ifndef _AVL_MAIN
#ifndef NDEBUG
#define NDEBUG
#endif
#endif

#include"avl.h"

#include<assert.h>
#include<errno.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>

#include"static_assert.h"

struct _avl_tree_t{
	void *nextpage;    // lines up with node->key, ptr to next page, if allocated
	avl_node_t page;   // current active page
	avl_node_t root;   // current root node
	avl_node_t trash;  // trash stack of released nodes
	avl_node_t target; // holds node ptr of target node of last avl_insert/avl_delete
	avl_cmp_func cmp;  // key compare function
	void *userdata;    // user data to pass to cmp()
	size_t size;       // current number of nodes in tree
	int idx;           // next available node from current page
	int pages;         // number of allocated pages
	size_t bs;
};

struct _avl_node_t{
	// store user-supplied key
	// also: lines up with tree->nextpage: first node on page stores next page ptr here
	// also: must remain first entry in struct: avl_insert/avl_delete return node ptr cast to void **
	void *key;

	// current height of node
	int height;

	// left/right children
	// also, child[1] is used in the trash stack
	avl_node_t child[2];
};

// page allocation foo below assumes node struct size is a multiple of pointer size
STATIC_ASSERT(sizeof(struct _avl_node_t) % sizeof(void *) == 0, "bad avl node struct size" );

static unsigned long _avl_pg = 0;

void avl_reset(avl_tree_t tree){
	tree->root = tree->trash = NULL;
	tree->page = (avl_node_t) tree;
	// first page holds tree object itself - set idx to next available node
	tree->idx = 1 + (sizeof(struct _avl_tree_t) + sizeof(struct _avl_node_t) - 1) / sizeof(struct _avl_node_t);
	tree->size = 0;
}

static inline void _avl_release(avl_tree_t tree){
	void **page, **nextpage;
	page = tree->nextpage;
	while(page){
		nextpage = *page;
		free(page);
		page = nextpage;
	}
}

void avl_release(avl_tree_t tree){
	avl_reset(tree);
	_avl_release(tree);
	tree->nextpage = NULL;
}

void avl_free(avl_tree_t tree){
	_avl_release(tree);
	free(tree);
}

static inline int _cmp_ptr(unsigned char *a, unsigned char *b){
	return a < b ? -1 : a > b;
}

static int cmp_ptr(void *a, void *b, void *userdata){
	(void)userdata;
	return _cmp_ptr(a, b);
}

static inline void *_avl_page_alloc(size_t bs){
//	return malloc(bs);
	void *page;
	if((errno = posix_memalign(&page, _avl_pg, bs)))
		page = NULL;
	return page;
}

avl_tree_t avl_new2(avl_cmp_func cmp, void *userdata, size_t nodes){
	avl_tree_t tree = NULL;

	// initialize page size
	unsigned long sz = _avl_pg;
	if(!sz){
		sz = sysconf(_SC_PAGESIZE);
		// I don't think the above can fail, but just in case...
		if(sz < sizeof(*tree))
			sz = 4096;
		_avl_pg = sz;
	}
	size_t bs = nodes ? sizeof(struct _avl_node_t) * nodes : sz;
	bs = ((bs + sz - 1) / sz) * sz;

	//bs = bs ? ((bs + sz - 1) / sz) * sz : sz;
	tree = (avl_tree_t) _avl_page_alloc(bs);
	if(tree){
		avl_reset(tree);
		tree->page->key = NULL;
		tree->cmp = cmp ? cmp : cmp_ptr;
		tree->pages = 1;
		tree->bs = bs;
		tree->userdata = userdata;
	}
	return tree;
}

avl_tree_t avl_new(avl_cmp_func cmp, void *userdata){
	return avl_new2(cmp, userdata, 0);
}

static inline void _avl_release_node(avl_tree_t tree, avl_node_t n){
	// build trash stack, using n->child[1] as ptr to next
	// specifically, don't touch n->key, so that it remains
	// valid after a delete so that the client can deal with it
	n->child[1] = tree->trash;
	tree->trash = n;
	tree->size--;
}

// previously used memory is kept in a linked list - step to next item
static inline avl_node_t _avl_alloc_node(avl_tree_t tree){
	// try to pop node off of trash stack
	avl_node_t n = tree->trash;
	if(n){
		// success! adjust trash head ptr
		tree->trash = n->child[1];
	}else{
		// else try to pull from current page
		n = tree->page + tree->idx++;
		// but if it ran off the end ...
		if(sizeof(*n) * tree->idx > tree->bs){
			// allocate and assign next page if it wasn't previously allocated
			if(!tree->page->key){
				// extend the page linked list
				tree->page->key = _avl_page_alloc(tree->bs);
				if(tree->page->key){
					((avl_node_t)(tree->page->key))->key = NULL;
					tree->pages++;
				}else{
					tree->idx--;
					return NULL;
				}
			}
			// advance to next page
			tree->page = tree->page->key;
			// zero index is burned for page linking
			n = tree->page + 1;
			tree->idx = 2;
		}
	}
	n->child[0] = n->child[1] = NULL;
	tree->size++;
	return n;
}

static inline int _avl_max(int a, int b){
	return a > b ? a : b;
}

static inline int _avl_height(avl_node_t n){
	return n ? n->height : -1;
}

static inline avl_node_t _avl_rotate(avl_node_t a, const int target){
	avl_node_t b = a->child[!target];
	a->child[!target] = b->child[target];
	b->child[target] = a;
	a->height = _avl_max(_avl_height(a->child[0]), _avl_height(a->child[1])) + 1;
	b->height = _avl_max(_avl_height(b->child[!target]), a->height) + 1;
	return b;
}

static inline avl_node_t _avl_balance(avl_node_t n, const int target){
	n->height = _avl_max(_avl_height(n->child[target]), _avl_height(n->child[!target])) + 1;
	const int balance = _avl_height(n->child[target]) - _avl_height(n->child[!target]);
	assert(balance > -2 && balance < 3);
	if(2 == balance){
		if(_avl_height(n->child[target]->child[!target]) > _avl_height(n->child[target]->child[target]))
			n->child[target] = _avl_rotate(n->child[target], target);
		n = _avl_rotate(n, !target);
	}
	return n;
}

static inline avl_node_t _avl_prune_min(avl_node_t n, avl_node_t *trimmed){
	if(n->child[0]){
		n->child[0] = _avl_prune_min(n->child[0], trimmed);
		return _avl_balance(n, 1);
	}
	*trimmed = n;
	return n->child[1];
}

static inline avl_node_t __avl_insert(avl_tree_t tree, void *key, avl_node_t n, void * const userdata){
	if(n){
		const int cmp = tree->cmp(key, n->key, userdata);
		if(cmp){
			const int target = (cmp > 0);
			avl_node_t child = __avl_insert(tree, key, n->child[target], userdata);
			if(child){
				n->child[target] = child;
				n = _avl_balance(n, target);
			}else{
				n = NULL;
			}
		}else{
			tree->target = n;
		}
	}else{
		n = tree->target = _avl_alloc_node(tree);
		if(n){
			n->key = key;
			n->height = 0;
		}
	}
	return n;
}

static inline avl_node_t __avl_find(avl_tree_t tree, void *key, avl_node_t n, void * const userdata){
	if(n){
		const int cmp = tree->cmp(key, n->key, userdata);
		if(cmp){
			const int target = (cmp > 0);
			avl_node_t child = __avl_find(tree, key, n->child[target], userdata);
			if(child){
				n->child[target] = child;
				n = _avl_balance(n, target);
			}else{
				n = NULL;
			}
		}else{
			tree->target = n;
		}
	}
	return n;
}

static inline avl_node_t __avl_snip(avl_tree_t tree, avl_node_t n){
	avl_node_t orig = tree->target = n;
	if(orig->child[1]){
		avl_node_t next, next_right;
		next_right = _avl_prune_min(orig->child[1], &next);
		next->child[1] = next_right;
		next->child[0] = orig->child[0];
		n = _avl_balance(next, 0);
	}else if((n = orig->child[0])){
		n = _avl_balance(n, 1);
	}
	_avl_release_node(tree, orig);
	return n;
}

static avl_node_t __avl_delete(avl_tree_t tree, void *key, avl_node_t n, void *userdata){
	if(n){
		const int cmp = tree->cmp(key, n->key, userdata);
		if(cmp){
			const int target = (cmp > 0);
			n->child[target] = __avl_delete(tree, key, n->child[target], userdata);
			n = _avl_balance(n, !target);
		}else{
			n = __avl_snip(tree, n);
		}
	}
	return n;
}

static avl_node_t __avl_pop(avl_tree_t tree, avl_node_t n, int target){
	if(n->child[target]){
		n->child[target] = __avl_pop(tree, n->child[target], target);
		n = _avl_balance(n, !target);
	}else{
		n = __avl_snip(tree, n);
	}
	return n;
}

static avl_node_t __avl_peek(avl_tree_t tree, avl_node_t n, int target){
	if(n->child[target]){
		n->child[target] = __avl_peek(tree, n->child[target], target);
		n = _avl_balance(n, !target);
	}else{
		tree->target = n;
	}
	return n;
}

static int _avl_walk(avl_node_t n, avl_walk_cb cb, void *cb_data, const int dir){
	int halt = 0;
	if(!n)
		return halt;
	if((halt = _avl_walk(n->child[dir], cb, cb_data, dir)))
		return halt;
	if((halt = cb((void **)n, cb_data)))
		return halt;
	return halt = _avl_walk(n->child[!dir], cb, cb_data, dir);
}

static int _avl_range(avl_cmp_func cmp, avl_node_t n, void *key, void *userdata, avl_walk_cb cb, void *cb_data, const int dir, int chk){
	int halt = 0;
	if(!n)
		return halt;
	if(cmp(key, n->key, userdata) * (dir ? -1 : 1) <= 0){
		if((halt = _avl_range(cmp, n->child[dir], key, userdata, cb, cb_data, dir, chk)))
			return halt;
		if((halt = cb((void **)n, cb_data)))
			return halt;
	}else{
		chk++;
	}
	// if parent node and current node was >= search key, all children will be >= search key too
	if(chk > 1)
		return halt = _avl_walk(n->child[!dir], cb, cb_data, dir);
	return halt = _avl_range(cmp, n->child[!dir], key, userdata, cb, cb_data, dir, chk);
}

static inline void **_avl_insert(avl_tree_t tree, void *key, void *userdata){
	tree->target = NULL;
	avl_node_t newroot = __avl_insert(tree, key, tree->root, userdata);
	if(newroot)
		tree->root = newroot;
	return (void **) tree->target;
}

static inline void **_avl_find(avl_tree_t tree, void *key, void *userdata){
	tree->target = NULL;
	avl_node_t newroot = __avl_find(tree, key, tree->root, userdata);
	if(newroot)
		tree->root = newroot;
	return (void **) tree->target;
}

static inline void **_avl_delete(avl_tree_t tree, void *key, void *userdata){
	tree->target = NULL;
	tree->root = __avl_delete(tree, key, tree->root, userdata);
	return (void **) tree->target;
}

static inline void **_avl_pop(avl_tree_t tree, int tail){
	tree->target = NULL;
	if(tree->root)
		tree->root = __avl_pop(tree, tree->root, tail);
	return (void **) tree->target;
}

static inline void **_avl_peek(avl_tree_t tree, int tail){
	tree->target = NULL;
	if(tree->root)
		tree->root = __avl_peek(tree, tree->root, tail);
	return (void **) tree->target;
}

void **avl_insert(avl_tree_t tree, void *key){
	return _avl_insert(tree, key, tree->userdata);
}

void **avl_insert2(avl_tree_t tree, void *key, void *userdata){
	return  _avl_insert(tree, key, userdata);
}

void **avl_find(avl_tree_t tree, void *key){
	return _avl_find(tree, key, tree->userdata);
}

void **avl_find2(avl_tree_t tree, void *key, void *userdata){
	return _avl_find(tree, key, userdata);
}

void **avl_delete(avl_tree_t tree, void *key){
	return _avl_delete(tree, key, tree->userdata);
}

void **avl_delete2(avl_tree_t tree, void *key, void *userdata){
	return _avl_delete(tree, key, userdata);
}

void **avl_pop(avl_tree_t tree, int tail){
	return _avl_pop(tree, tail ? 1 : 0);
}

void **avl_peek(avl_tree_t tree, int tail){
	return _avl_peek(tree, tail ? 1 : 0);
}

void **avl_root(avl_tree_t tree){
	return (void **)tree->root;
}

int avl_walk(avl_tree_t tree, avl_walk_cb cb, void *userdata, int desc){
	return _avl_walk(tree->root, cb, userdata, desc ? 1 : 0);
}

int avl_range(avl_tree_t tree, void *key, avl_walk_cb cb, void *cb_data, int desc){
	return _avl_range(tree->cmp, tree->root, key, tree->userdata, cb, cb_data, desc ? 1 : 0, 0);
}

int avl_range2(avl_tree_t tree, void *key, void *userdata, avl_walk_cb cb, void *cb_data, int desc){
	return _avl_range(tree->cmp, tree->root, key, userdata, cb, cb_data, desc ? 1 : 0, 0);
}

int avl_height(avl_tree_t tree){
	return _avl_height(tree->root);
}

size_t avl_size(avl_tree_t tree){
	return tree->size;
}

size_t avl_mem(avl_tree_t tree){
	return tree->pages * tree->bs;
}

int avl_pages(avl_tree_t tree){
	return tree->pages;
}

int avl_node_height(void **n){
	return _avl_height((avl_node_t)n);
}

#ifdef _AVL_MAIN

#include<stdio.h>
#include<time.h>

static int my_cmp(void *a, void *b, void *userdata){
	return (long)a > (long)b ? 1 : (long)a < (long) b ? -1 : 0;
}

static int walk_cb(void **key, void *userdata){
	printf("%*.0s%ld\n", 4 * avl_node_height(key), "", (long)(*key));
	return 0;
}

static int walk_range_cb(void **key, void *userdata){
	printf(" %ld", (long)(*key));
	return 0;
}

int main(int argc, char **argv){
	avl_tree_t tree = avl_new2(my_cmp, NULL, 10000);
	assert(tree);
	intptr_t t[] = { 9, 5, 10, 0, 6, 11, -1, 1, 2 };
	const int tc = sizeof(t) / sizeof(*t);
	printf("=== load/walk tree (asc) ===\n");
	{
		int i;
		for(i = 0; i < tc; i++)
			avl_insert(tree, (void *)t[i]);
	}
/*	avl_insert(tree, (void *)9);
	avl_insert(tree, (void *)5);
	avl_insert(tree, (void *)10);
	avl_insert(tree, (void *)0);
	avl_insert(tree, (void *)6);
	avl_insert(tree, (void *)11);
	avl_insert(tree, (void *)-1);
	avl_insert(tree, (void *)1);
	avl_insert(tree, (void *)2);*/
	avl_walk(tree, walk_cb, NULL, 0);

	printf("=== walk tree range (asc) ===\n");
	{
		int i;
		for(i = 0; i < tc; i++){
			printf("%ld(0):", t[i]);
			avl_range(tree, (void *)t[i], walk_range_cb, NULL, 0);
			printf("\n%ld(1):", t[i]);
			avl_range(tree, (void *)t[i], walk_range_cb, NULL, 1);
			printf("\n");
		}
		// now try it w/ a value not present in the tree
		printf("%d(0):", 3);
		avl_range(tree, (void *)3, walk_range_cb, NULL, 0);
		printf("\n%d(1):", 3);
		avl_range(tree, (void *)3, walk_range_cb, NULL, 1);
		printf("\n");
	}

	printf("=== delete item, walk tree (desc) ===\n");
	avl_delete(tree, (void *)10);
	avl_walk(tree, walk_cb, NULL, 1);

	printf("=== pop from alternating ends ===\n");
	void **found;
	int flag = 0;
	while((found = avl_pop(tree, flag))){
		printf("%d\n", (int)(intptr_t)*found);
		flag = !flag;
	}
	avl_reset(tree);

	int i = 1, count = 500000, loops = 1, seed = 1;

	if(i < argc) count = atoi(argv[i++]);
	if(i < argc) loops = atoi(argv[i++]);
	if(i < argc) seed = atoi(argv[i++]);

	srand(seed);

	long *buf = malloc(sizeof(*buf) * count);
	for(i = 0; i < count; i++)
		buf[i] = rand();

	printf("=== load %d srand() ints, %d time%.*s ===\n", count, loops, loops != 1, "s");
	struct timespec start, end;
	double elapsed;

	clock_gettime(CLOCK_MONOTONIC, &start);
	for(i = 0; i < count; i++)
		avl_insert(tree, (void *)buf[i]);
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = ((end.tv_sec * 1.0e3 + end.tv_nsec / 1e6) - (start.tv_sec * 1.0e3 + start.tv_nsec / 1e6)) / 1e3;
	fprintf(stderr, "Nodes:  %9d\n", (int)avl_size(tree));
	fprintf(stderr, "Height: %9d\n", avl_height(tree));
	fprintf(stderr, "Pages:  %9d\n", avl_pages(tree));
	fprintf(stderr, "Rate:   %9.0lf\n", count / elapsed);
	while(--loops > 0){
		avl_reset(tree);
		clock_gettime(CLOCK_MONOTONIC, &start);
		for(i = 0; i < count; i++)
			avl_insert(tree, (void *)buf[i]);
		clock_gettime(CLOCK_MONOTONIC, &end);
		elapsed = ((end.tv_sec * 1.0e3 + end.tv_nsec / 1e6) - (start.tv_sec * 1.0e3 + start.tv_nsec / 1e6)) / 1e3;
		fprintf(stderr, "Rate:   %9.0lf\n", count / elapsed);
	}

	printf("=== find ===\n");
	clock_gettime(CLOCK_MONOTONIC, &start);
	for(i = 0; i < count; i++)
		avl_find(tree, (void *)buf[i]);
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = ((end.tv_sec * 1.0e3 + end.tv_nsec / 1e6) - (start.tv_sec * 1.0e3 + start.tv_nsec / 1e6)) / 1e3;
	fprintf(stderr, "Rate:   %9.0lf\n", count / elapsed);

	printf("=== delete ===\n");
	clock_gettime(CLOCK_MONOTONIC, &start);
	for(i = 0; i < count; i++){
		avl_delete(tree, (void *)buf[i]);
	}
	clock_gettime(CLOCK_MONOTONIC, &end);
	elapsed = ((end.tv_sec * 1.0e3 + end.tv_nsec / 1e6) - (start.tv_sec * 1.0e3 + start.tv_nsec / 1e6)) / 1e3;
	fprintf(stderr, "Rate:   %9.0lf\n", count / elapsed);

	assert(avl_size(tree) == 0);
	assert(avl_height(tree) == -1);

	avl_free(tree);
	free(buf);

	return 0;
}

#endif
