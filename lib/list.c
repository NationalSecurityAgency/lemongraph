#include<stdlib.h>

#include"list.h"

static inline void **_list_remove(void ***n){
	n[1][0] = n[0];
	n[0][1] = n[1];
	return (void **)n;
}

static inline void _list_insert(void ***t, void **n, int dir){
	n[dir] = t[dir];
	t[dir][!dir] = n;
	n[!dir] = t;
	t[dir] = n;
}

static inline void **_list_peek(list_t ls, int dir){
	return (ls[dir] == (void *)ls) ? NULL : ls[dir];
}

static inline void *_list_next(list_t ls, void **n, int dir){
	return n[dir] == (void *)ls ? NULL : n[dir];
}

static inline void _list_splice(list_t dst, list_t src, int dir){
	src[!dir][dir] = dst[dir];
	dst[dir][!dir] = src[!dir];
	dst[dir] = src[dir];
	src[dir][!dir] = dst;
	src[0] = src[1] = (void **)src;
}

int list_empty(list_t ls){
	return *ls == (void *)ls;
}

void list_init(list_t ls){
	ls[0] = ls[1] = (void **)ls;
}

void list_push(list_t ls, void *n, int dir){
	_list_insert(ls, (void **)n, !!dir);
}

void *list_peek(list_t ls, int dir){
	return _list_peek(ls, !!dir);
}

void *list_pop(list_t ls, int dir){
	void **n = _list_peek(ls, !!dir);
	return n ? _list_remove((void ***)n) : n;
}

void list_insert(list_t ls, void *target, void *n, int dir){
	(void)ls;
	_list_insert((void ***)target, (void **)n, !dir);
}

void list_remove(list_t ls, void *n){
	(void)ls;
	_list_remove((void ***)n);
}

void *list_next(list_t ls, void *n, int dir){
	return _list_next(ls, n, !!dir);
}

void *list_next_wrap(list_t ls, void *n, int dir){
	dir = !!dir;
	n = _list_next(ls, n, dir);
	return n ? n : _list_peek(ls, dir);
}

void *list_consume(list_t ls, void *n, int dir){
	_list_remove((void ***)n);
	return _list_next(ls, n, !!dir);
}

void list_splice(list_t dst, list_t src, int dir){
	_list_splice(dst, src, !!dir);
}


#ifdef _LIST_MAIN

#include<stdio.h>
#include<string.h>

typedef struct {
	list_t ls;
	char *arg;
} arg_t;

static void dump_list(char *label, list_t ls, int dir){
	printf("%s(%d):", label, dir);
	arg_t *c = list_peek(ls, dir);
	while(c){
		printf(" %s", c->arg);
		c = list_next(ls, c, dir);
	}
	printf("\n");
}

int main(int argc, char **argv){
	list_t dst, src;
	arg_t n[--argc];
	argv++;
	int i;
	list_init(src);
	list_init(dst);
	char *tokens = "-+";
	int dir = -1;
	char *t;
	void ***target = dst;
	for(i = 0; i < argc; i++){
		if(dir == -1 && (t = strchr(tokens, argv[i][0])) && !argv[i][1]){
			dir = t - tokens;
			target = src;
		}else{
			n[i].arg = argv[i];
			list_push(target, n + i, 1);
		}
	}

	dump_list("dst", dst, 0);
	dump_list("dst", dst, 1);
	dump_list("src", src, 0);
	dump_list("src", src, 1);

	if(dir != -1){
		list_splice(dst, src, dir);
		dump_list("dst", dst, 0);
		dump_list("dst", dst, 1);
		dump_list("src", src, 0);
		dump_list("src", src, 1);
	}

	list_init(src);
	arg_t a = { .arg = "a" };
	arg_t b = { .arg = "b" };
	arg_t c = { .arg = "c" };

	list_push(src, &b, 1);
	list_insert(src, &b, &a, 0);
	list_insert(src, &b, &c, 1);
	dump_list("abc", src, 0);
	arg_t *x = list_peek(src, 0);
	while(x){
		printf("%s\n", x->arg);
		x = list_consume(src, x, 0);
	}
	return 0;
}

#endif
