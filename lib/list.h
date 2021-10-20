#ifndef _LIST_H
#define _LIST_H

// in all cases:
//	runtime is O(1)
//	set 'dir' to zero or non-zero to determine operation orientation

// list handle, or first entry in struct you'd like to make lists of
typedef void **list_t[2];

// if you need to alias a list
typedef void ***list_ptr;

// initialize previously allocated list handle
void list_init(list_t ls);

// return 1 if list is empty, else 0
int list_empty(list_t ls);

// push object onto end of list
void list_push(list_t ls, void *obj, int dir);

// remove and return object from end of list
void *list_pop(list_t ls, int dir);

// return object at end of list
void *list_peek(list_t ls, int dir);

// insert object into list before or after target object
void list_insert(list_t ls, void *target, void *n, int dir);

// remove object from list
void list_remove(list_t ls, void *obj);

// remove object from list, and return next object
void *list_consume(list_t ls, void *obj, int dir);

// given list, node, and direction, returns next object
void *list_next(list_t ls, void *obj, int dir);

// given list, node, and direction, returns next object, wrapping if last
void *list_next_wrap(list_t ls, void *obj, int dir);

// steal contents of src and add to dst
void list_splice(list_t dst, list_t src, int dir);

#endif
