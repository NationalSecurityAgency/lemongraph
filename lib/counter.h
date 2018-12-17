#ifndef _COUNTER_H
#define _COUNTER_H

#include<inttypes.h>

// supplied buffer should be at most uint8_t[256]
// returns number of bytes that matter
int ctr_init(uint8_t *buf);
int ctr_inc(uint8_t *buf);
int ctr_dec(uint8_t *buf);
int ctr_len(uint8_t *buf);

// for sorting
int ctr_cmp(uint8_t *a, uint8_t *b);

// returns unsigned diff between 2 counters, or (uint64_t)-1 if too big
uint64_t ctr_delta(uint8_t *a, uint8_t *b);

#endif
