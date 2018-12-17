#include<string.h>

#include"counter.h"

int ctr_init(uint8_t *buf){
	return *buf = 0, 1;
}

int ctr_inc(uint8_t *buf){
	int i = *buf;
	do{
		if(++buf[i]){
			if(0 == i){
				buf[*buf] = 0;
				buf[1] = 1;
			}
			return 1 + *buf;
		}
	}while(i--);
	return 1;
}

int ctr_dec(uint8_t *buf){
	int i = *buf;
	do{
		if(buf[i]--)
			return 1 + *buf;
	}while(i--);
	memset(buf, -1, 256);
	return 256;
}

int ctr_cmp(uint8_t *a, uint8_t *b){
	return memcmp(a, b, *a < *b ? *a : *b );
}

static inline uint64_t _delta(uint8_t *a, uint8_t *b){
	static const uint64_t check = 0xffULL << 56;
	uint64_t delta = 0;
	int i = *(a++);
	int j = *(b++);
	while(i > j){
		if(delta & check)
			return -1;
		delta = (delta << 8) | *(a++);
		i--;
	}
	while(i--){
		const int carry = (*a < *b) ? 0x100 : 0;
		if(carry)
			delta--;
		if(delta & check)
			return -1;
		delta = (delta << 8) | (carry + *(a++) - *(b++));
	}
	return delta;
}

uint64_t ctr_delta(uint8_t *a, uint8_t *b){
	return (*a < *b) ? _delta(b, a) : _delta(a, b);
}

int ctr_len(uint8_t *a){
	return 1 + *a;
}
