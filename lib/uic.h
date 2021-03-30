#ifndef _UIC_H
#define _UIC_H

#include<inttypes.h>

#define encode(x, buffer, iter) do{ iter += _encode(x,  iter + (uint8_t *)(buffer)); }while(0)
#define decode(x, buffer, iter) do{ iter += _decode(&x, iter + (uint8_t *)(buffer)); }while(0)
#define enclen(buffer, iter) _enclen(((uint8_t *)(buffer))[iter])

#ifndef INLINE
#define INLINE inline __attribute__((always_inline))
#endif

static INLINE int _encbytes(uint64_t n){
	if(n < 0x0000000000000080ULL) return 0;
	if(n < 0x0000000000004000ULL) return 1;
	if(n < 0x0000000000200000ULL) return 2;
	if(n < 0x0000000010000000ULL) return 3;
	if(n < 0x0000000800000000ULL) return 4;
	if(n < 0x0000040000000000ULL) return 5;
	if(n < 0x0002000000000000ULL) return 6;
	if(n < 0x0100000000000000ULL) return 7;
	return 8;
}

static INLINE uint8_t _encode(uint64_t x, uint8_t *buffer){
	uint8_t bytes = _encbytes(x);
	*(buffer++) = (0xff00 >> bytes) | (x >> (8 * bytes));
	switch(bytes++){
		case 8: *(buffer++) = (x >> 56); // fall through
		case 7: *(buffer++) = (x >> 48); // fall through
		case 6: *(buffer++) = (x >> 40); // fall through
		case 5: *(buffer++) = (x >> 32); // fall through
		case 4: *(buffer++) = (x >> 24); // fall through
		case 3: *(buffer++) = (x >> 16); // fall through
		case 2: *(buffer++) = (x >> 8);  // fall through
		case 1: *(buffer++) = x;
	}
	return bytes;
}

static INLINE uint8_t _enclen(int b){
	if(b < 0x80) return 1;
	if(b < 0xc0) return 2;
	if(b < 0xe0) return 3;
	if(b < 0xf0) return 4;
	if(b < 0xf8) return 5;
	if(b < 0xfc) return 6;
	if(b < 0xfe) return 7;
	if(b < 0xff) return 8;
	return 9;
}

static INLINE int _decode(uint64_t *x, uint8_t *buffer){
	const int bytes = _enclen(*buffer);
	uint64_t n = (0xfe >> bytes) & *(buffer++);
	switch(bytes){
		case 9: n = (n << 8) | *(buffer++); // fall through
		case 8: n = (n << 8) | *(buffer++); // fall through
		case 7: n = (n << 8) | *(buffer++); // fall through
		case 6: n = (n << 8) | *(buffer++); // fall through
		case 5: n = (n << 8) | *(buffer++); // fall through
		case 4: n = (n << 8) | *(buffer++); // fall through
		case 3: n = (n << 8) | *(buffer++); // fall through
		case 2: n = (n << 8) | *(buffer++);
	}
	*x = n;
	return bytes;
}

#endif
