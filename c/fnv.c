
#include "fnv.h"

#define FNV_PRIME 16777619
#define MAX 4294967296

unsigned int fnv_hash(const void* key, int length, unsigned int seed) {
  const unsigned char * data = (const unsigned char*) key;
  int n;
  unsigned int xord;
  unsigned int hash = seed;
  
  for(n=0; n < length; n++) {
    xord = hash ^ data[n];
    hash = (xord * FNV_PRIME) % MAX;
  }
  return hash;
}