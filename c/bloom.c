#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include "bloom.h"
#include "murmur.h"


#define BYTE_INDEX(index) ((int)(index/8))
#define BIT_INDEX(index) (index % 8)

#define SET_BIT(buff, index) buff[BYTE_INDEX(index)] |= (1 << BIT_INDEX(index))
#define GET_BIT(buff, index) (int)(buff[BYTE_INDEX(index)] & (1 << BIT_INDEX(index)))

#define 

//internal function headers
static void 
static void read_header(FILE *file, bloom_t *bloom);
static void write_bloom(FILE *file, bloom_t *bloom);

bloom_t *bloom_open(char *filename) {
  bloom_t *bloom;
  FILE *file;
  
  if (NULL == (file = fopen(filename, "r+"))) {
    return NULL;
  }
  
  bloom = malloc(sizeof(bloom_t));
  read_header(file, bloom);
  
  bloom->file = file;
  bloom->filename = filename;
  return bloom;
}

bloom_t *bloom_create(long n, double e) {
  bloom_t *bloom;
  FILE* file;
  
  if (NULL == (file = fopen(filename, "w+"))) {
    return NULL;
  }
  
  bloom = malloc(sizeof(bloom_t));
  
  bloom->n = n;
  bloom->e = e;
  bloom->keys = 0;
  bloom->seed = rand();
  
  bloom->m = (int) ceil(n * log(e) / log(1.0 / pow(2, log(2))));
  bloom->k = (int) round(log(2) * bloom->m / n);
  
  bloom->bits = malloc(sizeof(char) * (int) round(bloom->m / 8));
  write_bloom(file, bloom);
  return bloom;
}

void bloom_put(bloom_t *bloom, char *buff, int len) {
  int i=0;
  unsigned int hash = bloom->seed;
  unsigned int index;
    // printf("bloom %p\n", bloom);
  // printf("k %d\n", bloom->k);
  for(i=0; i<bloom->k; i++) {
    hash = MurmurHash2(buff, len, hash);
    index = hash % bloom->m;
    // printf("setting index %d\n", index);
    // printf("byte %d bit %d\n", BYTE_INDEX(index), BIT_INDEX(index));
    SET_BIT(bloom->bits, index);
    // printf("byte %d\n", bloom->bits[BYTE_INDEX(index)]);
  }
  bloom->keys++;
}

int bloom_has(bloom_t *bloom, char *buff, int len) {
  int i=0;
  unsigned int hash = bloom->seed;
  unsigned int index;
  // printf("bloom %p\n", bloom);
  // printf("k %d\n", bloom->k);
  for(i=0; i<bloom->k; i++) {
    hash = MurmurHash2(buff, len, hash);
    index = hash % bloom->m;
    // printf("getting index %d\n", index);
    // printf("byte %d bit %d\n", BYTE_INDEX(index), BIT_INDEX(index));
    // printf("byte %d\n", bloom->bits[BYTE_INDEX(index)]);
    // printf("get result %d\n", GET_BIT(bloom->bits, index));
    if (0 == GET_BIT(bloom->bits, index)) {
      return 0;
    }
  }
  return 1;
}

void bloom_destroy(bloom_t *bloom) {
  free(bloom->bits);
  free(bloom);
}

static void read_header(FILE *file, bloom_t *bloom) {
  
}
