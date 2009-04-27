#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include "bloom.h"
#include "murmur.h"

#define BYTE_SIZE(index) ((int)(index/8))
#define BYTE_INDEX(index) ((int)(index/8))
#define BIT_INDEX(index) (index % 8)

#define SET_BIT(buff, index) buff[BYTE_INDEX(index)] |= (1 << BIT_INDEX(index))
#define GET_BIT(buff, index) (int)(buff[BYTE_INDEX(index)] & (1 << BIT_INDEX(index)))


//internal function headers
static void read_header(int file, bloom_t *bloom);
static void write_bloom(int file, bloom_t *bloom);

bloom_t *bloom_open(char* filename, long n, double e) {
  bloom_t *bloom;
  int file;
  uint32_t m;
  uint32_t version;
  struct stat file_stat;
  
  // printf("sizeof(bloom_data_t) %d\n", sizeof(bloom_data_t));
  
  if (-1 == stat(filename, &file_stat)) {
    //create a new one
    // printf("creating new file\n");
    if (-1 == (file = open(filename, O_CREAT | O_RDWR, S_IWUSR | S_IRUSR | S_IRGRP | S_IWGRP))) {
      return NULL;
    }
    
    m = (int) ceil(n * log(e) / log(1.0 / pow(2, log(2))));
    bloom = malloc(sizeof(bloom_t) + BYTE_SIZE(m));
    bloom->data.n = n;
    bloom->data.e = e;
    bloom->data.keys = 0;
    bloom->data.seed = rand();
    
    bloom->data.m = m;
    bloom->data.k = (int) round(log(2) * m / n);
    pwrite(file, &bloom->data, sizeof(bloom_data_t) + BYTE_SIZE(m), 0);
  } else {
    // printf("opening existing file\n");
    if (-1 == (file = open(filename, O_RDWR))) {
      return NULL;
    }
    
    pread(file, &version, sizeof(uint32_t), 0);
    pread(file, &m, sizeof(uint32_t), 4);
    
    // printf("read version of %d\n", version);
    // printf("read m of %d\n", m);
    bloom = malloc(sizeof(bloom_t) + BYTE_SIZE(m));
    pread(file, &bloom->data, sizeof(bloom_data_t) + BYTE_SIZE(m), 0);
  }
  // printf("bloom->data %d\n", (int)&bloom->data);
  // printf("n %d\n", (int)&bloom->data.n);
  // printf("keys %d\n", (int)&bloom->data.keys);
  // printf("version = %d %d\n", bloom->data.version, (( int)&(bloom->data.version) - ( int)&(bloom->data)));
  // printf("m = %d %d\n", bloom->data.m, (( int)&(bloom->data.m) - ( int)&(bloom->data)));
  // printf("n = %d %d\n", bloom->data.n, (( int)&(bloom->data.n) - ( int)&(bloom->data)));
  // printf("e = %f %d\n", bloom->data.e, (( int)&(bloom->data.e) - ( int)&(bloom->data)));
  // printf("k = %d %d\n", bloom->data.k, (( int)&(bloom->data.k) - ( int)&(bloom->data)));
  // printf("keys = %d %d\n", bloom->data.keys, (( int)&(bloom->data.keys) - ( int)&(bloom->data)));
  // printf("seed = %d %d\n", bloom->data.seed, (( int)&(bloom->data.seed) - ( int)&(bloom->data)));
  bloom->file = file;
  bloom->filename = malloc(strlen(filename) + 1);
  strcpy(bloom->filename, filename);
  
  return bloom;
}

void bloom_put(bloom_t *bloom, char *buff, int len) {
  int i=0;
  unsigned int hash = bloom->data.seed;
  unsigned int index;
  unsigned int offset;
  unsigned int byte_index;
    // printf("bloom %p\n", bloom);
  // printf("k %d\n", bloom->k);
  for(i=0; i<bloom->data.k; i++) {
    hash = MurmurHash2(buff, len, hash);
    index = hash % bloom->data.m;
    // printf("setting index %d\n", index);
    // printf("byte %d bit %d\n", BYTE_INDEX(index), BIT_INDEX(index));
    byte_index = BYTE_INDEX(index);
    SET_BIT(bloom->data.bits, index);
    // if ((sizeof(bloom_data_t) + byte_index - 1) < 108) {
    //   printf("writing to %d\n", sizeof(bloom_data_t) + byte_index - 1);
    offset = (unsigned int)&(bloom->data.bits[byte_index]) - (unsigned int)&bloom->data;
    pwrite(bloom->file, &bloom->data.bits[byte_index], 1, offset);
    // printf("byte %d\n", bloom->bits[BYTE_INDEX(index)]);
  }
  bloom->data.keys++;
  offset = ((unsigned int)&(bloom->data.keys) - (unsigned int)&(bloom->data));
  // printf("writing keys into offset %d\n", offset);
  pwrite(bloom->file, &(bloom->data.keys), sizeof(uint32_t), offset);
}

int bloom_has(bloom_t *bloom, char *buff, int len) {
  int i=0;
  unsigned int hash = bloom->data.seed;
  unsigned int index;
  // printf("bloom %p\n", bloom);
  // printf("k %d\n", bloom->k);
  for(i=0; i<bloom->data.k; i++) {
    hash = MurmurHash2(buff, len, hash);
    index = hash % bloom->data.m;
    // printf("getting index %d\n", index);
    // printf("byte %d bit %d\n", BYTE_INDEX(index), BIT_INDEX(index));
    // printf("byte %d\n", bloom->bits[BYTE_INDEX(index)]);
    // printf("get result %d\n", GET_BIT(bloom->bits, index));
    if (0 == GET_BIT(bloom->data.bits, index)) {
      return 0;
    }
  }
  return 1;
}

void bloom_destroy(bloom_t *bloom) {
  if (NULL != bloom) {
    if (NULL != bloom->filename) free(bloom->filename);
    if (-1 != bloom->file) close(bloom->file);
    free(bloom);
  }
}
