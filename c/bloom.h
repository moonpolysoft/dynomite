
typedef struct _bloom_t {
  char *filename;
  FILE *file;
  char * bits;
  unsigned long n;
  double e;
  unsigned int m;
  unsigned int k;
  unsigned long keys;
  unsigned int seed;
} bloom_t;

bloom_t *bloom_open(char *filename);
bloom_t *bloom_create(char *filename, long n, double e);
void bloom_put(bloom_t* bloom, char *buff, int len);
int bloom_has(bloom_t* bloom, char *buff, int len);
void bloom_destroy(bloom_t* bloom);

#define bloom_key_size(bloom) ((bloom)->keys)
#define bloom_mem_size(bloom) ((bloom)->m / 8)