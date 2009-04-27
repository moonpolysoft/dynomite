
typedef struct _bloom_data_t {
  uint32_t version; //0
  uint32_t m;       //4
  uint64_t n;       //8
  double e;         //
  uint32_t k;       //
  uint64_t keys;    //3
  uint32_t seed;
  char reserved[64];
  char bits[1];
} bloom_data_t;

typedef struct _bloom_t {
  char *filename;
  int file;
  bloom_data_t data;
} bloom_t;

bloom_t *bloom_open(char *filename, long n, double e);
void bloom_put(bloom_t* bloom, char *buff, int len);
int bloom_has(bloom_t* bloom, char *buff, int len);
void bloom_destroy(bloom_t* bloom);

#define bloom_key_size(bloom) ((bloom)->data.keys)
#define bloom_mem_size(bloom) ((bloom)->data.m / 8)