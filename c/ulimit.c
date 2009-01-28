
#include <stdio.h>
#include <errno.h>
#include <sys/resource.h>

int main(void) {
  struct rlimit limit;
  int soft_limit;
  long hard_limit;
  int n=0;
  char filename[256];
  
  getrlimit(RLIMIT_NOFILE, &limit);
  soft_limit = (int) limit.rlim_cur;
  hard_limit = (long) limit.rlim_max;
  printf("soft limit %d\nhard limit %d\n", soft_limit, hard_limit);
  limit.rlim_cur = (rlim_t) 4096;
  if (-1 == setrlimit(RLIMIT_NOFILE, &limit)) {
    printf("failed at life, errno: %d\n", errno);
    return -1;
  } else {
    printf("successfully set rlimit\n");
    getrlimit(RLIMIT_NOFILE, &limit);
    printf("soft limit now at %d\n", (int)limit.rlim_cur);
  }
  //test out the limit
  //3 fd's for other shit lol
  for (n=0; n<16384; n++) {
    sprintf(filename, "file%d", n);
    if (NULL == fopen(filename, "w")) {
      printf("failed at opening file number %d\n", n+1);
      return -1;
    }
  }
  
  return 0;
}