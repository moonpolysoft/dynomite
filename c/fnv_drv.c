
#include "fnv.h"
#include <erl_driver.h>
#include <ei.h>
#include <stdio.h>

#define write_int32(i, s) {((char*)(s))[0] = (char)((i) >> 24) & 0xff; \
                        ((char*)(s))[1] = (char)((i) >> 16) & 0xff; \
                        ((char*)(s))[2] = (char)((i) >> 8)  & 0xff; \
                        ((char*)(s))[3] = (char)((i)        & 0xff);}

static ErlDrvData init(ErlDrvPort port, char *cmd);
static void stop(ErlDrvData handle);
static void outputv(ErlDrvData handle, ErlIOVec *ev);
static void send_hash(ErlDrvPort port, unsigned long hash);

static ErlDrvData init(ErlDrvPort port, char *cmd) {
  return (ErlDrvData) port;
}

static void stop(ErlDrvData handle) {
  //noop
}

static void outputv(ErlDrvData handle, ErlIOVec *ev) {
  ErlDrvPort port = (ErlDrvPort) handle;
  ErlDrvTermData caller;
  SysIOVec *bin;
  int i, n, index = 0;
  unsigned long hash;
  unsigned long seed;
  //first piece of the iovec is the seed
  // printf("ev->size %d\n", ev->size);
  // printf("ev-vsize %d\n", ev->vsize);
  //apparently we start counting at 1 round here?
  bin = &ev->iov[1];
  // printf("bin->orig_size %d\n", bin->iov_len);
  // printf("bin->iov_base %s\n", bin->iov_base);
  ei_decode_version(bin->iov_base, &index, NULL);
  ei_decode_ulong(bin->iov_base, &index, &seed);
  hash = seed;
  if (index < bin->iov_len) {
    hash = fnv_hash(&bin->iov_base[index], bin->iov_len - index, hash);
  }
  for (i=2; i<ev->vsize; i++) {
    bin = &ev->iov[i];
    hash = fnv_hash(bin->iov_base, bin->iov_len, hash);
  }
  caller = driver_caller(port);
  ErlDrvTermData spec[] = {
    ERL_DRV_UINT, hash
  };
  driver_send_term(port, caller, spec, 2);
}

static int control(ErlDrvData data, unsigned int seed, char *buf, int len, char **rbuf, int rlen) {
  unsigned int hash;
  int index = 0;
  // printf("length %d\n", len);
  // printf("buf %s\n", buf);
  hash = fnv_hash(buf, len, seed);
  if (rlen < sizeof(hash)) {
    (*rbuf) = (char *) driver_realloc(*rbuf, sizeof(hash));
  }
  write_int32(hash, *rbuf);
  return sizeof(hash);
}

static void send_hash(ErlDrvPort port, unsigned long hash) {
  ei_x_buff x;
  ei_x_new_with_version(&x);
  ei_x_encode_ulong(&x, hash);
  driver_output(port, x.buff, x.index);
  // printf("sent hash %d\n", hash);
  ei_x_free(&x);
}

static ErlDrvEntry fnv_driver_entry = {
    NULL,                     /* init */
    init, 
    stop, 
    NULL,                     /* output */
    NULL,                     /* ready_input */
    NULL,                     /* ready_output */ 
    "fnv_drv",             /* the name of the driver */
    NULL,                     /* finish */
    NULL,                     /* handle */
    control,                     /* control */
    NULL,                     /* timeout */
    outputv,                  /* outputv */
    NULL,                     /* ready_async */
    NULL,                     /* flush */
    NULL,                     /* call */
    NULL                      /* event */
};

DRIVER_INIT(fnv_driver) {
  return &fnv_driver_entry;
}