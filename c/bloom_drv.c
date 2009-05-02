#include <stdint.h>
#include <sys/types.h>
#include "bloom.h"
#include <erl_driver.h>
#include <ei.h>
#include <stdio.h>
#include <string.h>

typedef struct _bloom_drv_t {
  ErlDrvPort port;
  bloom_t *bloom;
} bloom_drv_t;

typedef union {
  long i;
  double d;
  char c[8];
} uni;

//=======================================================================
// COMMAND CODES
#define SETUP 's'
#define PUT   'p'
#define HAS   'h'
#define MEM_SIZE 'm'
#define KEY_SIZE 'k'

//=======================================================================
// ERL_DRIVER CALLBACKS
static ErlDrvData init(ErlDrvPort port, char *cmd);
static void stop(ErlDrvData handle);
static void output(ErlDrvData handle, char *buf, int len);

//=======================================================================
// Internal functions
static void setup(bloom_drv_t *driver, char *buf, int len);
static void put(bloom_drv_t *driver, char *buf, int len);
static void has(bloom_drv_t *driver, char *buf, int len);
static void mem_size(bloom_drv_t *driver);
static void key_size(bloom_drv_t *driver);

//=======================================================================
// ERL_DRIVER CALLBACKS
static ErlDrvData init(ErlDrvPort port, char *cmd) {
  bloom_drv_t *driver;
  
  driver = driver_alloc(sizeof(bloom_drv_t));
  driver->port = port;
  driver->bloom = NULL;
  
  return (ErlDrvData)driver;
}

static void stop(ErlDrvData handle) {
  bloom_drv_t *driver = (bloom_drv_t*)handle;
  
  bloom_destroy(driver->bloom);
  driver_free(driver);
}

static void output(ErlDrvData handle, char *buf, int len) {
  bloom_drv_t *driver = (bloom_drv_t *)handle;
  char command = buf[0];
  
  switch (command) {
    case SETUP:
    setup(driver, &buf[1], len-1);
    break;
    case PUT:
    put(driver, &buf[1], len-1);
    break;
    case HAS:
    has(driver, &buf[1], len-1);
    break;
    case MEM_SIZE:
    mem_size(driver);
    break;
    case KEY_SIZE:
    key_size(driver);
    break;
  }
}

//=======================================================================
//internal

static void setup(bloom_drv_t *driver, char *buf, int len) {
  long n;
  double e;
  char *filename;
  int size;
  int type;
  int index = 0;
  
  ei_decode_version(buf, &index, NULL);
  ei_decode_tuple_header(buf, &index, NULL);
  ei_get_type(buf, &index, &type, &size);
  filename = driver_alloc(size+1);
  ei_decode_string(buf, &index, filename);
  ei_decode_long(buf, &index, &n);
  ei_decode_double(buf, &index, &e);
  
  driver->bloom = bloom_open(filename, n, e);
  
  driver_free(filename);
}

static void put(bloom_drv_t *driver, char *buf, int len) {
  bloom_put(driver->bloom, buf, len);
}

static void has(bloom_drv_t *driver, char *buf, int len) {
  int result;
  ei_x_buff x;
  result = bloom_has(driver->bloom, buf, len);
  
  ei_x_new_with_version(&x);
  ei_x_encode_boolean(&x, result);
  
  driver_output(driver->port, x.buff, x.index);
  ei_x_free(&x);
}

static void mem_size(bloom_drv_t *driver) {
  long result;
  ei_x_buff x;
  result = bloom_mem_size(driver->bloom);
  
  ei_x_new_with_version(&x);
  ei_x_encode_long(&x, result);
  
  driver_output(driver->port, x.buff, x.index);
  ei_x_free(&x);
}

static void key_size(bloom_drv_t *driver) {
  long result;
  ei_x_buff x;
  result = bloom_key_size(driver->bloom);
  
  ei_x_new_with_version(&x);
  ei_x_encode_long(&x, result);
  
  driver_output(driver->port, x.buff, x.index);
  ei_x_free(&x);
}

static ErlDrvEntry bloom_driver_entry = {
  NULL,                             /* init */
  init, 
  stop, 
  output,                           /* output */
  NULL,                             /* ready_input */
  NULL,                             /* ready_output */ 
  "bloom_drv",                      /* the name of the driver */
  NULL,                             /* finish */
  NULL,                             /* handle */
  NULL,                             /* control */
  NULL,                             /* timeout */
  NULL,                             /* outputv */
  NULL,                             /* ready_async */
  NULL,                             /* flush */
  NULL,                             /* call */
  NULL,                             /* event */
  ERL_DRV_EXTENDED_MARKER,          /* ERL_DRV_EXTENDED_MARKER */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MAJOR_VERSION */
  ERL_DRV_EXTENDED_MAJOR_VERSION,   /* ERL_DRV_EXTENDED_MINOR_VERSION */
  ERL_DRV_FLAG_USE_PORT_LOCKING     /* ERL_DRV_FLAGs */
};

DRIVER_INIT(bloom_driver) {
  return &bloom_driver_entry;
}