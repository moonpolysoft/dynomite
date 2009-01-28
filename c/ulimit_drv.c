#if defined(__FreeBSD__) || defined(__APPLE__) || defined(__linux__)

#include <erl_driver.h>
#include <ei.h>
#include <stdio.h>
#include <sys/resource.h>
#include <errno.h>

#define SET 'g'
#define GET 's'

static ErlDrvData init(ErlDrvPort port, char *cmd);
static void output(ErlDrvData handle, char* buff, int len);
static void stop(ErlDrvData handle);

static ErlDrvData init(ErlDrvPort port, char *cmd) {
  return (ErlDrvData) port;
}

static void output(ErlDrvData handle, char* buff, int len) {
  ErlDrvPort port = (ErlDrvPort) handle;
  int index=1;
  long new_soft_limit;
  struct rlimit limit;
  
  switch (buff[0]) {
    case SET:
      ei_decode_long(buff, &index, &new_soft_limit);
      
      break;
    case GET:
      
  }
}

static void stop(ErlDrvData handle) {
  
}

static ErlDrvEntry ulimit_driver_entry = {
    NULL,                     /* init */
    init, 
    stop, 
    output,                   /* output */
    NULL,                     /* ready_input */
    NULL,                     /* ready_output */ 
    "ulimit_drv",             /* the name of the driver */
    NULL,                     /* finish */
    NULL,                     /* handle */
    NULL,                     /* control */
    NULL,                     /* timeout */
    NULL,                     /* outputv */
    NULL,                     /* ready_async */
    NULL,                     /* flush */
    NULL,                     /* call */
    NULL                      /* event */
};

DRIVER_INIT(ulimit_driver) {
  return &ulimit_driver_entry;
}

#endif