#if defined(__FreeBSD__) || defined(__APPLE__) || defined(__linux__)

#include <erl_driver.h>
#include <ei.h>
#include <stdio.h>
#include <sys/resource.h>
#include <errno.h>

#define SET 's'
#define GET 'g'

static ErlDrvData init(ErlDrvPort port, char *cmd);
static void output(ErlDrvData handle, char* buff, int len);
static void stop(ErlDrvData handle);
static void send_errno(ErlDrvPort port, int code);
static void send_rlimit(ErlDrvPort port, struct rlimit *limit);

static ErlDrvData init(ErlDrvPort port, char *cmd) {
  return (ErlDrvData) port;
}

static void output(ErlDrvData handle, char* buff, int len) {
  ErlDrvPort port = (ErlDrvPort) handle;
  int index=1;
  struct rlimit limit;
  
  switch (buff[0]) {
    case SET:
      getrlimit(RLIMIT_NOFILE, &limit);
      ei_decode_version(buff, &index, NULL);
      ei_decode_long(buff, &index, &limit.rlim_cur);
      if (-1 == setrlimit(RLIMIT_NOFILE, &limit)) {
        send_errno(port, errno);
        return;
      }
      getrlimit(RLIMIT_NOFILE, &limit);
      send_rlimit(port, &limit);
      return;
    case GET:
      getrlimit(RLIMIT_NOFILE, &limit);
      send_rlimit(port, &limit);
      return;
  }
}

static void stop(ErlDrvData handle) {
  
}

static void send_rlimit(ErlDrvPort port, struct rlimit *limit) {
  ei_x_buff x;
  ei_x_new_with_version(&x);
  ei_x_encode_tuple_header(&x, 2);
  ei_x_encode_long(&x, limit->rlim_cur);
  ei_x_encode_long(&x, limit->rlim_max);
  driver_output(port, x.buff, x.index);
  ei_x_free(&x);
}

static void send_errno(ErlDrvPort port, int code) {
  ei_x_buff x;
  char *msg = erl_errno_id(code);
  ei_x_new_with_version(&x);
  ei_x_encode_tuple_header(&x, 2);
  ei_x_encode_atom(&x, "error");
  ei_x_encode_atom(&x, msg);
  driver_output(port, x.buff, x.index);
  ei_x_free(&x);
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