#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "config.h"
#include "connmgr.h"
#include "datamgr.h"
#include "errmacros.h"
#include "sbuffer.h"
#include "sensor_db.h"
#include "./lib/dplist.h"

#define FIFO_NAME 	"logFifo"

void *connmgr(void *arg);
void *datamgr(void *arg);
void *storagemgr(void *arg);

sbuffer_t *buffer;
FILE *fp_fifo_w;


void run_child (void)
{
  FILE *fp_gateway, *fp_fifo_r;
  char str[256];

  int sequence_number = 0;

  fp_fifo_r = fopen(FIFO_NAME, "r");
  #ifdef DEBUG
    printf("syncing with writer ok\n");
  #endif
  FILE_OPEN_ERROR(fp_fifo_r);
  fp_gateway = fopen("gateway.log", "w");
  FILE_OPEN_ERROR(fp_gateway);

  while(1)
	{
    char *retval = NULL;
		retval = fgets(str,256,fp_fifo_r);
		if(retval == NULL) break;
		sequence_number++;
		fprintf(fp_gateway, "%2d %ld %s", sequence_number, time(NULL), str);
	}

	FILE_CLOSE_ERROR(fclose(fp_gateway));
	FILE_CLOSE_ERROR(fclose(fp_fifo_r));

  exit(EXIT_SUCCESS); // this terminates the child process
  #ifdef DEBUG
    printf("child process terminated\n");
  #endif
}

int main(int argc, const char *argv[])
{
  pid_t child_pid, pid;
  int result;
  pthread_t connmgr_thread, datamgr_thread, storagemgr_thread;

  //check the input argc
  if (argc < 2)
	{
		puts("not enough command line argument, please set port number");
		exit(EXIT_FAILURE);
	}
	int port_number = atoi(argv[1]);
	printf("port number = %d\n", port_number);

  /* Create the FIFO if it does not exist */
  result = mkfifo(FIFO_NAME, 0666);
  CHECK_MKFIFO(result);

  child_pid = fork();
  SYSCALL_ERROR(child_pid);

  if ( child_pid == 0  )
  {
    run_child();
  }

  fp_fifo_w = fopen(FIFO_NAME, "w");
	FILE_OPEN_ERROR(fp_fifo_w);

  // init sbuffer
  if (sbuffer_init(&buffer) != SBUFFER_SUCCESS) exit(EXIT_FAILURE);
  //run the threads
  result = pthread_create(&connmgr_thread, NULL, connmgr, &port_number);
	PTHREAD_ERROR(result);
	result = pthread_create(&datamgr_thread, NULL, datamgr, NULL);
	PTHREAD_ERROR(result);
	result = pthread_create(&storagemgr_thread, NULL, storagemgr, NULL);
	PTHREAD_ERROR(result);
  //close the threads
	result = pthread_join(connmgr_thread, NULL);
	PTHREAD_ERROR(result);
	result = pthread_join(datamgr_thread, NULL);
	PTHREAD_ERROR(result);
	result = pthread_join(storagemgr_thread, NULL);
	PTHREAD_ERROR(result);
  //close fp_fifo_w
  FILE_CLOSE_ERROR(fclose(fp_fifo_w));
  //close sbuffer
  if (sbuffer_free(&buffer) != SBUFFER_SUCCESS) exit(EXIT_FAILURE);
  pid = wait(NULL);
	SYSCALL_ERROR(pid);
	return 0;
}

void *connmgr(void * arg)
{
  // #ifdef DEBUG
  printf("connmgr thread starts to run\n");
  // #endif
  int port_number = *((int *)arg);
  connmgr_listen(port_number, &buffer);
	connmgr_free();
  printf("connmgr thread ends\n");
	pthread_exit(NULL);
}

void *datamgr(void *arg)
{
  // #ifdef DEBUG
  printf("datamgr thread starts to run\n");
  // #endif
  FILE *fp_sensor_map;
	fp_sensor_map = fopen("room_sensor.map", "r");
  FILE_ERROR(fp_sensor_map, "Couldn't create room_sensor.map\n");
	FILE_OPEN_ERROR(fp_sensor_map);

  datamgr_parse_sensor_data(fp_sensor_map, &buffer);
	datamgr_free();
  FILE_CLOSE_ERROR(fclose(fp_sensor_map));
  printf("datamgr thread ends\n");
	pthread_exit(NULL);
}

void *storagemgr(void *arg)
{
  // #ifdef DEBUG
  printf("storagemgr thread starts to run\n");
  // #endif
  sqlite3 *db;
	db = init_connection(1);
  if (db == NULL)
  {
    int num_try = 0;
    while (num_try < 3)
    {
      db = init_connection(1);
      if (db != NULL) break;
      printf("conneciton to database failed\n");
      num_try++;
      sleep(1);
    }
    if (db == NULL) exit(EXIT_FAILURE);
  }

	insert_sensor_from_buffer(db, &buffer);
	disconnect(db);
  printf("storagemgr thread ends\n");
	pthread_exit(NULL);
}
