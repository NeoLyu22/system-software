#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include "sbuffer.h"
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <inttypes.h>
#include <unistd.h>

typedef struct sbuffer_node {
  struct sbuffer_node * next;
  sbuffer_data_t data;
} sbuffer_node_t;

struct sbuffer {
  sbuffer_node_t * head;
  sbuffer_node_t * tail;
  pthread_mutex_t mtx;
  pthread_mutex_t read_mtx;
	pthread_cond_t read_cond;
  pthread_mutex_t remove_mtx;
	pthread_cond_t remove_cond;
  int remove_flag;
};


int sbuffer_init(sbuffer_t ** buffer)
{
  *buffer = malloc(sizeof(sbuffer_t));
  if (*buffer == NULL) return SBUFFER_FAILURE;
  (*buffer)->head = NULL;
  (*buffer)->tail = NULL;
  pthread_mutex_init(&((*buffer)->mtx) , NULL);
  pthread_mutex_init(&((*buffer)->read_mtx) , NULL);
	pthread_cond_init(&((*buffer)->read_cond), NULL);
  pthread_mutex_init(&((*buffer)->remove_mtx) , NULL);
	pthread_cond_init(&((*buffer)->remove_cond), NULL);
  (*buffer)->remove_flag = 0;
  return SBUFFER_SUCCESS;
}


int sbuffer_free(sbuffer_t ** buffer)
{

  if ((buffer==NULL) || (*buffer==NULL))
  {
    return SBUFFER_FAILURE;
  }
  while ( (*buffer)->head )
  {
    sbuffer_node_t * dummy;
    dummy = (*buffer)->head;
    (*buffer)->head = (*buffer)->head->next;
    free(dummy);
  }
  pthread_mutex_destroy(&((*buffer)->mtx));
  pthread_mutex_destroy(&((*buffer)->read_mtx));
	pthread_cond_destroy(&((*buffer)->read_cond));
  pthread_mutex_destroy(&((*buffer)->remove_mtx));
	pthread_cond_destroy(&((*buffer)->remove_cond));
  free(*buffer);
  *buffer = NULL;
  return SBUFFER_SUCCESS;
}


int sbuffer_remove(sbuffer_t * buffer,sbuffer_data_t * data, int timeout)
{
  // #ifdef DEBUG
  //   printf("remove begin  \n");
  // #endif
  sbuffer_node_t * dummy;
  struct timeval now;
  struct timespec abstime;


  if (buffer == NULL) return SBUFFER_FAILURE;

  pthread_mutex_lock(&(buffer->read_mtx));

  #ifdef DEBUG
    printf("remove CS lock  \n");
  #endif

  while (buffer->head == NULL)  //buffer is empty
  {
    gettimeofday(&now, NULL);
    abstime.tv_sec = now.tv_sec + timeout;
    // abstime.tv_sec = time(NULL) + (time_t)timeout;
    abstime.tv_nsec = now.tv_usec * 1000;
    int retval = pthread_cond_timedwait(&(buffer->read_cond), &(buffer->read_mtx), &abstime);
    if (retval == ETIMEDOUT )
    {
      pthread_mutex_unlock(&(buffer->read_mtx));
			return 1;
    }
  }//end while

  #ifdef DEBUG
    printf("get out of while loop  \n");
  #endif

  pthread_mutex_unlock(&(buffer->read_mtx));

  #ifdef DEBUG
    printf("start the remove sync  \n");
  #endif

  //read the data from buffer
  if (buffer->head != NULL)
  {
    *data = buffer->head->data;
  }

  pthread_mutex_lock(&(buffer->remove_mtx));

  #ifdef DEBUG
    printf("remove_flag is %d\n",(*buffer).remove_flag );
  #endif

  if ((*buffer).remove_flag == 0)
  {
    (*buffer).remove_flag = 1;
    #ifdef DEBUG
      printf("waiting for remove_cond  \n");
    #endif
    pthread_cond_wait(&(buffer->remove_cond), &(buffer->remove_mtx));
    pthread_mutex_unlock(&(buffer->remove_mtx));
    return SBUFFER_SUCCESS;
  }
  //remove head from sbuffer
  dummy = buffer->head;
  pthread_mutex_lock(&(buffer->mtx));
  if (buffer->head == buffer->tail) // buffer has only one node
  {
    buffer->head = buffer->tail = NULL;
  }
  else  // buffer has many nodes empty
  {
    buffer->head = buffer->head->next;
  }
  free(dummy);
  pthread_mutex_unlock(&(buffer->mtx));
  #ifdef DEBUG
    printf("first read head data finsih!\n");
  #endif
  //clear the remove_flag
  (*buffer).remove_flag = 0;
  pthread_cond_signal(&(buffer->remove_cond));
  pthread_mutex_unlock(&(buffer->remove_mtx));
  return SBUFFER_SUCCESS;
}


int sbuffer_insert(sbuffer_t * buffer, sbuffer_data_t * data)
{

  sbuffer_node_t * dummy;
  if (buffer == NULL)
  {
    #ifdef DEBUG
      printf("buffer is NULL\n");
    #endif
    return SBUFFER_FAILURE;
  }
  dummy = malloc(sizeof(sbuffer_node_t));
  if (dummy == NULL) return SBUFFER_FAILURE;
  dummy->data = *data;
  dummy->next = NULL;

  pthread_mutex_lock(&(buffer->mtx));
  if (buffer->tail == NULL) // buffer empty (buffer->head should also be NULL
  {
    buffer->head = buffer->tail = dummy;
  }
  else // buffer not empty
  {
    buffer->tail->next = dummy;
    buffer->tail = buffer->tail->next;
  }
  pthread_mutex_unlock(&(buffer->mtx));

  pthread_cond_broadcast(&(buffer->read_cond));
  return SBUFFER_SUCCESS;
}
