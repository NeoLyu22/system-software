#ifndef _CONNMGR_H_
#define _CONNMGR_H_

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <assert.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <string.h>

#include "sbuffer.h"

typedef struct conn conn_t;
typedef struct tcpsock tcpsock_t;
void connmgr_free();
/*This method should be called to clean up the connmgr, and to free all used
 * memory. After this no new connections will be accepted*/

void connmgr_listen(int port_number, sbuffer_t **buffer);
/*This method holds the core functionality of your connmgr. It starts listening
 * on the given port and when when a sensor node connects it writes the data to
 * a sensor_data_recv file. This file must have the same format as the
 * sensor_data file in assignment 6 and 7.*/
//
// void build_select_readfds(int * max_fd);
// /*This method creates the set of descriptors for select function*/
//
void handle_new_data(FILE * fp, sbuffer_t *buffer);
/*This method reads data from client socket and pass them to write_data_to_file().*/

void handle_new_sensor();
/*This method waits for new connection, When there is new connection, the info of the
 * connection will be stored in the conn_list.*/



#endif /* _CONNMGR_H_*/
