#define _GNU_SOURCE
#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>
#include <wait.h>

#include "connmgr.h"
#include "config.h"
#include "lib/tcpsock.h"
#include "lib/dplist.h"


// #define TIMEOUT 20

#define FILE_ERROR(fp,error_msg) 	do { \
					  if ((fp)==NULL) { \
					    printf("%s\n",(error_msg)); \
					    exit(EXIT_FAILURE); \
					  }	\
					} while(0)

fd_set readfds;
dplist_t * conn_list = NULL;
tcpsock_t * server,*client;
struct timeval tv;
int dplist_errno;
extern FILE *fp_fifo_w;

struct conn{
	tcpsock_t *client;
	time_t timestamp;
	sensor_id_t sensor_id;
} conn;


void * connmgr_element_copy(void *element)
{
	conn_t *copy = malloc(sizeof(conn_t));
	if (copy == NULL)
	{
		fprintf(stderr, "\nIn %s - function %s at line %d: failed\n", __FILE__, __func__, __LINE__);
		exit(EXIT_FAILURE);
	}
	copy->client = ((conn_t *)element)->client;
	copy->timestamp =((conn_t *)element)->timestamp;
	copy->sensor_id =((conn_t *)element)->sensor_id;
	return ( void *)copy;
}

void connmgr_element_free(void **element)
{
	free(*element);
	*element = NULL;
}

int connmgr_element_compare(void *x, void *y)
{
	int client_sdx, client_sdy;
	tcp_get_sd(((conn_t *)x)->client, &client_sdx);
	tcp_get_sd(((conn_t *)y)->client, &client_sdy);
  if(client_sdx > client_sdy) return 1;
	if(client_sdx == client_sdy) return 0;
	return -1;
}



void handle_new_sensor()
{

	if (tcp_wait_for_connection(server,&client)!=TCP_NO_ERROR) exit(EXIT_FAILURE);
		printf("handle new sensor \n");
	conn_t * connection = malloc(sizeof(conn_t));
	if (connection == NULL)
	{
		fprintf(stderr, "failed to malloc\n" );
		exit(EXIT_FAILURE);
	}
	connection->client = client;
	connection->timestamp = time(NULL);
	connection->sensor_id = 0; //default sensor_id value
	// put new sensor in the conn_list

	int size = dpl_size(conn_list);
	dpl_insert_at_index(conn_list, (void *)connection, size, true);

	#ifdef DEBUG
		size = dpl_size(conn_list);
		printf("list size %d!\n",size);
	#endif
	printf("Incoming client connection\n");
	free(connection);

}

void handle_new_data(FILE *fp, sbuffer_t *buffer)
{
	#ifdef DEBUG
		printf("status ok 4!\n");
	#endif
	sensor_data_t data;
	int bytes, result1, result2,result3;
	int client_sd;

	dplist_node_t * ref = dpl_get_first_reference(conn_list);
	while ( ref != NULL) // check all the connections for data
	{
		#ifdef DEBUG
			printf("status ok 5!\n");
		#endif
		conn_t *sock = NULL;
		sock = (conn_t *)dpl_get_element_at_reference(conn_list,ref);
		tcp_get_sd(sock->client,&client_sd);
		#ifdef DEBUG
			printf(" data client_sd %d\n",client_sd);
			printf("isset %d\n",FD_ISSET(client_sd,&readfds) );

		#endif
		if (FD_ISSET(client_sd,&readfds))
		{
			#ifdef DEBUG
				printf("status ok 6!\n");
			#endif
			// read sensor ID
			 bytes = sizeof(data.id);
			 result1 = tcp_receive(sock->client,(void *)&data.id,&bytes);
			// read temperature
			 bytes = sizeof(data.value);
			 result2 = tcp_receive(sock->client,(void *)&data.value,&bytes);
			// read timestamp
			 bytes = sizeof(data.ts);
			 result3 = tcp_receive(sock->client, (void *)&data.ts,&bytes);
			 if ((result1==TCP_NO_ERROR) && (result2==TCP_NO_ERROR) && (result3==TCP_NO_ERROR) && bytes)
			 {
				 //update the timestamp
				printf("sensor id = %" PRIu16 " - temperature = %g - timestamp = %ld\n", data.id, data.value, (long int)data.ts);
				sock->timestamp = time(NULL);

				if(sock->sensor_id == 0)
				{
					fprintf(fp_fifo_w, "A sensor node with %" PRIu16 " has opened a new connection\n", data.id);
					fflush(fp_fifo_w);
					#ifdef DEBUG
						printf( "A sensor node with %" PRIu16 " has opened a new connection\n", data.id);
					#endif
				}
				sock->sensor_id = data.id;

				//insert into sbuffer
				sbuffer_data_t sbuffer_data;
				sbuffer_data.sensor_data = data;
				if (sbuffer_insert(buffer, &sbuffer_data) != SBUFFER_SUCCESS)
				{
					#ifdef DEBUG
						printf("fail to insert data to buffer\n");
					#endif
					exit(EXIT_FAILURE);
				}
				#ifdef DEBUG
					printf("sensor id = %" PRIu16 " - temperature = %g - timestamp = %ld inserted!\n", sbuffer_data.id, sbuffer_data.value, (long int)sbuffer_data.ts);
				#endif
				// write into sensor_data_rec file
				fprintf(fp,"%" PRIu16 " %g %ld\n", data.id,data.value,(long)data.ts);
				fflush(fp);
			 }
			 else if (result3==TCP_CONNECTION_CLOSED)
			 {
				 printf("sensor node has closed connection\n");
				 tcp_close(&(sock->client));
				 FD_CLR(client_sd, &readfds);

				 fprintf(fp_fifo_w, "The sensor node with %" PRIu16 " has closed the connection\n", sock->sensor_id);
				 fflush(fp_fifo_w);
				 #ifdef DEBUG
					printf("The sensor node with %" PRIu16 " has closed the connection\n", sock->sensor_id);
				#endif

				 dplist_node_t *temp = ref;
				 ref = dpl_get_previous_reference(conn_list, ref);
				 dpl_remove_at_reference(conn_list, temp, true);
			 }
			 else
			 {
					printf("Error occured to connection\n");
					tcp_close(&(sock->client));
					FD_CLR(client_sd, &readfds);
	 				dplist_node_t *temp = ref;
	 				ref = dpl_get_previous_reference(conn_list, ref);
	 				dpl_remove_at_reference(conn_list, temp, true);
			 }
		 }
		 ref = dpl_get_next_reference(conn_list, ref);
	 }//end while

}
  /*This method holds the core functionality of your connmgr. It starts listening on the given port and when when a sensor node connects it writes the data to a sensor_data_recv file. This file must have the same format as the sensor_data file in assignment 6 and 7.
*/

  void connmgr_listen(int port_number, sbuffer_t **buffer)
  {
		  int server_sd;
			int maxfdp;

		  FILE *fp;
		  fp = fopen("sensor_data_recv", "w");
			FILE_ERROR(fp,"Couldn't create sensor_data_recv \n");
			// start the server
		  printf("Test server is started\n");
			conn_list = dpl_create(connmgr_element_copy,connmgr_element_free, connmgr_element_compare);
			#ifdef DEBUG
			int size = dpl_size(conn_list);
		 printf("after creating the list size %d!\n",size);
		 #endif
		   /*make sure the server is open*/
		  if (tcp_passive_open(&server,port_number)!=TCP_NO_ERROR) exit(EXIT_FAILURE);
		  if(server == NULL || (tcp_get_sd(server, &server_sd) == TCP_SOCKET_ERROR))  exit(EXIT_FAILURE);
		  printf("The sd of the server is %d\n", server_sd);
		  maxfdp = server_sd;

			do
			{
				//wait for timeout
		    int nready;
				int client_sd;

				tv.tv_sec = TIMEOUT;
				tv.tv_usec = 0;

				FD_ZERO(&readfds);
				FD_SET(server_sd, &readfds);
				// check al the clients for timeout
				dplist_node_t * ref = dpl_get_first_reference(conn_list);


				while ( ref != NULL)
				{
					#ifdef DEBUG
						printf("status ok 1!\n");
					#endif
					long int t = 0;
					conn_t* connection = NULL;
					connection = dpl_get_element_at_reference(conn_list,ref);
					//get current timestamp
					// time_t tp;
					// time(&tp);
					// double diffTime = difftime(tp,connection->timestamp);
					// if (diffTime >= TIMEOUT)
					t = time(NULL) - connection->timestamp;
					if (t >= TIMEOUT)
					{

						fprintf(fp_fifo_w, "The sensor node with %" PRIu16 " has closed the connection\n", connection->sensor_id);
						fflush(fp_fifo_w);

						#ifdef DEBUG
							printf("The sensor node with %" PRIu16 " has closed the connection\n", connection->sensor_id);
						#endif
						tcp_close(&connection->client);
						// printf("connection timout, disconnect peer %ld\n",t);
						dplist_node_t *temp = ref;
						ref = dpl_get_next_reference(conn_list,ref);
						dpl_remove_at_reference(conn_list,temp,true);
					}
					else
					{
						#ifdef DEBUG
							printf("status ok 2!\n");
						#endif
						tcp_get_sd(connection->client, &client_sd);
						FD_SET(client_sd,&readfds);
						#ifdef DEBUG
							printf(" add client_sd %d\n",client_sd);
						#endif
						if (client_sd > maxfdp ) maxfdp = client_sd;

						ref = dpl_get_next_reference(conn_list,ref);

					}//end if

				}//end while

				nready = select(maxfdp+1, &readfds, NULL, NULL, &tv);

		    #ifdef DEBUG
					printf("status ok 3!\n");
				#endif
				// check the recevied signal
				#ifdef DEBUG
					printf(" nready: %d\n",nready);
				#endif
				if (nready < 0 )
		    {
		      perror("select error");
					exit(EXIT_FAILURE);
				}
				else if (nready == 0 )
		    {
		      printf("connection timeout!\n");
					if (tcp_close( &server )!=TCP_NO_ERROR) exit(EXIT_FAILURE);
		 		 	printf("Test server is shutting down\n");
					break;
		    }
				else
				{
					// printf("Ready sensor number : %d \n", nready);
						if (FD_ISSET(server_sd,&readfds))
						{
							// printf("handle_new_sensor! \n");
							handle_new_sensor();
						}

						handle_new_data(fp, *buffer);

				}//end if

	}while(1); //end do-while

		dpl_free(&conn_list,true);
		fclose(fp);
}

void connmgr_free()
{
        if (tcp_close( &server )!=TCP_NO_ERROR) exit(EXIT_FAILURE);
        printf("Test server is closed !\n");
}
