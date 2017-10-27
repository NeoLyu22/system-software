#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#include "errmacros.h"
#include "lib/dplist.h"
#include "datamgr.h"
#include "config.h"

int dplist_errno;
dplist_t *list = NULL;
sensor_data_t *sensor_data = NULL;
extern FILE *fp_fifo_w;

typedef struct {
	sensor_id_t sensor_id;
	uint16_t room_id;
	sensor_value_t recent_values[RUN_AVG_LENGTH];
	sensor_value_t avg;
	int counter;
  int buffer_size;
	time_t timestamp;
} sensor_element_t;

void* datamgr_element_copy(void * element);
void datamgr_element_free(void** element);
int datamgr_element_compare(void* x,void* y);

struct dplist_node{
	dplist_node_t *prev,*next ;
	void *element;
};



sensor_element_t * get_element_by_id(sensor_id_t sensor_id)
	{
	dplist_node_t *dummy;
	//literation the list to find the pointer to the element
	for (dummy = dpl_get_first_reference(list); dummy != NULL; dummy = dpl_get_next_reference(list, dummy))
	{
		if(((sensor_element_t *)(dpl_get_element_at_reference(list, dummy)))->sensor_id == sensor_id)
		return (sensor_element_t *)(dpl_get_element_at_reference(list, dummy));
	}
	return NULL;
}

void update_element(sensor_element_t *sensor_element, sensor_value_t value, sensor_ts_t ts )
{
	if (sensor_element!= NULL)
	{
			//update the sensor node data
			sensor_element->timestamp = ts;
			sensor_element->recent_values[sensor_element->counter++] = value;

			if (sensor_element->buffer_size < RUN_AVG_LENGTH)
			sensor_element->buffer_size++;

			if (sensor_element->counter >= RUN_AVG_LENGTH)
			sensor_element->counter = 0;

			// update the average
			if (sensor_element->buffer_size >= RUN_AVG_LENGTH)
			{
				// recalcuate the avg
				sensor_element->avg = 0;
				int i;
				for ( i = 0; i < RUN_AVG_LENGTH; i++)
				{
					sensor_element->avg += sensor_element->recent_values[i];
				}
				sensor_element->avg = sensor_element->avg/RUN_AVG_LENGTH;

				if (sensor_element->avg > SET_MAX_TEMP) {
				fprintf(stderr, "Warning: The room %" PRIu16 " is too hot!\n", sensor_element->room_id);
				}
				if (sensor_element->avg < SET_MIN_TEMP) {
				fprintf(stderr, "Warning: The room %" PRIu16 " is too cold!\n", sensor_element->room_id);
				}
			}
	}
}

void datamgr_parse_sensor_files(FILE * fp_sensor_map, FILE * fp_sensor_data)
{
//create the list
	sensor_element_t sensor_element;
	sensor_data_t sensor_data;

	// create the list
	list = dpl_create(datamgr_element_copy,datamgr_element_free,datamgr_element_compare);

	// initalise the sensor_element with zeros
	sensor_element.avg = 0;
	sensor_element.timestamp = 0;
	sensor_element.counter = 0;
	sensor_element.buffer_size = 0;

	int i;
	for(i = 0; i < RUN_AVG_LENGTH; i++) sensor_element.recent_values[i] = 0;

	// read ascii file room_sensor.map
	while(fscanf(fp_sensor_map, "%" SCNu16 " %" SCNu16 "\n", &sensor_element.room_id, &sensor_element.sensor_id)!=EOF) {
		dpl_insert_at_index(list, (void*)&sensor_element,0,true);
	}
	// read binary file for the sensor data
	while ((fread(&sensor_data.id, sizeof(sensor_id_t), 1, fp_sensor_data)) == 1
	&& (fread(&sensor_data.value, sizeof(sensor_value_t), 1, fp_sensor_data)) == 1
	&& (fread(&sensor_data.ts, sizeof(sensor_ts_t), 1, fp_sensor_data)) == 1)
	{
		// update sensor element
		update_element(get_element_by_id(sensor_data.id), sensor_data.value, sensor_data.ts);
	}
}




void datamgr_parse_sensor_data(FILE * fp_sensor_map, sbuffer_t  **buffer)
{
	//create the list

		sensor_element_t *sensor_element = NULL;
		sensor_element = malloc(sizeof(sensor_element_t));
		MALLOC_ERROR(sensor_element);


		sensor_data = malloc(sizeof(sensor_data_t));
		MALLOC_ERROR(sensor_data);

		// initalise the sensor_element with zeros
		// sensor_element->avg = 0;
		// sensor_element->timestamp = 0;
		sensor_element->counter = 0;
		sensor_element->buffer_size = 0;
		int i;
		for(i = 0; i < RUN_AVG_LENGTH; i++) sensor_element->recent_values[i] = 0;
		// memset(sensor_element->recent_values, 0, sizeof(sensor_element->recent_values));

		// create the list
		list = dpl_create(datamgr_element_copy,datamgr_element_free,datamgr_element_compare);

		// read ascii file room_sensor.map
		while(fscanf(fp_sensor_map, "%" SCNu16 " %" SCNu16 "\n", &sensor_element->room_id, &sensor_element->sensor_id)!=EOF)
		{

			dpl_insert_at_index(list, sensor_element,0,true);
		}
		free(sensor_element);
		sensor_element = NULL;

	// 	// print out all the sensor_node stored in the list.
	// int j = dpl_size(list);
	// for (i = 0; i<j; i++)
	// {
	// 	sensor_element_t * element = dpl_get_element_at_index(list, i);
	// 	// element->counter = 0;
	// 	printf("element at index %d = %"PRIu16" %" PRIu16" counter = %d   \n", i, element->room_id, element->sensor_id, element->counter);
	// }

		// read data from sbuffer
		while(1)
		{
			if (list != NULL)
			{
				sbuffer_data_t sbuffer_data;
				int retval = sbuffer_remove(*buffer,&sbuffer_data, 5);
				if(retval == SBUFFER_NO_DATA) break;

				*sensor_data = sbuffer_data.sensor_data;
				sensor_element_t * temp = get_element_by_id(sensor_data->id);
				#ifdef DEBUG
					printf("element Id: %d\t \t", (int)sensor_data->id);
				#endif


				if (temp == NULL)
				{
					fprintf(fp_fifo_w, "Received sensor data with invalid sensor node ID %"PRIu16"\n", sensor_data->id);
					fflush(fp_fifo_w);
					#ifdef DEBUG
								printf( "Received sensor data with invalid sensor node ID %"PRIu16"\n", sensor_data->id);
					#endif
					continue;
				}
				if (temp != NULL )
				{

						// update the sensor node data
						if (temp->counter >= (RUN_AVG_LENGTH))
						temp->counter = 0;
						#ifdef DEBUG
								printf("counter value is %d\n",temp->counter );
						#endif

						temp->timestamp = sensor_data->ts;
						temp->recent_values[temp->counter++] = sensor_data->value;
						//
						if (temp->buffer_size < (RUN_AVG_LENGTH))
						temp->buffer_size++;

						// update the average
						if (temp->buffer_size >= (RUN_AVG_LENGTH))
						{
							// printf("segmentation flag 2 \n");
							// recalcuate the avg
							temp->avg = 0;
							int i;
							for ( i = 0; i < (RUN_AVG_LENGTH); i++)
							{
								temp->avg += temp->recent_values[i];
							}
							temp->avg = (temp->avg) / (RUN_AVG_LENGTH-1);
							// printf("avg value is %f\n",temp->avg );
							if (temp->avg > SET_MAX_TEMP)
							{
								// printf("segmentation flag 3 \n");
								#ifdef DEBUG
								fprintf(stderr, "Warning: The room %" PRIu16 " is too hot!\n", temp->room_id);
								#endif
								fprintf(fp_fifo_w, "The sensor node with %"PRIu16" reports it’s too hot (running avg temperature = %f)\n", temp->sensor_id, temp->avg);
								fflush(fp_fifo_w);
							}
							if (temp->avg < SET_MIN_TEMP)
							{
								// printf("segmentation flag 4 \n");
								#ifdef DEBUG
								fprintf(stderr, "Warning: The room %" PRIu16 " is too cold!\n", temp->room_id);
								#endif
								fprintf(fp_fifo_w, "The sensor node with %"PRIu16" reports it’s too cold (running avg temperature = %f)\n", temp->sensor_id, temp->avg);
								fflush(fp_fifo_w);
							}
						}//end if
						// printf("segmentation flag 5 \n");
				}//end if
			}//end if
		}//end while
}

void datamgr_free(){
	free(sensor_data);
	sensor_data = NULL;
	dpl_free(&list,true);
}
//This method should be called to clean up the datamgr, and to free all used memory. After this, any call to datamgr_get_room_id, datamgr_get_avg, datamgr_get_last_modified or datamgr_get_total_sensors will not return a valid result

uint16_t datamgr_get_room_id(sensor_id_t sensor_id)
{
	sensor_element_t * sensor_element = get_element_by_id(sensor_id);
	//~ ERROR_HANDLER(sensor_element == NULL);
    if(sensor_element != NULL) return sensor_element->room_id;
    else return -1;

}
/*
 * Gets the room ID for a certain sensor ID
 * Use ERROR_HANDLER() if sensor_id is invalid
 */
sensor_value_t datamgr_get_avg(sensor_id_t sensor_id)
{
    sensor_element_t * sensor_element = get_element_by_id(sensor_id);
    //~ ERROR_HANDLER(sensor_element == NULL);
    if(sensor_element != NULL) return sensor_element->avg;
    return 0;
}

//Gets the running AVG of a certain senor ID (if less then RUN_AVG_LENGTH measurements are recorded the avg is 0)

time_t datamgr_get_last_modified(sensor_id_t sensor_id)
{
	sensor_element_t * sensor_element = get_element_by_id(sensor_id);
	//~ ERROR_HANDLER(sensor_element == NULL);
    if(sensor_element != NULL) return sensor_element->timestamp;
    else return 0;
}
//Returns the time of the last reading for a certain sensor ID

int datamgr_get_total_sensors(){
	return dpl_size(list);
}

//Return the total amount of unique sensor ID's recorded by the datamgr


void * datamgr_element_copy(void * element) {
    sensor_element_t* copy = malloc(sizeof (sensor_element_t));
		if (copy == NULL)
		{
			fprintf(stderr, "\nIn %s - function %s at line %d: failed\n", __FILE__, __func__, __LINE__);
			exit(EXIT_FAILURE);
		}
    copy->sensor_id = ((sensor_element_t*)element)->sensor_id;
    copy->room_id = ((sensor_element_t*)element)->room_id;
    return (void *) copy;
}

void datamgr_element_free(void ** element) {
    free(*element);
    *element = NULL;
}

int datamgr_element_compare(void * x, void * y) {
		if (((sensor_element_t *)x)->sensor_id == ((sensor_element_t *)y)->sensor_id)
			return 0;
		else if (((sensor_element_t *)x)->sensor_id > ((sensor_element_t *)y)->sensor_id)
			return 1;
		return -1;
}
