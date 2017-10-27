#include <string.h>
#include <sqlite3.h>
#include <string.h>

#include "config.h"
#include "errmacros.h"
#include "sensor_db.h"


extern FILE *fp_fifo_w;

/*
 * Make a connection to the database server
 * Create (open) a database with name DB_NAME having 1 table named TABLE_NAME
 * If the table existed, clear up the existing data if clear_up_flag is set to 1
 * Return the connection for success, NULL if an error occurs
 */
DBCONN * init_connection(char clear_up_flag)
{
	//define a database handle
	sqlite3 *db;

  //open a new database connection, create a database named DB_NAME
	int rc = sqlite3_open(TO_STRING(DB_NAME),&db);
  if (rc != SQLITE_OK)
	{
      fprintf(stderr, "Cannot open database: %s\n", sqlite3_errmsg(db));
      sqlite3_close(db);

			fprintf(fp_fifo_w, "Unable to connect to SQL server.\n");
			fflush(fp_fifo_w);

      exit(EXIT_FAILURE);
	}

	fprintf(fp_fifo_w, "Connection to SQL server established.\n");
	fflush(fp_fifo_w);
   //clear the data if the table existed
 	char  sql[256];
 	char *err_msg ;
	if (clear_up_flag)
	{
		sprintf(sql, "DROP TABLE IF EXISTS %s;\nCREATE TABLE %s%s",TO_STRING(TABLE_NAME), TO_STRING(TABLE_NAME),
			"(id INTEGER PRIMARY KEY, sensor_id INTEGER, sensor_value DECIMAL(4,2),timestamp TIMESTAMP);");
	}
	else
	{
		sprintf(sql, "CREATE TABLE IF NOT EXISTs %s%s", TO_STRING(TABLE_NAME),
			"(id INTEGER PRIMARY KEY, sensor_id INTEGER, sensor_value DECIMAL(4,2),timestamp TIMESTAMP);");
	}


  rc = sqlite3_exec(db, sql, NULL, NULL, &err_msg);
    //wrapper runs multiple statements of SQL
  if (rc != SQLITE_OK)
	{
      fprintf(stderr, "SQL error: %s\n", err_msg);

			fprintf(fp_fifo_w, "Connection to SQL server lost.\n");
			fflush(fp_fifo_w);

      sqlite3_free(err_msg);
      sqlite3_close(db);
      exit(EXIT_FAILURE);
  }
  //  printf("connected\n");

	fprintf(fp_fifo_w, "New table <%s> created.\n", TO_STRING(TABLE_NAME));
	fflush(fp_fifo_w);

  return db;
}

	/*
 * Disconnect from the database server
 */
void disconnect(DBCONN *conn)
{
	sqlite3_close(conn);
	fprintf(fp_fifo_w, "Connection to SQL server lost.\n");
	fflush(fp_fifo_w);
	//printf("disconnected\n");
}

/*
 * Write an INSERT query to insert a single sensor measurement
 * Return zero for success, and non-zero if an error occurs
 */


int insert_sensor(DBCONN * conn, sensor_id_t id, sensor_value_t value, sensor_ts_t ts)
{
	char  sql[256];
	char *err_msg ;
	sprintf(sql,"INSERT INTO %s(sensor_id, sensor_value, timestamp) VALUES(%d, %g,%ld)", TO_STRING(TABLE_NAME), id, value, ts);

	int rc;
	rc = sqlite3_exec(conn, sql, NULL, NULL, &err_msg);
	if(rc != SQLITE_OK)
	{
		fprintf(stderr,"SQL error: %s\n", err_msg);
		sqlite3_free(err_msg);
		sqlite3_close(conn);
		return -1;
	}
 	return 0;
}

int insert_sensor_from_buffer(DBCONN * conn, sbuffer_t ** buffer)
{
	sensor_data_t *sens_data = NULL;
	sens_data = malloc(sizeof(sensor_data_t));
	if (sens_data == NULL)
	{
		fprintf(stderr, "failed to malloc\n");
		return -1;
	}
	do
	{
		if(buffer != NULL && *buffer != NULL)
		{
			sbuffer_data_t sbuffer_data;
			int retval = sbuffer_remove(*buffer, &sbuffer_data, 5);
			if(retval == SBUFFER_NO_DATA) break;
			*sens_data = sbuffer_data.sensor_data;
			insert_sensor(conn, sens_data->id, sens_data->value, sens_data->ts);
		}
	} while(*buffer != NULL);
	//end do-while
	free(sens_data);
	return 0;
}


/*
 * Write an INSERT query to insert all sensor measurements available in the file 'sensor_data'
 * Return zero for success, and non-zero if an error occurs
 */
int insert_sensor_from_file(DBCONN * conn, FILE * sensor_data)
{
	sensor_data_t * data = NULL;
	data = malloc(sizeof(sensor_data_t));
	if (data == NULL)
	{
		fprintf(stderr, "failed to malloc\n");
		return -1;
	}
	while (1)
	{
		fread(&data->id, sizeof(sensor_id_t), 1, sensor_data);
		fread(&data->value, sizeof(sensor_value_t), 1, sensor_data);
		fread(&data->ts, sizeof(sensor_ts_t), 1, sensor_data);
		if (feof(sensor_data) != 0)
			break;
		insert_sensor(conn, data->id, data->value, data->ts);
	}
	free(data);
	return 0;
}
	/*
  * Write a SELECT query to select all sensor measurements in the table
  * The callback function is applied to every row in the result
  * Return zero for success, and non-zero if an error occurs
  */

int find_sensor_all(DBCONN * conn, callback_t f)
{
	char sql[256];
	char *err_msg;
	sprintf(sql, "SELECT * FROM %s;", TO_STRING(TABLE_NAME));

	int rc = sqlite3_exec(conn, sql, f, NULL, &err_msg);
	if(rc != SQLITE_OK)
	{
		fprintf(stderr,"SQL error: %s\n", err_msg);
		sqlite3_free(err_msg);
		sqlite3_close(conn);
		return -1;
	}
	return 0;
}

	/*
 * Write a SELECT query to return all sensor measurements having a temperature of 'value'
 * The callback function is applied to every row in the result
 * Return zero for success, and non-zero if an error occurs
 */
int find_sensor_by_value(DBCONN * conn, sensor_value_t value, callback_t f)
{
	char sql[256];
	char *err_msg ;
	sprintf(sql, "SELECT * FROM %s WHERE sensor_value == %f;",
	TO_STRING(TABLE_NAME), value);

	int rc = sqlite3_exec(conn, sql, f, NULL, &err_msg);
	if(rc != SQLITE_OK)
	{
		fprintf(stderr,"SQL error: %s\n", err_msg);
		sqlite3_free(err_msg);
		sqlite3_close(conn);
		return -1;
	}
	return 0;
}


/*
 * Write a SELECT query to return all sensor measurements of which the temperature exceeds 'value'
 * The callback function is applied to every row in the result
 * Return zero for success, and non-zero if an error occurs
 */
int find_sensor_exceed_value(DBCONN * conn, sensor_value_t value, callback_t f)
{
	char  sql[256];
	char *err_msg ;
	sprintf(sql, "SELECT * FROM %s WHERE sensor_value > %f;",
	TO_STRING(TABLE_NAME), value);

	int rc = sqlite3_exec(conn, sql, f, NULL, &err_msg);
	if(rc != SQLITE_OK)
	{
		fprintf(stderr,"SQL error: %s\n", err_msg);
		sqlite3_free(err_msg);
		sqlite3_close(conn);
		return -1;
	}
	return 0;
}
/*
 * Write a SELECT query to return all sensor measurements having a timestamp 'ts'
 * The callback function is applied to every row in the result
 * Return zero for success, and non-zero if an error occurs
 */

int find_sensor_by_timestamp(DBCONN * conn, sensor_ts_t ts, callback_t f)
{
	char  sql[256];
	char *err_msg ;
	sprintf(sql, "SELECT * FROM %s WHERE timestamp == %ld;",
	TO_STRING(TABLE_NAME), ts);

	int rc = sqlite3_exec(conn, sql, f, NULL, &err_msg);
	if(rc != SQLITE_OK)
	{
		fprintf(stderr,"SQL error: %s\n", err_msg);
		sqlite3_free(err_msg);
		sqlite3_close(conn);
		return -1;
	}
	return 0;
}

/*
 * Write a SELECT query to return all sensor measurements recorded after timestamp 'ts'
 * The callback function is applied to every row in the result
 * return zero for success, and non-zero if an error occurs
 */
int find_sensor_after_timestamp(DBCONN * conn, sensor_ts_t ts, callback_t f)
{
	char sql[256];
	char *err_msg ;
	int rc;
	sprintf(sql, "SELECT * FROM %s WHERE timestamp > %ld;",
	TO_STRING(TABLE_NAME), ts);

	rc = sqlite3_exec(conn, sql, f, NULL, &err_msg);
	if(rc != SQLITE_OK)
	{
		fprintf(stderr,"SQL error: %s\n", err_msg);
		sqlite3_free(err_msg);
		sqlite3_close(conn);
		return -1;
	}
	return 0;
}
