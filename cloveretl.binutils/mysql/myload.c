/*
*    jETeL/Clover - Java based ETL application framework.
*    Copyright (C) 2006 Javlin Consulting <info@javlinconsulting>
*    
*    This library is free software; you can redistribute it and/or
*    modify it under the terms of the GNU Lesser General Public
*    License as published by the Free Software Foundation; either
*    version 2.1 of the License, or (at your option) any later version.
*    
*    This library is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU    
*    Lesser General Public License for more details.
*    
*    You should have received a copy of the GNU Lesser General Public
*    License along with this library; if not, write to the Free Software
*    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*
* @author Jan Hadrava (jan.hadrava@javlinconsulting.cz), Javlin Consulting (www.javlinconsulting.cz)
* @since 10/18/06  
*/

/* This program lets specified mysql server process a "LOAD DATA LOCAL INFILE" query and supplies data
 * from stdin instead of the file */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef WIN32
#include <io.h>
#include <fcntl.h>
#endif

#include <mysql.h>

#include "myload_opt.h"

void check_error(MYSQL *mysql) {
	if (mysql_errno(mysql) == 0) {
		return;
	}
	fprintf(stderr, "mysql error: \"%s\"\n", mysql_error(mysql));
	exit(-1);
}

int infile_init(void **ptr, const char *filename, void *userdata) {
	return 0;
}

int infile_read(void *ptr, char *buf, unsigned int buf_len) {
	int n;
	n = fread(buf, 1, buf_len, stdin);
	if (ferror(stdin)) {
		perror("error occured while reading input data");
		return -1;
	}
	return n;
}

void infile_end(void *ptr) {
}

int infile_error(void *ptr, char *error_msg, unsigned int error_msg_len) {
	*error_msg = 0;
	return -1;
}

int main(int argc, char **argv) {
	struct myload_opt options;
	MYSQL *mysql = NULL;
	char *query1 = "LOAD DATA LOCAL INFILE '' INTO TABLE ";
	char *query = NULL;

#ifdef WIN32
	/*set output to binary mode to avoid newline transformation*/
	setmode(fileno(stdout), O_BINARY);
#endif

	if (getopt_myload(argc, argv, &options) == NULL) {
		exit(-1);
	}

	if (mysql_library_init(-1, NULL, NULL) != 0) {
		fprintf(stderr, "An error occured while initializing mysql library\n");
		exit(-1);
	}

	if ((mysql = mysql_init(NULL)) == NULL) {
		fprintf(stderr, "Unsufficient memory to initialize mysql\n");
		exit(-1);
	}
	
	mysql_real_connect(mysql, options.host, options.user, options.pwd, options.dbase,
		options.port, NULL, 0);
	check_error(mysql);
	
	mysql_set_local_infile_handler(mysql, infile_init, infile_read, infile_end, infile_error, NULL);
	check_error(mysql);
	
	query = malloc(100 + strlen(options.table));
	*query = 0;
	strcat(query, "LOAD DATA LOCAL INFILE '' "); 
	strcat(query, options.replace ? "REPLACE" : "IGNORE");
	strcat(query, " INTO TABLE ");	
	strcat(query, options.table);

	printf("sending query \"%s\" to mysql server\n", query);
	mysql_query(mysql, query);	
	check_error(mysql);
	
	printf("mysql info: %s\n", mysql_info(mysql));
	
	mysql_close(mysql);
	mysql_library_end();
	
	return 0;	
}
