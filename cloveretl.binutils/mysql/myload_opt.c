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
* @since 09/05/06  
*/

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>

#include "myload_opt.h"

struct myload_opt *getopt_myload(int argc, char *argv[], struct myload_opt *opt) {
	struct option longopt[] = {
		{"help", no_argument, NULL, 'h'},
		{"username", required_argument, NULL, 'u'},
		{"password", required_argument, NULL, 'p'},
		{"database", required_argument, NULL, 'd'},
		{"hostname", required_argument, NULL, 'H'},
		{"port", required_argument, NULL, 'P'},
		{"table", required_argument, NULL, 't'},
		{"replace", no_argument, NULL, 'r'},
		{0, 0, 0, 0}
	};
	int curr;
	int idx;
	int c;

	
	opt->user = opt->pwd = opt->dbase = opt->host = opt->table = NULL;
	opt->port = 0;
	opt->replace = 0;
	
	while ((c = getopt_long(argc, argv, ":u:p:d:H:P:t:h", longopt, &idx)) != -1) {
		switch ((char)c) {
			case ':':
				fprintf(stderr, "Missing parameter for an option\n");
				return NULL;
			break;
			case 'u':
				opt->user = optarg;
			break;
			case 'p':
				opt->pwd = optarg;
			break;
			case 'd':
				opt->dbase = optarg;
			break;
			case 'H':
				opt->host = optarg;
			break;
			case 'P': {
				char *endptr;
				opt->port = strtol(optarg, &endptr, 10);
				if (*endptr != 0 || opt->port < 0) {
					fprintf(stderr, "Invalid argument for option %s\n", longopt[idx].name);
					return NULL;
				}
			} break;
			case 't':
				opt->table = optarg;
			break;
			case '?':
			case 'r':
				opt->replace = 1;
			case 'h':
			default:
				printf("\nUsage: myload [options]\n"
					"available options:\n"
					"\t-h, --help\n"
					"\t-u, --username=<username>\n"
					"\t-p, --password=<password>\n"
					"\t-d, --database=<database>\n"
					"\t-H, --hostname=<hostname>\n"
					"\t-P, --port=<port>\n"
					"\t-t, --table=<table>\n"
					"\t\tmandatory argument\n"
				);
			return NULL;
		}
	}

	/* process rest of the commandline */
	
	if (opt->table == NULL)	{
		fprintf(stderr, "Missing arguments\n");
		return NULL;
	}
	
	return opt;
}
