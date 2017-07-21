myload loads data from stdin to MySQL using SQL statement LOAD DATA LOCAL INFILE...

********Compilation********
prerequisities: mysql libraries, GNU make, gcc
step 1
	edit Makefile to set its variables to values conforming to your installation
step 3
	$ make build
	
	you are supposed to get binary file on successful run of the command above. in case anything fails,
	further tweaking of Makefile may be necessary

System specific notes:
WINDOWS: compilation is tricky. pls, use precompiled binary

**********Usage***********
myload [options]
available options:
        -h, --help
        -u, --username=<username>
        -p, --password=<password>
        -d, --database=<mysql db id>
		-H, --hostname=<hostname>
		-P, --port=<port>
		-t, --table=<table>
				tmandatory argument
