LOAD DATA INFILE "/data/hudson/tests/data/mysqlFlat.dat"
INTO TABLE test
FIELDS
TERMINATED BY ","
OPTIONALLY ENCLOSED BY '*'
