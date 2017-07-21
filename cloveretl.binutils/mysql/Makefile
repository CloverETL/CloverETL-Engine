# linux settings
INCLUDEPATH=/usr/include/mysql
LIBS=-lmysqlclient

# windows (mingw+msys) settings
## well, this is difficult to automate, before calling make myload,
## we need to do following steps:
## (1) add missing symbols to include/libmysql.def (!)
## (2) $ dlltool --input-def include/libmySQL.def --dllname lib/opt/libmySQL.dll --output-lib  lib/libmysqlclient.a -A -k
#INCLUDEPATH="c:/Program files/MySQL/MySQL Server 5.0/include"
#LIBS=-L"c:/Program files/MySQL/MySQL Server 5.0/lib/" -lmysqlclient

all: build

build: myload

myload_opt.o: myload_opt.c myload_opt.h
	gcc -c myload_opt.c -o myload_opt.o

myload: myload_opt.o myload.o
	gcc myload_opt.o myload.o -o myload \
	$(LIBS)

myload.o: myload_opt.h myload.c
	gcc -c myload.c -o myload.o \
	-I$(INCLUDEPATH)

clean:
	rm -f myload *.o
