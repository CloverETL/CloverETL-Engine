# linux settings
INCLUDEPATH=/usr/include/mysql
LIBS=-lmysqlclient

# windows (mingw+msys) settings
#INCLUDEPATH=/usr/include/mysql
#LIBS=-lclntsh

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
