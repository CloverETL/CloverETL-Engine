# linux settings
#ORAPATH=/opt/oracle/product/10.2.0.1
#ORALIBPATH=$(ORAPATH)/lib
#LIBS=-lnnz10  -lclntst10 -lpthread -ldl -lm 	# static libraries
#LIBS=-lclntsh									# dynamic libraries

# windows (mingw+msys) settings
#ORAPATH=d:/oracle/product/10.1.0/client_1
#ORALIBPATH=$(ORAPATH)/precomp/lib
#LIBS=-lorasql10

all: build

build: myload

myload_opt.o: myload_opt.c myload_opt.h
	gcc -c myload_opt.c -o myload_opt.o

myload: myload_opt.o myload.o
	gcc myload_opt.o myload.o -o myload \
	-lmysqlclient

myload.o: myload_opt.h myload.c
	gcc -c myload.c -o myload.o \
	-I/usr/include/mysql

clean:
	rm -f myload *.o
