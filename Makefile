CC=gcc

all: libsqlitefunctions.so

libsqlitefunctions.so: extension-functions.c
	$(CC) -fPIC -lm -shared extension-functions.c -o libsqlitefunctions.so
