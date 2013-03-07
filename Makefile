CFLAGS+=	-Wall -W
LDLIBS+=	-lrados -lrbd

all:	rbdiotest

clean:
	rm rbdiotest rbdiotest.o

rbdiotest:	rbdiotest.o
