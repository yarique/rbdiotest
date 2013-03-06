LDLIBS+=	-lrados -lrbd

all:	rbdtest

clean:
	rm rbdtest rbdtest.o

rbdtest:	rbdtest.o
