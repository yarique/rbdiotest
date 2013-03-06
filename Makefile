LDLIBS+=	-lrados -lrbd

all:	rbdtest

rbdtest:	rbdtest.o
