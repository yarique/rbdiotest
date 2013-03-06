#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <rados/librados.h>
#include <rbd/librbd.h>

int blocksize = 512;
int count = 10;
int writemode = 0;

int getint(const char *s);
void usage(void);

int
main(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "c:b:w")) != -1) {
		switch (c) {
		case 'c':
			count = getint(optarg);
			break;
		case 'b':
			blocksize = getint(optarg);
			break;
		case 'w':
			writemode = 1;
			break;
		default:
			usage();
		}
	}
}

void
usage()
{
	fprintf(stderr, "Usage!\n");
	exit(2);
}

int
getint(const char *s)
{
	char *ep = NULL;
	int i;
	long l;
	
	l = strtol(s, &ep, 0);
	if (ep == NULL || *ep != '\0') {
		fprintf(stderr, "Bad number: %s\n", s);
		exit(2);
	}
	i = l;
	if (i != l) {
		fprintf(stderr, "Number out of range: %s\n", s);
		exit(2);
	}
	return (i);
}
