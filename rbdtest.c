#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <rados/librados.h>
#include <rbd/librbd.h>

int blocksize = 512;
int count = 10;
char *image;
char *pool = "rbd";
int writemode = 0;

int getint(const char *s);
void usage(void);

int
main(int argc, char **argv)
{
	int c;

	while ((c = getopt(argc, argv, "b:c:i:p:w")) != -1) {
		switch (c) {
		case 'b':
			blocksize = getint(optarg);
			break;
		case 'c':
			count = getint(optarg);
			break;
		case 'i':
			image = optarg;
			break;
		case 'p':
			pool = optarg;
			break;
		case 'w':
			writemode = 1;
			break;
		default:
			usage();
		}
	}
	if (image == NULL) {
		fprintf(stderr, "Need image name\n");
		exit(2);
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
