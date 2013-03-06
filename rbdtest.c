#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <rados/librados.h>
#include <rbd/librbd.h>

/* configuration */
int blocksize = 512;
int count = 10;
char *imagename;
char *poolname = "rbd";
int writemode = 0;

/* runtime */
rados_t cluster;
rados_ioctx_t ioctx;
rbd_image_t ih;

int getint(const char *s);
void usage(void);

int
main(int argc, char **argv)
{
	int c;
	int rc;

	while ((c = getopt(argc, argv, "b:c:i:p:w")) != -1) {
		switch (c) {
		case 'b':
			blocksize = getint(optarg);
			break;
		case 'c':
			count = getint(optarg);
			break;
		case 'i':
			imagename = optarg;
			break;
		case 'p':
			poolname = optarg;
			break;
		case 'w':
			writemode = 1;
			break;
		default:
			usage();
		}
	}
	if (imagename == NULL) {
		fprintf(stderr, "Need image name\n");
		exit(2);
	}

	rc = rados_create(&cluster, NULL);
	if (rc < 0) {
		fprintf(stderr, "rados_create: %s\n", strerror(-rc));
		exit(2);
	}
	rc = rados_conf_read_file(cluster, NULL);
	if (rc < 0) {
		fprintf(stderr, "rados_conf_read_file: %s\n", strerror(-rc));
		exit(2);
	}
	rc = rados_connect(cluster);
	if (rc < 0) {
		fprintf(stderr, "rados_connect: %s\n", strerror(-rc));
		exit(2);
	}
	rc = rados_ioctx_create(cluster, poolname, &ioctx);
	if (rc < 0) {
		fprintf(stderr, "rados_ioctx_create: %s\n", strerror(-rc));
		exit(2);
	}
	rc = rbd_open(ioctx, imagename, &ih, NULL);
	if (rc < 0) {
		fprintf(stderr, "rbd_open: %s\n", strerror(-rc));
		exit(2);
	}

	exit(0);
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
