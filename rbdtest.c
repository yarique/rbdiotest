#include <sys/time.h>
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <rados/librados.h>
#include <rbd/librbd.h>

/* configuration */
int blocksize = 512;
int count = 10;
char *imagename;
char *poolname = "rbd";
int verbose = 1;
int writemode = 0;

/* runtime */
rados_t cluster;
rados_ioctx_t ioctx;
rbd_image_t ih;

int dotest(void);
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
	if (verbose)
		printf("Created cluster\n");
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
	if (verbose)
		printf("Connected cluster\n");
	rc = rados_ioctx_create(cluster, poolname, &ioctx);
	if (rc < 0) {
		fprintf(stderr, "rados_ioctx_create: %s\n", strerror(-rc));
		goto cleanup2;
	}
	if (verbose)
		printf("Created io context for pool '%s'\n", poolname);
	rc = rbd_open(ioctx, imagename, &ih, NULL);
	if (rc < 0) {
		fprintf(stderr, "rbd_open: %s\n", strerror(-rc));
		goto cleanup1;
	}
	if (verbose)
		printf("Opened rbd image '%s'\n", imagename);

	rc = dotest();

	rbd_close(ih);
	if (verbose)
		printf("Closed rbd image\n");
cleanup1:
	rados_ioctx_destroy(ioctx);
	if (verbose)
		printf("Destroyed io context\n");
cleanup2:
	rados_shutdown(cluster);
	if (verbose)
		printf("Shut down cluster\n");

	exit(rc < 0 ? 2 : 0);
}

int
dotest()
{
	struct timeval tv, tv0;
	char *buf;
	int i;
	long long dt;
	uint64_t offset;
	int64_t rc;

	buf = malloc(blocksize);
	if (buf == NULL) {
		fprintf(stderr, "Out of memory\n");
		return (-1);
	}
	if (writemode) {
		/* fill the buffer with random data */
		srandom(time(NULL) % getpid());
		for (i = 0; i < blocksize; i++)
			buf[i] = random();
	}
	if (verbose)
		printf("Start IO loop\n");
	offset = 0;
	gettimeofday(&tv0, NULL);
	for (i = 0; i < count; i++) {
		if (writemode)
			rc = rbd_write(ih, offset, blocksize, buf);
		else
			rc = rbd_read(ih, offset, blocksize, buf);

		/* no reason to tolerate short ios */
		if (rc < 0 || (uint64_t)rc != (uint64_t)blocksize) {
			fprintf(stderr, "rbd io returned %"PRId64" (%s)\n",
			    rc, rc < 0 ? strerror(-rc) : "short io");
			return (-1);
		}

		offset += rc;
	}
	gettimeofday(&tv, NULL);
	dt = 1000000LL * (tv.tv_sec - tv0.tv_sec) + tv.tv_usec - tv0.tv_usec;
	printf("Time elapsed: %lld usec\n", dt);
	if (dt > 0)
		printf("IO rate: %ju byte/s\n",
		    (uintmax_t)offset * 1000000 / (uintmax_t)dt);
	else
		printf("IO rate would be %s!\n",
		    dt == 0 ? "infinity" : "negative");

	return (0);
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
