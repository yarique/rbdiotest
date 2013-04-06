#include <sys/time.h>
#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <rados/librados.h>
#include <rbd/librbd.h>

/* configuration */
long blocksize = 512;
long count = 10;
char *imagename;
int iomode = 'S';
long iosize;
long maxqlen;
char *poolname = "rbd";
int readcache = 0;
int terse = 0;
int verbose = 0;
int writecache = 0;
int writemode = 0;

/* runtime */
rados_t cluster;
rados_ioctx_t ioctx;
rbd_image_t ih;

int aio_inflight;
pthread_cond_t aio_inflight_cond;
pthread_mutex_t aio_inflight_mtx;

pthread_cond_t aio_queue_cond;
pthread_mutex_t aio_queue_mtx;

struct queue_entry {
	struct queue_entry *prev;
	struct queue_entry *next;
	void *data;
};

struct queue {
	struct queue_entry *head;
	struct queue_entry *tail;
} aioqueue;

int dotest(void);
long getint(const char *s);
void usage(void);

void aio_cb(rbd_completion_t c, void *arg);
void *queue_pickup(void *dummy);

int aioloop(char *buf, uint64_t *offset);
int queuedloop(char *buf, uint64_t *offset);
int syncloop(char *buf, uint64_t *offset);

int
main(int argc, char **argv)
{
	int c;
	int rc;

	while ((c = getopt(argc, argv, "RWb:c:i:m:p:q:s:tvw")) != -1) {
		switch (c) {
		case 'R':
			readcache = 1;
			break;
		case 'W':
			writecache = 1;
			break;
		case 'b':
			blocksize = getint(optarg);
			break;
		case 'c':
			count = getint(optarg);
			break;
		case 'i':
			imagename = optarg;
			break;
		case 'm':
			iomode = toupper(optarg[0]);
			break;
		case 'p':
			poolname = optarg;
			break;
		case 'q':
			maxqlen = getint(optarg);
			break;
		case 's':
			iosize = getint(optarg);
			count = iosize / blocksize;
			break;
		case 't':
			terse++;
			break;
		case 'v':
			verbose++;
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

	if (readcache || writecache) {
		rc = rados_conf_set(cluster, "rbd_cache", "true");
		if (!(rc < 0) && !writecache)
			rc = rados_conf_set(cluster, "rbd_cache_max_dirty", "0");
	} else
		rc = rados_conf_set(cluster, "rbd_cache", "false");
	if (rc < 0) {
		fprintf(stderr, "cache control: %s\n", strerror(-rc));
		exit(2);
	}
	if (verbose)
		printf("RBD cache read %d write %d\n", readcache, writecache);

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
	long i;
	intmax_t dt;
	uint64_t offset;
	uintmax_t rate;

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
		printf("Start %s IO loop with %ld cycles, %ld bytes per each\n",
		    writemode ? "write" : "read", count, blocksize);
	offset = 0;
	gettimeofday(&tv0, NULL);
	switch (iomode) {
	case 'A':
		if (verbose) {
			printf("Async mode loop, max queue length ");
			if (maxqlen)
				printf("%ld\n", maxqlen);
			else
				printf("unlimited\n");
		}
		if (aioloop(buf, &offset) < 0)
			return (-1);
		break;
	case 'Q':
		if (verbose) {
			printf("Queued mode loop, max queue length ");
			if (maxqlen)
				printf("%ld\n", maxqlen);
			else
				printf("unlimited\n");
		}
		if (queuedloop(buf, &offset) < 0)
			return (-1);
		break;
	case 'S':
		if (verbose)
			printf("Sync mode loop\n");
		if (syncloop(buf, &offset) < 0)
			return (-1);
		break;
	default:
		fprintf(stderr, "Bad IO mode: %c\n", iomode);
		return (-1);
	}
	gettimeofday(&tv, NULL);

	dt = (intmax_t)1000000LL * (tv.tv_sec - tv0.tv_sec) +
	    tv.tv_usec - tv0.tv_usec;
	if (!terse) {
		printf("Time elapsed: %jd usec\n", dt);
		printf("Bytes xferred: %"PRIu64"\n", offset);
	}

	if (dt > 0) {
		rate = (uintmax_t)offset * 1000000 / (uintmax_t)dt;
		if (!terse)
			printf("IO rate: %ju byte/s\n", rate);
		else
			printf("%ju\n", rate);
	} else
		printf("IO rate would be %s!\n",
		    dt == 0 ? "infinity" : "negative");

	return (0);
}

/* asynchronous IO based implementation */
int
aioloop(char *buf, uint64_t *offset)
{
	rbd_completion_t c;
	long i;
	int rc;

	if (pthread_mutex_init(&aio_inflight_mtx, NULL) != 0) {
		perror("pthread_mutex_init");
		return (-1);
	}
	if (pthread_cond_init(&aio_inflight_cond, NULL) != 0) {
		perror("pthread_cond_init");
		return (-1);
	}

	for (i = 0; i < count; i++) {
		rc = rbd_aio_create_completion(NULL, aio_cb, &c);
		if (rc < 0) {
			fprintf(stderr, "create_completion: %s\n",
			    strerror(-rc));
			return (-1);
		}

		pthread_mutex_lock(&aio_inflight_mtx);
		if (maxqlen > 0) {
			for (;;) {
				if (aio_inflight < maxqlen)
					break;
				pthread_cond_wait(&aio_inflight_cond,
				    &aio_inflight_mtx);
			}
		}
		aio_inflight++;
		pthread_mutex_unlock(&aio_inflight_mtx);

		if (writemode)
			rc = rbd_aio_write(ih, *offset, blocksize, buf, c);
		else
			rc = rbd_aio_read(ih, *offset, blocksize, buf, c);

		if (rc < 0) {
			fprintf(stderr, "rbd_aio: %s\n", strerror(-rc));
			return (-1);
		}

		*offset += blocksize;	/* we'll bail out on short read */
	}

	if (verbose)
		printf("Now waiting for all AIO to complete\n");
#if 1
	pthread_mutex_lock(&aio_inflight_mtx);
	for (;;) {
		if (aio_inflight < 0) {
			pthread_mutex_unlock(&aio_inflight_mtx);
			fprintf(stderr, "Oooooops!\n");
			return (-1);
		}
		if (aio_inflight == 0)
			break;
		pthread_cond_wait(&aio_inflight_cond, &aio_inflight_mtx);
	}
	pthread_mutex_unlock(&aio_inflight_mtx);
#else
	rbd_flush(ih);		/* DOES NOT WORK AS NEEDED */
#endif
	if (verbose)
		printf("All AIO complete\n");

	if (pthread_mutex_destroy(&aio_inflight_mtx) != 0) {
		perror("pthread_mutex_destroy");
		return (-1);
	}
	if (pthread_cond_destroy(&aio_inflight_cond) != 0) {
		perror("pthread_cond_destroy");
		return (-1);
	}

	return (0);
}

void
aio_cb(rbd_completion_t c, void *arg)
{
	(void)arg;	/* unused for now */
	(void)rbd_aio_get_return_value(c);
	rbd_aio_release(c);

	if (verbose)
		write(STDOUT_FILENO, ".", 1);

	pthread_mutex_lock(&aio_inflight_mtx);
	if (aio_inflight > 0) {
		aio_inflight--;
		if (aio_inflight == 0 ||
		    (maxqlen > 0 && aio_inflight < maxqlen)) {
			pthread_cond_broadcast(&aio_inflight_cond);
			if (verbose && aio_inflight == 0)
				write(STDOUT_FILENO, "#\n", 2);
		}
	} else
		write(STDOUT_FILENO, "Oops!\n", 6);
	pthread_mutex_unlock(&aio_inflight_mtx);
}

/* queued IO based implementation */
int
queuedloop(char *buf, uint64_t *offset)
{
	struct queue_entry *qp;
	pthread_t qthr;
	rbd_completion_t c;
	long i;
	int rc;

	if (pthread_mutex_init(&aio_inflight_mtx, NULL) != 0) {
		perror("pthread_mutex_init");
		return (-1);
	}
	if (pthread_cond_init(&aio_inflight_cond, NULL) != 0) {
		perror("pthread_cond_init");
		return (-1);
	}
	if (pthread_mutex_init(&aio_queue_mtx, NULL) != 0) {
		perror("pthread_mutex_init");
		return (-1);
	}
	if (pthread_cond_init(&aio_queue_cond, NULL) != 0) {
		perror("pthread_cond_init");
		return (-1);
	}
	if (pthread_create(&qthr, NULL, queue_pickup, NULL) != 0) {
		perror("pthread_create");
		return (-1);
	}

	for (i = 0; i < count; i++) {
		rc = rbd_aio_create_completion(NULL, NULL, &c);
		if (rc < 0) {
			fprintf(stderr, "create_completion: %s\n",
			    strerror(-rc));
			return (-1);
		}

		qp = malloc(sizeof(*qp));
		if (qp == NULL) {
			fprintf(stderr, "Out of memory\n");
			return (-1);
		}

		pthread_mutex_lock(&aio_inflight_mtx);
		if (maxqlen > 0) {
			for (;;) {
				if (aio_inflight < maxqlen)
					break;
				pthread_cond_wait(&aio_inflight_cond,
				    &aio_inflight_mtx);
			}
		}
		aio_inflight++;
		pthread_mutex_unlock(&aio_inflight_mtx);

		if (writemode)
			rc = rbd_aio_write(ih, *offset, blocksize, buf, c);
		else
			rc = rbd_aio_read(ih, *offset, blocksize, buf, c);

		if (rc < 0) {
			fprintf(stderr, "rbd_aio: %s\n", strerror(-rc));
			return (-1);
		}

		*offset += blocksize;	/* we'll bail out on short read */

		pthread_mutex_lock(&aio_queue_mtx);
		qp->prev = aioqueue.tail;
		qp->next = NULL;
		qp->data = c;
		if (aioqueue.tail != NULL)
			aioqueue.tail->next = qp;
		aioqueue.tail = qp;
		if (aioqueue.head == NULL)
			aioqueue.head = qp;
		pthread_cond_broadcast(&aio_queue_cond);
		pthread_mutex_unlock(&aio_queue_mtx);
	}

	if (verbose)
		printf("Now waiting for all AIO to complete\n");
#if 1
	pthread_mutex_lock(&aio_inflight_mtx);
	for (;;) {
		if (aio_inflight < 0) {
			pthread_mutex_unlock(&aio_inflight_mtx);
			fprintf(stderr, "Oooooops!\n");
			return (-1);
		}
		if (aio_inflight == 0)
			break;
		pthread_cond_wait(&aio_inflight_cond, &aio_inflight_mtx);
	}
	pthread_mutex_unlock(&aio_inflight_mtx);
#else
	rbd_flush(ih);		/* DOES NOT WORK AS NEEDED */
#endif
	if (verbose)
		printf("All AIO complete\n");

	if (pthread_mutex_destroy(&aio_inflight_mtx) != 0) {
		perror("pthread_mutex_destroy");
		return (-1);
	}
	if (pthread_cond_destroy(&aio_inflight_cond) != 0) {
		perror("pthread_cond_destroy");
		return (-1);
	}
	if (pthread_mutex_destroy(&aio_queue_mtx) != 0) {
		perror("pthread_mutex_destroy");
		return (-1);
	}
	if (pthread_cond_destroy(&aio_queue_cond) != 0) {
		perror("pthread_cond_destroy");
		return (-1);
	}

	return (0);
}

void *
queue_pickup(void *dummy)
{
	struct queue_entry *qp;
	rbd_completion_t c;

	(void)dummy;	/* unused */

	for (;;) {
		pthread_mutex_lock(&aio_queue_mtx);
		for (;;) {
			if (aioqueue.head != NULL)
				break;
			pthread_cond_wait(&aio_queue_cond, &aio_queue_mtx);
		}

		qp = aioqueue.head;
		aioqueue.head = qp->next;
		if (aioqueue.head != NULL)
			aioqueue.head->prev = NULL;
		else
			aioqueue.tail = NULL;
		pthread_mutex_unlock(&aio_queue_mtx);

		c = qp->data;
		free(qp);

		(void)rbd_aio_wait_for_complete(c);
		(void)rbd_aio_get_return_value(c);
		rbd_aio_release(c);

		if (verbose)
			write(STDOUT_FILENO, ".", 1);

		pthread_mutex_lock(&aio_inflight_mtx);
		if (aio_inflight > 0) {
			aio_inflight--;
			if (aio_inflight == 0 ||
			    (maxqlen > 0 && aio_inflight < maxqlen)) {
				pthread_cond_broadcast(&aio_inflight_cond);
				if (verbose && aio_inflight == 0)
					write(STDOUT_FILENO, "#\n", 2);
			}
		} else
			write(STDOUT_FILENO, "Oops!\n", 6);
		pthread_mutex_unlock(&aio_inflight_mtx);
	}
	return (NULL);
}

/* synchronous IO based implementation */
int
syncloop(char *buf, uint64_t *offset)
{
	long i;
	int64_t rc;

	for (i = 0; i < count; i++) {
		if (writemode)
			rc = rbd_write(ih, *offset, blocksize, buf);
		else
			rc = rbd_read(ih, *offset, blocksize, buf);

		/* no reason to tolerate short ios */
		if (rc < 0 || (uint64_t)rc != (uint64_t)blocksize) {
			fprintf(stderr, "rbd io returned %"PRId64" (%s)\n",
			    rc, rc < 0 ? strerror(-rc) : "short io");
			return (-1);
		}

		if (verbose) {
			printf(".");
			fflush(stdout);
		}

		*offset += rc;
	}

	if (verbose)
		printf("\n");

	return (0);
}

void
usage()
{
	fprintf(stderr, "Usage!\n");
	exit(2);
}

long
getint(const char *s)
{
	char *ep = NULL;
	long rv;
	intmax_t j;
	intmax_t scale;

	j = strtoimax(s, &ep, 0);
	if (ep == NULL) {
		fprintf(stderr, "Bad number: %s\n", s);
		exit(2);
	}
	if (*ep != '\0') {
		if (ep[1] == '\0') {
			scale = 1;
			switch (toupper(*ep)) {
			case 'T':
				scale *= 1024;
			case 'G':
				scale *= 1024;
			case 'M':
				scale *= 1024;
			case 'K':
				scale *= 1024;
				break;
			default:
				fprintf(stderr, "Bad scale: %s\n", ep);
				exit(2);
			}
			j *= scale;
		} else {
			fprintf(stderr, "Bad number: %s\n", s);
			exit(2);
		}
	}
	rv = j;
	if (rv != j) {
		fprintf(stderr, "Number out of range: %s\n", s);
		exit(2);
	}
	return (rv);
}
