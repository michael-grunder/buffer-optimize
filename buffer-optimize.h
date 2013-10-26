#ifndef BUFFER_OPTIMIZE_H
#define BUFFER_OPTIMIZE_H

#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <zlib.h>
#include <time.h>
#include <getopt.h>

#include "cmdhash.h"
#include "buffer.h"

#define BUFFER_OPTIMIZE_VERSION "0.1.0"

/**
 * Simple macro to detect a .gz extension
 */
#define IS_GZ_FILE(f, l) \
    (f[l-3]=='.' && f[l-2]=='g' && f[l-1]=='z')

/** 
 * Hash sizes to use
 */
#define KHASH_SIZE 22016
#define MHASH_SIZE 512

/**
 * How much data to read from our input file at a time.  This
 * is set to 1024 bytes because hiredis will attempt to memmove
 * the buffer if it processes more than this amount of data,
 * which will slow down performance.
 */
#define CHUNK_SIZE 1024

typedef struct _optimizerContext {
    /*
     * Input and output files
     */
    char infile[1024];
    char outfile[1024];

    /**
     * Input FD
     */
    gzFile fd_in;

    /**
     * Output FD if any
     */
    FILE *fd_out;
    gzFile fd_out_gz;

    /**
     * Do we just want statistics
     */
    unsigned short stats;

    /**
     * Quiet mode
     */
    unsigned short quiet;

    /**
     * Do we want the output to be gzipped
     */
    unsigned short gz;

    /**
     * Total input commands processed
     */
    unsigned int cmd_count;

    /**
     * Timing information
     */
    clock_t start;
    clock_t end;

    /**
     * Our redisReader
     */
    redisReader *reader;

    /**
     * Redis output buffer
     */
    cmdBuffer *cmd_buffer;

    /**
     * ZINCRBY hash object
     */
    cmdHash *cmd_hash;

} optimizerContext;

static const struct option g_long_opts[] = {
    { "gzip", no_argument, NULL, 'z' },
    { "stat", no_argument, NULL, 's' },
    { "quiet", no_argument, NULL, 'q' },
    { "version", no_argument, NULL, 'v' },
    { "help", no_argument, NULL, 'h' },
    { 0, 0, 0, 0 }
};

#endif
