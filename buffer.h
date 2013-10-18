#ifndef REDIS_CMD_BUFFER_H
#define REDIS_CMD_BUFFER_H

#include <hiredis/hiredis.h>
#include <string.h>
#include <stdlib.h>

/*
 * Initial allocation size
 */
#define INITIAL_ALLOC 32768

/**
 * Maximum preallocation size
 */
#define BUF_MAX_PREALLOC (1024*1024)


/**
 * Our buffer structure
 */
typedef struct _cmdBuffer {
    /**
     * The buffer itself
     */
    char *buf;

    /**
     * Allocated memory
     */
    size_t size;

    /**
     * Current position (same as length)
     */
    size_t pos;

    /**
     * The toal number of commands in this buffer
     */
    unsigned int cmd_count;
} cmdBuffer;

// Allocation, deallocation
cmdBuffer *cmdBufferCreate(void);
int cmdBufferFree(cmdBuffer *buffer);

// Feed a redisReply directly into our command buffer this will append to the
// command in the Redis protocol to the end of our bufer  end of our buffer.
int cmdBufferAddReply(cmdBuffer *buffer, redisReply *reply);

// Append data directly into the buffer (which should already be in the Redis
// protocol.
int cmdBufferAppend(cmdBuffer *buffer, char *str, size_t len, unsigned int cmd_count);

#endif
