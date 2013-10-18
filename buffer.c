/**
 * Append only redis command buffer
 */

#include "buffer.h"

cmdBuffer* cmdBufferCreate(void) {
    cmdBuffer *buffer;

    // Allocate our buffer structure
    if((buffer = calloc(1, sizeof(cmdBuffer))) == NULL)
        return NULL;

    // Allocate our initial buffer
    if((buffer->buf = malloc(INITIAL_ALLOC)) == NULL) {
        free(buffer);
        return NULL;
    }

    // Set our initial size
    buffer->size = INITIAL_ALLOC;

    // Success
    return buffer;
}

int cmdBufferFree(cmdBuffer *buffer) {
    if(!buffer)
        return -1;

    if(buffer->buf)
        free(buffer->buf);

    free(buffer);

    return 0;
}

int cmdBufferAddReply(cmdBuffer *buffer, redisReply *reply) {
    if(!buffer || !reply)  
        return -1;

    char **argv, *cmd, intargv[reply->elements][33];
    size_t argvlen[reply->elements];
    unsigned int len;
    int i, retval;

    if((argv = malloc(sizeof(char*)*reply->elements)) == NULL) 
        return -1;

    for(i=0;i<reply->elements;i++) {
        switch(reply->element[i]->type) {
            case REDIS_REPLY_STRING:
                argv[i] = reply->element[i]->str;
                argvlen[i] = reply->element[i]->len;
                break;
            case REDIS_REPLY_INTEGER:
                argvlen[i] = snprintf(intargv[i], sizeof(intargv[i]), "%lld",
                                      reply->integer);
                argv[i] = (char*)intargv[i];
                break;
        }
    }

    // Format the command
    if((len = redisFormatCommandArgv(&cmd, reply->elements, (const char **)argv, (const size_t*)argvlen))<0)
        return -1;

    // Append the command
    retval = cmdBufferAppend(buffer, cmd, len, 1);

    // Free our allocated argv array
    free(argv);

    // Free command that hiredis allocated
    free(cmd);

    return retval;
}

// Reallocate our command buffer
static inline int cmdBufferGrow(cmdBuffer *buffer, size_t addlen) {
    size_t newlen = buffer->pos+addlen;
    char *newbuf;

    if(newlen < BUF_MAX_PREALLOC) {
        newlen *= 2;
    } else {
        newlen += BUF_MAX_PREALLOC;
    }

    if((newbuf = realloc(buffer->buf, newlen)) == NULL)
        return -1;

    buffer->size = newlen;
    buffer->buf = newbuf;
    return 0;
}

// Append redis protocl string into our buffer
int cmdBufferAppend(cmdBuffer *buffer, char *str, size_t len, 
                    unsigned int cmd_count) 
{
    // Reallocate if necissary
    if(buffer->pos + len > buffer->size && cmdBufferGrow(buffer, len)!=0)
        return -1;

    // Copy in the data, increment our position
    memcpy(buffer->buf+buffer->pos, str, len);
    buffer->pos += len;

    // Add to our command count
    buffer->cmd_count += cmd_count;

    return 0;
}
