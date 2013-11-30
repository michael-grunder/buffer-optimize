#ifndef REDIS_ZHASH_H
#define REDIS_ZHASH_H

#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define CMD_ZINCRBY "ZINCRBY"
#define CMD_SADD    "SADD"

#define ARG_MAX 1024*1024

/**
 * Initial buffer allocation size
 */
#define BUF_INIT_ALLOC 32768

/**
 * Maximum preallocation size
 */
#define BUF_MAX_PREALLOC (1024*1024)

/**
 * For now we'll support ZINCRBY and SADD
 */
typedef enum _cmdType {
    TYPE_ZINCRBY,
    TYPE_SADD,
    TYPE_UNSUPPORTED
} cmdType;

/**
 * Leaf linked list to store members for SADD
 * aggregation or members and scores for
 * ZINCRBY commands.
 */
typedef struct _cmdMemberList {
    /**
     * Member and member length
     */
    char *member;
    size_t len;

    /**
     * Score for ZINCRBY type commands, and
     * a hit count for SADD commands.
     */
    union {
        double score;
        unsigned int hits;
    };

    /**
     * Pointer to next element
     */
    struct _cmdMemberList *next;
} cmdMemberList;

/**
 * A hash table with members involved with a given key
 */
typedef struct _cmdKeyList {
    /**
     * Key and length
     */
    char *key;
    size_t len;

    /**
     * Number of members for this key
     */
    unsigned int count;

    /**
     * Member hash table
     */
    cmdMemberList **bucket;

    /**
     * Next key
     */
    struct _cmdKeyList *next;
} cmdKeyList;

/**
 * Container for a hash of keys and members that keeps track of count and
 * overall string length for the keys and members involved
 */
typedef struct _cmdHashContainer {
    /**
     * Size of the key and member hash tables
     */
    unsigned int ksize, msize;

    /**
     * Key, member count
     */
    unsigned int keys;
    unsigned int members;

    /**
     * Total string length of all keys + members
     */
    unsigned int str_len;

    /**
     * Command hash itself
     */
    cmdKeyList **bucket;
} cmdHashContainer;

/**
 * Our command hash object which can be used to aggregate
 * ZINCRBY and SADD commands.
 */
typedef struct _cmdHash {
    /**
     * Aggregated SADD commands, with info
     */
    cmdHashContainer *s_cmds;

    /**
     * Aggregated ZINCRBY commands, with info
     */
    cmdHashContainer *z_cmds;

    /**
     * Internal buffer storage for protocol generation
     */
    char *_buf;
    size_t _size;
    size_t _len;
} cmdHash;

cmdHash *cmdHashCreate(unsigned int ksize, unsigned int msize);

int cmdHashFree(cmdHash *ht);
int cmdHashAdd(cmdHash  *ht, redisReply *reply);
int cmdHashGetCommands(cmdHash *ht, char **ret, size_t *len);
unsigned cmdHashGetCount(cmdHash *ht);

#endif
