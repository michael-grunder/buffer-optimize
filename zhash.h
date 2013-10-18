#ifndef REDIS_ZHASH_H
#define REDIS_ZHASH_H

#include <hiredis/hiredis.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/**
 * Basic ZINCRBY protocol len without the score or member sizes, which we can
 * guess at given we know how many commands we'll be sending and the total
 * string length for all keys and members.
 *      *4\r\n
 *      $<key len>\r\n
 *      <key>\r\n
 *      $<score len>\r\n
 *      <score>\r\n
 *      $<member len>\r\n
 *      <member>\r\n
 */
#define ZINCRBY_PROT_LEN \
    (sizeof("*4") + (sizeof("\r\n")*7) + (sizeof("$")*3)) \

/**
 * Maximum preallocation size
 */
#define ZBUF_MAX_PREALLOC (1024*1024)

typedef struct _zMemberList {
    // Member and length
    char *member;
    size_t len;

    // Our value
    double value;
    
    // linked list iteration
    struct _zMemberList *next;
} zMemberList;

typedef struct _zKeyList {
    // Key and length
    char *key;
    size_t len;
    
    // Hash table of members
    zMemberList **bucket;
    
    // Next/Tail
    struct _zKeyList *next;
} zKeyList;

typedef struct _zHash {
    // Key and member hash table size
    unsigned int ksize;
    unsigned int msize;
    
    // Number of ZINCRBY commands
    unsigned int count;

    // Total length of keys and members
    unsigned int str_len;

    // Hash table buckets
    zKeyList **bucket;

    // Internal command buffer
    char *_buf;
    size_t _size;
    size_t _len;
} zHash;

zHash *zHashCreate(unsigned int ksize, unsigned int msize);

int zHashFree(zHash *ht);

int zHashAddReply(zHash *ht, redisReply *reply);

int zHashAdd(zHash *ht, const char *key, size_t klen, 
             double value, const char *member, size_t mlen);

int zHashGetCommands(zHash *ht, char **ret, size_t *len);

#endif
