//
// zHash.c
//
// Simple hash table with an outer and inner linked list used to
// aggregate ZINCRBY commands together.
//
// Author:  Mike Grunder
//
//

#include "zhash.h"


static void __freeMemberList(zHash *ht, zMemberList **members) {
    zMemberList *list, *tmp;
    int i;

    for(i=0;i<ht->msize;i++) {
        if(!members[i])
            continue;

        list = members[i];
        while(list != NULL) {
            free(list->member);

            tmp = list->next;
            free(list);
            list = tmp;
        }
    }
}

static void __freeKeyBucket(zHash *ht, zKeyList *key) {
    zKeyList *tmp;

    while(key != NULL) {
        __freeMemberList(ht, key->bucket);

        free(key->key);
        free(key->bucket);

        tmp = key->next;
        free(key);
        key = tmp;
    }
}

/*
#define GET_BUCKET(str, len, size) \
    djb2((unsigned const char *)str,len) & (size-1) \
*/

#define GET_BUCKET(str, len, size) \
    djb2((unsigned const char *)str, len) % size \

static inline int djb2(unsigned const char *str, size_t len) {
    unsigned long hash = 5381;
    int i=0, c;

    while(i<len) {
        c = str[i];
        hash = ((hash << 5) + hash) + c;
        i++;
    }

    return hash;
}

/**
 * Create a zHash object, given an outer and inner hash size
 */
zHash *zHashCreate(unsigned int ksize, unsigned int msize) {
    if(ksize < 1 || msize < 1) 
        return NULL;

    // Our hash table
    zHash *ht;

    // Allocate structure itself
    if((ht = calloc(1, sizeof(zHash))) == NULL)
        return NULL;

    // Set outer and inner hash table sizes
    ht->ksize = ksize;
    ht->msize = msize;

    // Allocate hash table
    if((ht->bucket = calloc(ksize, sizeof(zKeyList*))) == NULL) {
        free(ht);
        return NULL;
    }

    // Return our hash table
    return ht;
}

/**
 * Free memory allocated for our hash table
 */
int zHashFree(zHash *ht) {
    if(ht == NULL) 
        return -1;
    
    int i;

    // Iterate outer hash table
    for(i=0;i<ht->ksize;i++) {
        if(ht->bucket[i]==NULL)
            continue;

        // Free this key bucket
       __freeKeyBucket(ht, ht->bucket[i]);
    }

    // Free buffer, buckets, hash table
    if(ht->_buf) free(ht->_buf);
    free(ht->bucket);
    free(ht);

    return 0;
}

static inline zMemberList *__findMember(zHash *ht, zKeyList *key, const char *member, size_t len)
{
    unsigned int num = GET_BUCKET(member, len, ht->msize);
    zMemberList *item, *tail = NULL;

    // Create bucket array itself if this is a new entry
    if(key->bucket == NULL) {
        if((key->bucket = calloc(ht->msize, sizeof(zMemberList*))) == NULL) 
            return NULL;
    }
    
    if(key->bucket[num] == NULL) {
        if((key->bucket[num] = calloc(1, sizeof(zMemberList)))==NULL)
            return NULL;

        // This is a new bucket, use it
        item = key->bucket[num];
    } else {
        // Bucket has data, look for our item
        for(item = key->bucket[num]; item != NULL; item = item->next) {
            // Keep track of our last non null item
            tail = item;

            // Compare
            if(len == item->len && !strncmp(member, item->member, len)) {
                return item; // Found it
            }
        }

        // It's new, allocate
        if((item = calloc(1, sizeof(zMemberList))) == NULL)
            return NULL;

        // Append this item
        tail->next = item;
    }
    
    // Make a copy of our key
    if((item->member = strndup(member, len)) == NULL)
        return NULL;

    // Increment overall string length
    ht->str_len += len;
    
    // Increment overall command count
    ht->count++;

    // Set length
    item->len = len;

    // Return our item
    return item;
}

static inline zKeyList *__findKey(zHash *ht, const char *key, 
                                  size_t len) 
{
    unsigned int num = GET_BUCKET(key, len, ht->ksize);
    zKeyList *list, *tail = NULL;

    // Create bucket if it doesn't exist
    if(ht->bucket[num] == NULL) {
        if((ht->bucket[num] = calloc(1, sizeof(zKeyList)))==NULL)
            return NULL;

        // Bucket is new, we'll put it here
        list = ht->bucket[num];
    } else {
        // Look for it if it's not totally new
        for(list = ht->bucket[num]; list != NULL; list = list->next) {
            tail = list;
            if(list->len == len && !strncmp(list->key, key, len)) {
                return list;
            }
        }

        // Allocate our item
        if((list = calloc(1, sizeof(zKeyList))) == NULL)
            return NULL;
        
        // Append this item
        tail->next = list;
    }

    // Make a copy of our key
    if((list->key = strndup(key, len)) == NULL) {
        free(list);
        return NULL;
    }

    // Set our length
    list->len = len;

    // Return it
    return list;
}

/**
 * Add a ZINCRBY command from a redisReply object
 */
int zHashAddReply(zHash *ht, redisReply *reply) {
    double dval;
    
    if(!ht || !reply)
        return -1;

    // Quick sanity check on element count
    if(reply->elements != 4)
        return -1;

    // Convert score to double
    dval = strtod(reply->element[2]->str, NULL);

    // Pass through
    return zHashAdd(ht, reply->element[1]->str, reply->element[1]->len, dval,
                    reply->element[3]->str, reply->element[3]->len);
}

/**
 * Update our ZINCRBY value @ key/member
 */
int zHashAdd(zHash *ht, const char *key, size_t klen, 
             double value, const char *member, size_t mlen)
{
    zKeyList *klist;
    zMemberList *mlist;

    // Null, length and value check
    if(key == NULL || klen < 1 || member == NULL || mlen < 1 || !value)
        return -1;

    // Find or create our key
    if((klist = __findKey(ht, key, klen)) == NULL) 
        return -1;

    // Find or create our member
    if((mlist = __findMember(ht, klist, member, mlen)) == NULL)
        return -1;

    // Increment our value
    mlist->value += value;

    // Success
    return 0;
}

/**
 * Free our buffer
 */
static void __flushBuffer(zHash *ht) {
    if(ht->_buf) {
        free(ht->_buf);
        ht->_size=0;
        ht->_len=0;
    }
}

/**
 * Internal helper to reallocate our buffer
 */
static inline int __reallocBuffer(zHash *ht, size_t addlen) {
    size_t len = ht->_len, newlen = len+addlen;
    char *newbuf;

    // Aggressive allocation up to a point
    if(newlen < ZBUF_MAX_PREALLOC) {
        newlen *= 2;
    } else {
        newlen += ZBUF_MAX_PREALLOC;
    }

    // Reallocate buffer
    newbuf = realloc(ht->_buf, newlen);

    // Check for allocation failure
    if(newbuf == NULL)
        return -1;
    
    // Success
    ht->_size = newlen;
    ht->_buf = newbuf;
    return 0;
};

/**
 * Internal helpper to append (and reallocate if necissary) our buffer
 */
static inline int __appendBuffer(zHash *ht, char *buf, size_t len) {
    // Attempt a realloc if we need one
    if(ht->_len + len > ht->_size && __reallocBuffer(ht, len) != 0)
        return -1;

    memcpy(ht->_buf+ht->_len, buf, len);
    ht->_len+=len;

    return 0;
}

/**
 * Append each command for a given key
 */
static inline int __appendMemberCommands(zHash *ht, zKeyList *key) {
    zMemberList *mem;
    char *cmd;
    size_t len;
    int i, ovr;

    for(i=0;i<ht->msize;i++) {
        mem = key->bucket[i];
        while(mem != NULL) {
            // Format our Redis command string
            len = redisFormatCommand(&cmd, "ZINCRBY %b %f %b", key->key,
                                     key->len, mem->value, mem->member,
                                     mem->len);

            // Would this overflow
            ovr = ht->_len + len > ht->_size;

            // If we got an error from hiredis or we can't realloc, fail
            if(len < 0 || (ovr && __reallocBuffer(ht, len) < 0)) {
                __flushBuffer(ht);
                return -1;
            }

            // Append this command
            __appendBuffer(ht, cmd, len);
            free(cmd);

            // Move on
            mem = mem->next;
        }
    }

    // success
    return 0;
}

/**
 * Construct ZINCRBY command buffers
 */
int zHashGetCommands(zHash *ht, char **ret_str, size_t *ret_len) {
    zKeyList *key;
    int i;

    if(ret_str == NULL)
        return -1;

    // Flush our buffer if necissary
    __flushBuffer(ht);

    // Attempt our initial allocation
    ht->_size = ht->str_len + (8 * ht->count) + (ZINCRBY_PROT_LEN * ht->count);
    if((ht->_buf = malloc(ht->_size))==NULL) {
        ht->_size = 0;
        return -1;
    }
    
    // Iterate over each key bucket
    for(i=0;i<ht->ksize;i++) {
        key = ht->bucket[i];
        
        // Loop over each key
        while(key != NULL) {
            // Append member commands for this key
            if(__appendMemberCommands(ht, key) < 0) {
                __flushBuffer(ht);
                return -1;
            }
            key = key->next;
        }
    }

    // Set our return string and length
    *ret_str = ht->_buf;
    *ret_len = ht->_len;

    return 0;
}















