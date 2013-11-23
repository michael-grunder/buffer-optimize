//
// cmdhash.c
//
// Simple hash table with an outer and inner linked list used to
// aggregate ZINCRBY commands together.
//
// Author:  Mike Grunder
//
//

#include "cmdhash.h"

/**
 * Free our command buffer
 */
static void __flush_buffer(cmdHash *ht) {
    if(ht->_buf) {
        // Free buffer, reset size/len
        free(ht->_buf);
        ht->_buf = NULL;
        ht->_size = 0;
        ht->_len = 0;
    }
}

/*
 * Free member pointers
 */
static void __free_member_list(cmdHashContainer *c, cmdMemberList **members) {
    cmdMemberList *list, *tmp;
    int i;

    for(i=0;i<c->msize;i++) {
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

/**
 * Free a key bucket and all associated members
 */
static void __free_key_bucket(cmdHashContainer *c, cmdKeyList *key) {
    cmdKeyList *tmp;

    while(key != NULL) {
        __free_member_list(c, key->bucket);

        free(key->key);
        free(key->bucket);

        tmp = key->next;
        free(key);
        key = tmp;
    }
}

static cmdHashContainer *__container_create(unsigned int ksize,
                                            unsigned int msize)
{
    cmdHashContainer *c;

    if((c = malloc(sizeof(cmdHashContainer)))==NULL)
        return NULL;

    if((c->bucket = calloc(ksize, sizeof(cmdKeyList*)))==NULL) {
        free(c);
        return NULL;
    }

    // Set key/member bucket sizes
    c->ksize = ksize;
    c->msize = msize;

    // Initialize counts
    c->keys = 0;
    c->members = 0;

    return c;
}


/**
 * Free a command container pointer
 */
static void __container_free(cmdHashContainer *c) {
    int i;

    for(i=0;i<c->ksize;i++) {
        if(c->bucket[i]==NULL)
            continue;

        __free_key_bucket(c, c->bucket[i]);
    }

    // Free bucket pointer and container itself
    free(c->bucket);
    free(c);
}

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
 * Create a command hash object
 */
cmdHash *cmdHashCreate(unsigned int ksize, unsigned int msize)
{
    // Hash table size sanity checks
    if(ksize < 1 || msize < 1)
        return NULL;

    // Our return value
    cmdHash *ht;

    if((ht = calloc(1,sizeof(cmdHash)))==NULL)
        return NULL;

    if((ht->s_cmds = __container_create(ksize,msize))==NULL) {
        free(ht);
        return NULL;
    }

    if((ht->z_cmds = __container_create(ksize,msize))==NULL) {
        __container_free(ht->s_cmds);
        return NULL;
    }

    // Return our hash table
    return ht;
}

/**
 * Free our command hash object
 */
int cmdHashFree(cmdHash *ht) {
    if(ht == NULL)
        return -1;

    // Free ZINCRBY and SADD hashes
    __container_free(ht->s_cmds);
    __container_free(ht->z_cmds);

    // Free our buffer
    if(ht->_buf) free(ht->_buf);

    // Free our hash table
    free(ht);

    return 0;
}
    
/**
 * Find or create a member hash in a given key hash bucket
 */
static inline cmdMemberList *__find_member(cmdHashContainer *c, cmdKeyList *key,
                                        const char *member, size_t len)
{
    unsigned int num = GET_BUCKET(member, len, c->msize);
    cmdMemberList *item, *tail = NULL;

    // Create bucket array itself if this is a new entry
    if(key->bucket == NULL) {
        if((key->bucket = calloc(c->msize, sizeof(cmdMemberList*))) == NULL)
            return NULL;
    }
    
    if(key->bucket[num] == NULL) {
        if((key->bucket[num] = calloc(1, sizeof(cmdMemberList)))==NULL)
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
                // Increment hit count
                item->hits++;

                return item; // Found it
            }
        }

        // It's new, allocate
        if((item = calloc(1, sizeof(cmdMemberList))) == NULL)
            return NULL;

        // Append this item
        tail->next = item;
    }
    
    // Make a copy of our key
    if((item->member = strndup(member, len)) == NULL)
        return NULL;

    // Increment overall string length
    c->str_len += len;
    
    // Increment member count for this key
    key->count++;

    // Increment overall member count
    c->members++;

    // Set length
    item->len = len;

    // Return our item
    return item;
}

/**
 * Find or create  akey
 */
static inline cmdKeyList *__find_key(cmdHashContainer *c, const char *key,
                                  size_t len) 
{
    unsigned int num = GET_BUCKET(key, len, c->ksize);
    cmdKeyList *list, *tail = NULL;

    // Create bucket if it doesn't exist
    if(c->bucket[num] == NULL) {
        if((c->bucket[num] = calloc(1, sizeof(cmdKeyList)))==NULL)
            return NULL;

        // Bucket is new, we'll put it here
        list = c->bucket[num];
    } else {
        // Look for it if it's not totally new
        for(list = c->bucket[num]; list != NULL; list = list->next) {
            tail = list;
            if(list->len == len && !strncmp(list->key, key, len)) {
                return list;
            }
        }

        // Allocate our item
        if((list = calloc(1, sizeof(cmdKeyList))) == NULL)
            return NULL;
        
        // Append this item
        tail->next = list;
    }

    // Make a copy of our key
    if((list->key = strndup(key, len)) == NULL) {
        free(list);
        return NULL;
    }

    // Increment key count
    c->keys++;

    // Set our length
    list->len = len;

    // Return it
    return list;
}

/**
 * Append a ZINCRBY command to our hash
 */
static int __hash_zincrby_cmd(cmdHash *ht, redisReply *r) {
    cmdKeyList *k;
    cmdMemberList *m;

    // Find or create the key
    if((k = __find_key(ht->z_cmds, r->element[1]->str, r->element[1]->len))==NULL)
        return -1;

    // Find or create the member
    if((m = __find_member(ht->z_cmds, k, r->element[3]->str, r->element[3]->len))==NULL)
        return -1;

    // Increment our score
    m->score += strtod(r->element[2]->str, NULL);

    // Success
    return 0;
}

/**
 * Append an SADD command to our hash
 */
static int __hash_sadd_cmd(cmdHash *ht, redisReply *r) {
    cmdKeyList *k;
    cmdMemberList *m;
    int i;

    // Find or create this key
    if((k = __find_key(ht->s_cmds, r->element[1]->str, r->element[1]->len))==NULL)
        return -1;

    // Find or create every member being added
    for(i=2;i<r->elements;i++) {
        // Find or create the member
        m = __find_member(ht->s_cmds, k, r->element[i]->str, r->element[i]->len);

        // We had a failure if we couldn't find or create it
        if(m == NULL)
            return -1;
    }

    // Success
    return 0;
}

/**
 * Determine if we can add this command to the hash
 */
static inline cmdType __get_type(redisReply *r) {
    // Check for ZINCRBY
    if(r->elements==4 && !strncasecmp(r->element[0]->str,"ZINCRBY",sizeof("ZINCRBY")))
       return TYPE_ZINCRBY;

    // Check for SADD
    if(r->elements>2 && !strncasecmp(r->element[0]->str,"SADD",sizeof("SADD")))
       return TYPE_SADD; 

   // Not a supported command
   return TYPE_UNSUPPORTED;
}

int cmdHashAdd(cmdHash *ht, redisReply *r) {
    // Add based on command content
    switch(__get_type(r)) {
        case TYPE_ZINCRBY:
            return __hash_zincrby_cmd(ht,r);
            break;
        case TYPE_SADD:
            return __hash_sadd_cmd(ht,r);
            break;
        default:
            return TYPE_UNSUPPORTED;
            break;
    }
}

/**
 * Internal helper to reallocate our buffer
 */
static inline int __realloc_buffer(cmdHash *ht, size_t addlen) {
    size_t len = ht->_len, newlen = len+addlen;
    char *newbuf;

    // Aggressive allocation up to a point
    if(newlen < BUF_MAX_PREALLOC) {
        newlen *= 2;
    } else {
        newlen += BUF_MAX_PREALLOC;
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
static inline int __append_buffer(cmdHash *ht, char *buf, size_t len) {
    // Attempt a realloc if we need one
    if(ht->_len + len > ht->_size && __realloc_buffer(ht, len) != 0)
        return -1;

    memcpy(ht->_buf+ht->_len, buf, len);
    ht->_len+=len;

    return 0;
}

/**
 * Append a ZINCRBY command for every member relating to a given key
 */
static inline int __append_zincrby_key_cmds(cmdHash *ht, cmdKeyList *key) {
    cmdMemberList *mem;
    char *cmd;
    size_t len;
    int i, ovr;

    for(i=0;i<ht->z_cmds->msize;i++) {
        mem = key->bucket[i];
        while(mem != NULL) {
            // Construct our ZINCRBY command
            len = redisFormatCommand(&cmd, "ZINCRBY %b %f %b", key->key,
                                     key->len, mem->score, mem->member,
                                     mem->len);

            // Would this overflow our buffer
            ovr = ht->_len + len > ht->_size;

            if(len < 0 || (ovr && __realloc_buffer(ht, len)<0)) {
                __flush_buffer(ht);
                return -1;
            }

            // Append this command
            __append_buffer(ht, cmd, len);
            free(cmd);

            // Move on
            mem = mem->next;
        }
    }

    // Success
    return 0;
}

/**
 * Append all ZINCRBY commands we've got hashed
 */
static inline int __append_zincrby_cmds(cmdHash *ht) {
    cmdKeyList *key;
    int i;

    // Iterate zincrby buckets
    for(i=0;i<ht->z_cmds->ksize;i++) {
        key = ht->z_cmds->bucket[i];
        while(key != NULL) {
            if(__append_zincrby_key_cmds(ht, key)<0)
                return -1;

            // Move on
            key=key->next;
        }
    }

    // Success
    return 0;
}

/**
 * Append an SADD command for however many members it contains
 */
static inline int __append_sadd_key_cmd(cmdHash *ht, cmdKeyList *key) 
{
    cmdMemberList *mem;
    char **argv, *cmd = NULL;
    size_t *argvlen;
    unsigned int len;
    int i, idx = 2, retval;

    // Allocate argument array
    if(!(argv = malloc(sizeof(char*)*(key->count+2)))) {
        return -1;
    }
    // Allocate size array
    if(!(argvlen = malloc(sizeof(size_t)*(key->count+2)))) {
        free(argv);
        return -1;
    }

    // "SADD"
    argv[0] = "SADD";
    argvlen[0] = sizeof("SADD")-1;

    // <key>
    argv[1] = key->key;
    argvlen[1] = key->len;

    // Iterate our buckets
    for(i=0;i<ht->s_cmds->msize;i++) {
        mem = key->bucket[i];
        while(mem != NULL) {
            // Add this member
            argv[idx] = mem->member;
            argvlen[idx] = mem->len;

            // Move forward
            idx++;
            mem = mem->next;
        }
    }

    // Attempt to format this command
    len = redisFormatCommandArgv(&cmd, key->count+2, (const char **)argv,
                                 (const size_t*)argvlen);

    // Fail if there is a hiredis error
    if(len < 1 || !cmd) return -1;
    
    // Append to our buffer
    retval = __append_buffer(ht, cmd, len);

    // Cleanup
    free(cmd);
    free(argv);
    free(argvlen);
    
    // Return success/failure
    return retval;
}

/**
 * Append all SADD commands we have hashed
 */
static inline int __append_sadd_cmds(cmdHash *ht) {
    cmdKeyList *key;
    int i;

    // Iterate sadd buckets
    for(i=0;i<ht->s_cmds->ksize;i++) {
        key = ht->s_cmds->bucket[i];
        while(key!=NULL) {
            // Add SADD members for this key
            if(__append_sadd_key_cmd(ht, key)<0)
                return -1;

            // Move on
            key=key->next;
        }
    }

    // Success
    return 0;
}

/**
 * Get our aggregated command buffer
 */
int cmdHashGetCommands(cmdHash *ht, char **ret, size_t *len) {
    if(!ht || !ret || !len)
        return -1;

    // Flush our buffer if we've already created one
    __flush_buffer(ht);

    // Initial buffer allocation
    if((ht->_buf = malloc(BUF_INIT_ALLOC)) == NULL)
       return -1; 

    // Add ZINCRBY commands
    if(__append_zincrby_cmds(ht) < 0)
        return -1;

    // Append SADD commands
    if(__append_sadd_cmds(ht) < 0)
       return -1;

    // Set our return pointers
    *ret = ht->_buf;
    *len = ht->_len;

    // Success
    return 0;
}

/**
 * Our total command count is the number of ZINCRBY members and SADD keys.
 */
unsigned int cmdHashGetCount(cmdHash *ht) {
    if(ht == NULL) return -1;
    return ht->z_cmds->members + ht->s_cmds->keys;
}
