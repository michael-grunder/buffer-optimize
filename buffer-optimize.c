/*
 * buffer-optimize.c
 *
 *  Created on: Oct 15, 2013
 *      Author: mike
 */

#include "buffer-optimize.h"

/**
 * Open our input and possibly output file
 */
int openFiles(optimizerContext *ctx) {
    if((ctx->fd_in = gzopen(ctx->infile, "r")) == NULL)
        return -1;

    // We may not need to open our output file if we're just runing stats
    if(*ctx->outfile) {
        if(ctx->gz) {
            if((ctx->fd_out_gz = gzopen(ctx->outfile, "w")) == NULL)
                return -1;
        } else {
            if((ctx->fd_out = fopen(ctx->outfile, "w")) == NULL)
                return -1;
        }
    }

    // Success
    return 0;
}

/**
 * Write our output file
 */
int writeFile(optimizerContext *ctx, char *buffer, size_t size) {
    size_t written;

    // Write either to our gzFile or FILE*
    if(ctx->gz) {
        written = gzwrite(ctx->fd_out_gz, buffer, size);
    } else {
        written = fwrite(buffer, 1, size, ctx->fd_out);
    }

    if(written != size)
        return -1;

    // Success
    return 0;
}

/**
 * Process our input buffer, using our cmdHash object to aggregate ZINCRBY
 * and SADD commands that can be combined together.  We read from the input file
 * in chunks, passing data to a redisReader object for parsing.  
 *
 * When we encounter anything except for a ZINCRBY command, we append it to 
 * our Redis command buffer in the raw Redis protocol.
 */
int processBufferFile(optimizerContext *ctx) {
    unsigned int read;
    char buffer[CHUNK_SIZE];
    redisReply *reply;

    // While we can read data
    while((read = gzread(ctx->fd_in, buffer, sizeof(buffer))) > 0) {
        // Feed the reader
        redisReaderFeed(ctx->reader, buffer, read);

        // Process replies
        do {
            // Read the next available reply
            if(redisReaderGetReply(ctx->reader, (void**)&reply) == REDIS_ERR) {
                return -1;
            }

            // If we have a reply, process it
            if(reply) {
                // Either add it to our hash or to our pass-thru buffer
                if(cmdHashAdd(ctx->cmd_hash, reply)==TYPE_UNSUPPORTED) {
                    cmdBufferAddReply(ctx->cmd_buffer, reply);
                }

                // Increment total commands processed
                ctx->cmd_count++;

                // Clean up reply
                freeReplyObject(reply);
            }
        } while(reply);
    }

    // Success
    return 0;
}

/**
 * Append aggregated commands to our command buffer or just add the aggregated
 * counts depending on stat mode.
 */
int appendAggCommands(optimizerContext *ctx) {
    char *cmd;
    size_t size;

    if(!ctx->stats) {
        // Get aggregated and hashed commands
        if(cmdHashGetCommands(ctx->cmd_hash, &cmd, &size)!=0)
            return -1;

        // Append it to our overall command buffer
        if(cmdBufferAppend(ctx->cmd_buffer, cmd, size, cmdHashGetCount(ctx->cmd_hash))<0)
            return -1;
    } else {
        // Just add the aggregated command count, no need to process
        ctx->cmd_buffer->cmd_count += cmdHashGetCount(ctx->cmd_hash);
    }

    // Success
    return 0;
}

/**
 * Output statistics about our compression
 */
void outputStats(optimizerContext *ctx) {
    double pct=0.0, timing;

    // Calculate how long the compression took
    timing = ((double)(ctx->end-ctx->start)) / CLOCKS_PER_SEC;

    // Calculate compression ratio
    if(ctx->cmd_count > 0) {
        pct = 1-((double)cmdHashGetCount(ctx->cmd_hash))/(double)ctx->cmd_count;
    } 

    // Output input file
    printf("%s\t", ctx->infile);

    // Print output file if we have one
    if(*ctx->outfile) {
        printf("%s\t", ctx->outfile);
    }

    // Print the rest of our statistics
    printf("%d\t%d\t%2.2f\t%f\n",
           ctx->cmd_count, cmdHashGetCount(ctx->cmd_hash),
           pct, timing);
}

/**
 * Simple usage output
 */
void printUsage(char *cmd) {
    printf("%s: [OPTIONS] INFILE OUTFILE\n", cmd);
    printf("   --stat     Display statistics but don't write anything\n");
    printf("   --gzip     Compress output file with gzip\n");
    printf("   --version  Print version number\n");
    printf("   --quiet    Don't output information about compression\n");
    printf("   --help     This message\n");
}

/**
 * Parse our command's arguments and set context
 */
void parseArgs(optimizerContext *ctx, int argc, char **argv) {
    int opt, opt_idx;

    while((opt = getopt_long(argc, argv, "qszvh", g_long_opts, &opt_idx)) != -1) {
        switch(opt) {
            case 'q':
                // Don't print anything
                ctx->quiet = 1;
                break;
            case 's':
                // We just want statistics
                ctx->stats = 1;
                break;
            case 'z':
                // gzip output buffer file
                ctx->gz = 1;
                break;
            case 'v':
                printf("buffer-optimize " BUFFER_OPTIMIZE_VERSION "\n");
                exit(0);
            case 'h':
            case '?':
                printUsage(argv[0]);
                exit(0);
        }
    }

    // quiet and stat mode make no sense together
    if(ctx->quiet && ctx->stats) {
        fprintf(stderr, "Setting both quiet and stat mode doesn't make sense!\n");
        exit(1);
    }

    // We'll need an input file
    if(!argv[optind] || !*argv[optind]) {
        fprintf(stderr, "Error:  Must specify input file!\n");
        exit(1);
    }

    // If we're not in stats mode, we'll need an output file
    if(!ctx->stats && (!argv[optind+1] || !*argv[optind+1])) {
        fprintf(stderr, "Error:  Must specificy output file!\n");
        exit(1);
    }

    // Copy in our input and output files
    strncpy(ctx->infile, argv[optind], sizeof(ctx->infile));

    // Copy in our output file if not in stats mode
    if(!ctx->stats) {
        strncpy(ctx->outfile, argv[optind+1], sizeof(ctx->outfile));

        // Append .gz extension if it's not already there
        if(ctx->gz &&!IS_GZ_FILE(ctx->outfile, strlen(ctx->outfile)))
        {
            strncat(ctx->outfile, ".gz", sizeof(ctx->outfile));
        }
    }
}


void initContext(optimizerContext *ctx) {
    // Zero out everything
    memset(ctx, 0, sizeof(optimizerContext));

    // Make sure we can allocate our command buffer object
    if((ctx->cmd_buffer = cmdBufferCreate()) == NULL) {
        fprintf(stderr, "Error:  Can't create command buffer!\n");
        exit(1);
    }

    // Make sure we can allocate our cmdHash
    if((ctx->cmd_hash = cmdHashCreate(KHASH_SIZE, MHASH_SIZE)) == NULL) {
        fprintf(stderr, "Error:  Couldn't create cmdHash object\n");
        exit(1);
    }

    // Create our redisReader object
    if((ctx->reader = redisReaderCreate()) == NULL) {
        fprintf(stderr, "Error:  Couldn't create redisReader\n");
        exit(1);
    }
}

/**
 * Free our context
 */
void freeContext(optimizerContext *ctx) {
    // Free our redisReader
    if(ctx->reader)
        redisReaderFree(ctx->reader);

    // Free our command buffer
    if(ctx->cmd_buffer) 
        cmdBufferFree(ctx->cmd_buffer);

    // Free our ZINCRBY hash
    if(ctx->cmd_hash)
        cmdHashFree(ctx->cmd_hash);

    // Close our input file
    if(ctx->fd_in)
        gzclose(ctx->fd_in);

    // Close our non gzip output file if open
    if(ctx->fd_out)
        fclose(ctx->fd_out);

    // Close our gzip output file if open
    if(ctx->fd_out_gz)
        gzclose(ctx->fd_out_gz);
}
    
int main(int argc, char **argv) {
    optimizerContext ctx;

    // Initialize our context object
    initContext(&ctx);

    // Parse our arguments
    parseArgs(&ctx, argc, argv);

    // Start timing
    ctx.start = clock();

    // Open our input and possibly output file
    if(openFiles(&ctx) < 0) {
        fprintf(stderr, "Error:  Couldn't open input or output file!\n");
        freeContext(&ctx);
        exit(1);
    }

    // Process the command buffer
    if(processBufferFile(&ctx)<0) {
        fprintf(stderr, "Error processing file '%s'\n", ctx.infile);
        freeContext(&ctx);
        exit(1);
    }

    // Append our aggregated commands or just add to overall counts
    if(appendAggCommands(&ctx)<0) {
        fprintf(stderr, "Error appending aggregated commands!\n");
        freeContext(&ctx);
        exit(1);
    }

    // If we're not in stats mode, attempt to write the file if it's not empty
    if(!ctx.stats) {
        if(ctx.cmd_count>0 && writeFile(&ctx,ctx.cmd_buffer->buf,ctx.cmd_buffer->pos)<0) {
            fprintf(stderr, "Error writing buffer file '%s'\n", ctx.outfile);
            exit(1);
        } else if(!ctx.cmd_count) {
            fprintf(stderr, "Error:  Not writing empty command buffer!\n");
            exit(1);
        }
    }

    // Time the process
    ctx.end = clock();

    // If we're not in quiet mode, output statistics
    if(!ctx.quiet) {
        outputStats(&ctx);
    }

    // Cleanup (free buffers, close files, etc)
    freeContext(&ctx);

    // Success
    return 0;
}
