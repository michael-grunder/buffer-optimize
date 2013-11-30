CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
LINK=-lz -lhiredis -lm
DEBUG?=-g -ggdb
OPTIMIZATION?=-O2
CFLAGS=-Wall $(DEBUG) $(OPTIMIZATION)
INSTALL_PATH?=/usr/local
BIN=buffer-optimize
DEPS=buffer.c cmdhash.c buffer-optimize.c
#MANPREFIX?=/usr/share/man/man1
#MANPAGE=csv-split.1
#MANCMP=csv-split.1.gz

.PHONY: debug

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

buffer-optimize: buffer.o cmdhash.o buffer-optimize.o
	$(CC) -o $(BIN) buffer.o cmdhash.o buffer-optimize.o $(CFLAGS) $(LINK)

debug:
	$(MAKE) OPTIMIZATION=""

all:
	$(MAKE) DEBUG=""

clean:
	rm -f *.o *.gz $(BIN)

install: all
	#gzip -c $(MANPAGE) > $(MANPAGE).gz && cp -pf $(MANPAGE).gz $(MANPREFIX)
	cp -pf $(BIN) $(INSTALL_PATH)/bin

dep: 
	$(CC) -MM *.c

