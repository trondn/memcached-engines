/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Definition of a persistent engine that serves items from memory, but also
 * tries to save it to a persistant storage.
 *
 * Copy: See COPYING for the status of this software.
 *
 */
#ifndef PERSISTENT_ENGINE_H
#define PERSISTENT_ENGINE_H

#include "config.h"

#include <pthread.h>
#include <stdbool.h>

#include <memcached/engine.h>
#ifdef __cplusplus
extern "C" {
#endif

/* Slab sizing definitions. */
#define POWER_SMALLEST 1
#define POWER_LARGEST  200
#define CHUNK_ALIGN_BYTES 8
#define DONT_PREALLOC_SLABS
#define MAX_NUMBER_OF_SLAB_CLASSES (POWER_LARGEST + 1)

/** How long an object can reasonably be assumed to be locked before
    harvesting it on a low memory condition. */
#define TAIL_REPAIR_TIME (3 * 3600)


/* Forward decl */
struct persistent_engine;

#include "items.h"
#include "assoc.h"
#include "slabs.h"
#include "sqlite.h"

   /* Flags */
#define ITEM_WITH_CAS 1

#define ITEM_LINKED (1<<8)

/* temp */
#define ITEM_SLABBED (2<<8)

struct config {
    bool use_cas;
    size_t verbose;
    rel_time_t oldest_live;
    bool evict_to_free;
    size_t maxbytes;
    bool preallocate;
    float factor;
    size_t chunk_size;
    size_t item_size_max;
    bool warmup;
    char *dbname;
};

MEMCACHED_PUBLIC_API
ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle);

/**
 * Statistic information collected by the persistent engine
 */
struct engine_stats {
    pthread_mutex_t lock;
    uint64_t evictions;
    uint64_t curr_bytes;
    uint64_t curr_items;
    uint64_t total_items;
    uint64_t reclaimed;
};

/**
 * Definition of the private instance data used by the persistent engine.
 *
 * This is currently "work in progress" so it is not as clean as it should be.
 */
struct persistent_engine {
    ENGINE_HANDLE_V1 engine;
    SERVER_HANDLE_V1 server;
    GET_SERVER_API get_server_api;
    
    /* Handle to the reader and writer classes. The rest of the code is in C
     * so let's just use a void pointer :-)
     */
    void *reader;
    void *writer;

    /**
     * Is the engine initalized or not
     */
    bool initialized;

    struct assoc assoc;
    struct slabs slabs;
    struct items items;

    /**
     * The cache layer (item_* and assoc_*) is currently protected by
     * this single mutex
     */
    pthread_mutex_t cache_lock;

    struct config config;
    struct engine_stats stats;
};

char* item_get_data(const hash_item* item);
const char* item_get_key(const hash_item* item);
void item_set_cas(ENGINE_HANDLE *handle, const void *cookie, item* item, uint64_t val);
uint64_t item_get_cas(const hash_item* item);
uint8_t item_get_clsid(const hash_item* item);

#ifdef __cplusplus
}
#endif

#endif
