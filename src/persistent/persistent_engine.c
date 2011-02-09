/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Implementation of a small engine that push items to persistent storage
 * as well.
 *
 * Author: Trond Norbye
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <unistd.h>
#include <stddef.h>

#include "persistent_engine.h"
#include "memcached/config_parser.h"

static const engine_info* persistent_get_info(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE persistent_initialize(ENGINE_HANDLE* handle,
                                               const char* config_str);
static void persistent_destroy(ENGINE_HANDLE* handle);
static ENGINE_ERROR_CODE persistent_item_allocate(ENGINE_HANDLE* handle,
                                                  const void* cookie,
                                                  item **item,
                                                  const void* key,
                                                  const size_t nkey,
                                                  const size_t nbytes,
                                                  const int flags,
                                                  const rel_time_t exptime);
static ENGINE_ERROR_CODE persistent_item_delete(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                const void* key,
                                                const size_t nkey,
                                                uint64_t cas,
                                                uint16_t vbucket);
static void persistent_item_release(ENGINE_HANDLE* handle, const void *cookie,
                                    item* item);
static ENGINE_ERROR_CODE persistent_get(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        item** item,
                                        const void* key,
                                        const int nkey,
                                        uint16_t vbucket);
static ENGINE_ERROR_CODE persistent_get_stats(ENGINE_HANDLE* handle,
                                              const void *cookie,
                                              const char *stat_key,
                                              int nkey,
                                              ADD_STAT add_stat);
static void persistent_reset_stats(ENGINE_HANDLE* handle, const void *cookie);
static ENGINE_ERROR_CODE persistent_store(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          item* item,
                                          uint64_t *cas,
                                          ENGINE_STORE_OPERATION operation,
                                          uint16_t vbucket);
static ENGINE_ERROR_CODE persistent_arithmetic(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               const void* key,
                                               const int nkey,
                                               const bool increment,
                                               const bool create,
                                               const uint64_t delta,
                                               const uint64_t initial,
                                               const rel_time_t exptime,
                                               uint64_t *cas,
                                               uint64_t *result,
                                               uint16_t vbucket);
static ENGINE_ERROR_CODE persistent_flush(ENGINE_HANDLE* handle,
                                          const void* cookie, time_t when);
static ENGINE_ERROR_CODE initalize_configuration(struct persistent_engine *se,
                                                 struct config *config,
                                                 const char *cfg_str);
static ENGINE_ERROR_CODE persistent_unknown_command(ENGINE_HANDLE* handle,
                                                    const void* cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response);

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie, const item* item, item_info *item_info);


ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                  GET_SERVER_API get_server_api,
                                  ENGINE_HANDLE **handle) {
    SERVER_HANDLE_V1 *api = get_server_api();
    if (interface != 1 || api == NULL) {
        return ENGINE_ENOTSUP;
    }

    struct persistent_engine *engine = malloc(sizeof(*engine));
    if (engine == NULL) {
        return ENGINE_ENOMEM;
    }

    struct persistent_engine persistent_engine = {
        .engine = {
            .interface = {
                .interface = 1
            },
            .get_info = persistent_get_info,
            .initialize = persistent_initialize,
            .destroy = persistent_destroy,
            .allocate = persistent_item_allocate,
            .remove = persistent_item_delete,
            .release = persistent_item_release,
            .get = persistent_get,
            .get_stats = persistent_get_stats,
            .reset_stats = persistent_reset_stats,
            .store = persistent_store,
            .arithmetic = persistent_arithmetic,
            .flush = persistent_flush,
            .unknown_command = persistent_unknown_command,
            .item_set_cas = item_set_cas,
            .get_item_info = get_item_info,
        },
        .server = *api,
        .initialized = true,
        .assoc = {
            .hashpower = 16,
        },
        .slabs = {
            .lock = PTHREAD_MUTEX_INITIALIZER
        },
        .cache_lock = PTHREAD_MUTEX_INITIALIZER,
        .stats = {
            .lock = PTHREAD_MUTEX_INITIALIZER,
        },
        .config = {
            .use_cas = true,
            .verbose = 0,
            .oldest_live = 0,
            .evict_to_free = true,
            .maxbytes = 64 * 1024 * 1024,
            .preallocate = false,
            .factor = 1.25,
            .chunk_size = 48,
            .item_size_max= 1024 * 1024,
            .warmup = false,
            .dbname = "/tmp/memcached"
        }
    };

    persistent_engine.server = *api;
    *engine = persistent_engine;

    *handle = (ENGINE_HANDLE*)&engine->engine;
    return ENGINE_SUCCESS;
}

static inline struct persistent_engine* get_handle(ENGINE_HANDLE* handle) {
    return (struct persistent_engine*)handle;
}

static inline hash_item* get_real_item(item* item) {
    return (hash_item*)item;
}

static const engine_info* persistent_get_info(ENGINE_HANDLE* handle) {
    static union {
        engine_info engine_info;
        char buffer[sizeof(engine_info) + (sizeof(feature_info) * 10)];
    } info = {
        .engine_info = {
            .description = "Persistent engine v0.1",
            .num_features = 3,
            .features = {
                [0].feature = ENGINE_FEATURE_LRU
            }
        }
    };

    info.engine_info.features[1].feature = ENGINE_FEATURE_PERSISTENT_STORAGE;
    info.engine_info.features[2].feature = ENGINE_FEATURE_CAS;

    return &info.engine_info;
}

static ENGINE_ERROR_CODE persistent_initialize(ENGINE_HANDLE* handle,
                                               const char* config_str) {
    struct persistent_engine* se = get_handle(handle);

    ENGINE_ERROR_CODE ret = initalize_configuration(se, &se->config, config_str);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    ret = assoc_init(se);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    ret = slabs_init(se, se->config.maxbytes, se->config.factor,
                     se->config.preallocate);
    if (ret != ENGINE_SUCCESS) {
        return ret;
    }

    if ((ret = sqlite_io_start_threads(se)) != ENGINE_SUCCESS) {
        return ret;
    }

    return ENGINE_SUCCESS;
}

static void persistent_destroy(ENGINE_HANDLE* handle) {
    struct persistent_engine* se = get_handle(handle);

    if (se->initialized) {
        pthread_mutex_destroy(&se->cache_lock);
        pthread_mutex_destroy(&se->stats.lock);
        se->initialized = false;
        free(se);
    }
}

static ENGINE_ERROR_CODE persistent_item_allocate(ENGINE_HANDLE* handle,
                                                  const void* cookie,
                                                  item **item,
                                                  const void* key,
                                                  const size_t nkey,
                                                  const size_t nbytes,
                                                  const int flags,
                                                  const rel_time_t exptime) {
    struct persistent_engine* engine = get_handle(handle);
    size_t ntotal = sizeof(hash_item) + nkey + nbytes;
    if (engine->config.use_cas) {
        ntotal += sizeof(uint64_t);
    }
    unsigned int id = slabs_clsid(engine, ntotal);
    if (id == 0) {
        return ENGINE_E2BIG;
    }

    hash_item *it;
    it = item_alloc(engine, key, nkey, flags, exptime, nbytes, cookie);

    if (it != NULL) {
        *item = (void*)it;
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_ENOMEM;
    }
}

static ENGINE_ERROR_CODE persistent_item_delete(ENGINE_HANDLE* handle,
                                                const void* cookie,
                                                const void* key,
                                                const size_t nkey,
                                                uint64_t cas,
                                                uint16_t vbucket) {

    item* it;
    if (persistent_get(handle, cookie, &it, key, nkey, vbucket) == ENGINE_SUCCESS) {
        item_unlink(get_handle(handle), get_real_item(it));
        item_release(get_handle(handle), get_real_item(it));
    }
    return ENGINE_SUCCESS;
}

static void persistent_item_release(ENGINE_HANDLE* handle,
                                    const void *cookie,
                                    item* item) {
    item_release(get_handle(handle), get_real_item(item));
}

static ENGINE_ERROR_CODE persistent_get(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        item** item,
                                        const void* key,
                                        const int nkey,
                                        uint16_t vbucket) {
    struct persistent_engine* engine = get_handle(handle);
    hash_item *it = item_get(engine, key, nkey);
    if (it != NULL) {
        *item = (void*)it;
        return ENGINE_SUCCESS;
    } else {
        sqlite_io_get_item(engine, cookie, key, nkey);
        return ENGINE_EWOULDBLOCK;
    }
}

static ENGINE_ERROR_CODE persistent_get_stats(ENGINE_HANDLE* handle,
                                              const void* cookie,
                                              const char* stat_key,
                                              int nkey,
                                              ADD_STAT add_stat)
{
    struct persistent_engine* engine = get_handle(handle);
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (stat_key == NULL) {
        char val[128];
        int len;

        pthread_mutex_lock(&engine->stats.lock);
        len = sprintf(val, "%llu", (unsigned long long)engine->stats.evictions);
        add_stat("evictions", 9, val, len, cookie);
        len = sprintf(val, "%llu", (unsigned long long)engine->stats.curr_items);
        add_stat("curr_items", 10, val, len, cookie);
        len = sprintf(val, "%llu", (unsigned long long)engine->stats.total_items);
        add_stat("total_items", 11, val, len, cookie);
        len = sprintf(val, "%llu", (unsigned long long)engine->stats.curr_bytes);
        add_stat("bytes", 5, val, len, cookie);
        pthread_mutex_unlock(&engine->stats.lock);
    } else if (strncmp(stat_key, "slabs", 5) == 0) {
        slabs_stats(engine, add_stat, cookie);
    } else if (strncmp(stat_key, "items", 5) == 0) {
        item_stats(engine, add_stat, cookie);
    } else if (strncmp(stat_key, "sizes", 5) == 0) {
        item_stats_sizes(engine, add_stat, cookie);
    } else {
        ret = ENGINE_KEY_ENOENT;
    }

    return ret;
}

static ENGINE_ERROR_CODE persistent_store(ENGINE_HANDLE* handle,
                                          const void *cookie,
                                          item* item,
                                          uint64_t *cas,
                                          ENGINE_STORE_OPERATION operation,
                                          uint16_t vbucket) {
    return store_item(get_handle(handle), get_real_item(item), cas, operation,
                      true, cookie);
}

static ENGINE_ERROR_CODE persistent_arithmetic(ENGINE_HANDLE* handle,
                                               const void* cookie,
                                               const void* key,
                                               const int nkey,
                                               const bool increment,
                                               const bool create,
                                               const uint64_t delta,
                                               const uint64_t initial,
                                               const rel_time_t exptime,
                                               uint64_t *cas,
                                               uint64_t *result,
                                               uint16_t vbucket) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;
    struct persistent_engine *engine = get_handle(handle);
    hash_item *item = item_get(engine, key, nkey);

    if (item == NULL) {
        if (!create) {
            return ENGINE_KEY_ENOENT;
        } else {
            char buffer[1023];
            int len = snprintf(buffer, sizeof(buffer), "%llu\r\n",
                               (unsigned long long)initial);

            item = item_alloc(engine, key, nkey, 0, exptime, len, cookie);
            if (item == NULL) {
                return ENGINE_ENOMEM;
            }
            memcpy((void*)item_get_data(item), buffer, len);
            if ((ret = store_item(engine, item, cas, OPERATION_ADD, true,
                                  cookie)) == ENGINE_KEY_EEXISTS) {
                item_release(engine, item);
                return persistent_arithmetic(handle, cookie, key, nkey, increment,
                                             create, delta, initial, exptime, cas,
                                             result, vbucket);
            }

            *result = initial;
            *cas = item_get_cas(item);
            item_release(engine, item);
        }
    } else {
        ret = add_delta(engine, item, increment, delta, cas, result, cookie);
        item_release(engine, item);
    }

    return ret;
}

static ENGINE_ERROR_CODE persistent_flush(ENGINE_HANDLE* handle,
                                          const void* cookie, time_t when) {
    item_flush_expired(get_handle(handle), when);

    return ENGINE_SUCCESS;
}

static void persistent_reset_stats(ENGINE_HANDLE* handle, const void *cookie) {
    struct persistent_engine *engine = get_handle(handle);
    item_stats_reset(engine);

    pthread_mutex_lock(&engine->stats.lock);
    engine->stats.evictions = 0;
    engine->stats.total_items = 0;
    pthread_mutex_unlock(&engine->stats.lock);
}

static ENGINE_ERROR_CODE initalize_configuration(struct persistent_engine *se,
                                                 struct config *config,
                                                 const char *cfg_str) {
    ENGINE_ERROR_CODE ret = ENGINE_SUCCESS;

    if (cfg_str != NULL) {
        struct config_item items[] = {
            { .key = "use_cas",
              .datatype = DT_BOOL,
              .value.dt_bool = &config->use_cas },
            { .key = "verbose",
              .datatype = DT_SIZE,
              .value.dt_size = &config->verbose },
            { .key = "eviction",
              .datatype = DT_BOOL,
              .value.dt_bool = &config->evict_to_free },
            { .key = "cache_size",
              .datatype = DT_SIZE,
              .value.dt_size = &config->maxbytes },
            { .key = "preallocate",
              .datatype = DT_BOOL,
              .value.dt_bool = &config->preallocate },
            { .key = "factor",
              .datatype = DT_FLOAT,
              .value.dt_float = &config->factor },
            { .key = "chunk_size",
              .datatype = DT_SIZE,
              .value.dt_size = &config->chunk_size },
            { .key = "item_size_max",
              .datatype = DT_SIZE,
              .value.dt_size = &config->item_size_max },
            { .key = "warmup",
              .datatype = DT_BOOL,
              .value.dt_bool = &config->warmup },
            { .key = "dbname",
              .datatype = DT_STRING,
              .value.dt_string = &config->dbname },
            { .key = "config_file",
              .datatype = DT_CONFIGFILE },
            { .key = NULL}
        };

        ret = se->server.core->parse_config(cfg_str, items, stderr);
    }

    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE persistent_unknown_command(ENGINE_HANDLE* handle,
                                                    const void* cookie,
                                                    protocol_binary_request_header *request,
                                                    ADD_RESPONSE response)
{
    if (response(NULL, 0, NULL, 0, NULL, 0,
                 PROTOCOL_BINARY_RAW_BYTES,
                 PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND, 0, cookie)) {
        return ENGINE_SUCCESS;
    } else {
        return ENGINE_FAILED;
    }
}


uint64_t item_get_cas(const hash_item* item)
{
    if (item->iflag & ITEM_WITH_CAS) {
        return *(uint64_t*)(item + 1);
    }
    return 0;
}

void item_set_cas(ENGINE_HANDLE* handle, const void *cookie,
                  item* item, uint64_t val)
{
    hash_item *it = get_real_item(item);
    if (it->iflag & ITEM_WITH_CAS) {
        *(uint64_t*)(it + 1) = val;
    }
}

const char* item_get_key(const hash_item* item)
{
    char *ret = (void*)(item + 1);
    if (item->iflag & ITEM_WITH_CAS) {
        ret += sizeof(uint64_t);
    }

    return ret;
}

char* item_get_data(const hash_item* item)
{
    return ((char*)item_get_key(item)) + item->nkey;
}

uint8_t item_get_clsid(const hash_item* item)
{
    return 0;
}

static bool get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                          const item* item, item_info *item_info)
{
    hash_item* it = (hash_item*)item;
    if (item_info->nvalue < 1) {
        return false;
    }
    item_info->cas = item_get_cas(it);
    item_info->exptime = it->exptime;
    item_info->nbytes = it->nbytes;
    item_info->flags = it->flags;
    item_info->clsid = it->slabs_clsid;
    item_info->nkey = it->nkey;
    item_info->nvalue = 1;
    item_info->key = item_get_key(it);
    item_info->value[0].iov_base = item_get_data(it);
    item_info->value[0].iov_len = it->nbytes;
    return true;
}
