/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Implementation of a small engine using std::string and std::map
 * for item storage.
 *
 * Author: Trond Norbye
 */
#include "stl_engine.h"

#include <pthread.h>

/**
 * This is the _only_ function exported from the library. Create a new instance
 * of the stl engine, and return a handler to it.
 *
 * @param interface the highest supported server interface
 * @param get_server_api callback function to get the API exported by the server
 * @param handle pointer to a memory location we should return the engine handle
 *               in.
 * @return ENGINE_NOTSUP if we don't support the interface from the server, or
 *         ENGINE_SUCCESS if we initialized everything successful.
 *
 */
extern "C" {
    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle) {
        SERVER_HANDLE_V1 *api;

        if (interface != 1 ||
            (api = static_cast<SERVER_HANDLE_V1 *>(get_server_api())) == NULL) {
            return ENGINE_ENOTSUP;
        }

        STLEngine *engine = new STLEngine(api);
        *handle = reinterpret_cast<ENGINE_HANDLE*>(engine);
        return ENGINE_SUCCESS;
    }
}

/*
 * The Engine API use "C-linkage" so I need to put all the "wrapper-functions"
 * in an extern "C" block. Originally I wanted to put inside the class as
 * static member functions, but that made the C++ compiler spit out warnings :(
 */
extern "C" {
    static const engine_info *stl_get_info(ENGINE_HANDLE* handle)
    {
        static engine_info info;
        info.description = (reinterpret_cast<STLEngine*>(handle))->Version().c_str();
        return &info;
    }

    static ENGINE_ERROR_CODE stl_initialize(ENGINE_HANDLE* handle, const char* config)
    {
        return (reinterpret_cast<STLEngine*>(handle))->Initialize(config);
    }

    static void stl_destroy(ENGINE_HANDLE* handle)
    {
        delete reinterpret_cast<STLEngine*>(handle);
    }

    static ENGINE_ERROR_CODE stl_allocate(ENGINE_HANDLE* handle,
                                          const void* cookie,
                                          item **item,
                                          const void* key,
                                          const size_t nkey,
                                          const size_t nbytes,
                                          const int flags,
                                          const rel_time_t exptime)
    {
        return reinterpret_cast<STLEngine*>(handle)->Allocate(cookie,
                                                              item,
                                                              key,
                                                              nkey,
                                                              nbytes,
                                                              flags,
                                                              exptime);
    }


    static ENGINE_ERROR_CODE stl_remove(ENGINE_HANDLE* handle,
                                        const void* cookie,
                                        const void* key,
                                        const size_t nkey,
                                        uint64_t cas,
                                        uint16_t vbucket)
    {
        return reinterpret_cast<STLEngine*>(handle)->Remove(cookie, key, nkey, cas);
    }

    static void stl_release(ENGINE_HANDLE* handle, const void *cookie,
                            item* item)
    {
        return reinterpret_cast<STLEngine*>(handle)->Release(cookie, item);
    }

    static ENGINE_ERROR_CODE stl_get(ENGINE_HANDLE* handle,
                                     const void* cookie,
                                     item** item,
                                     const void* key,
                                     const int nkey,
                                     uint16_t vbucket)
    {
        return reinterpret_cast<STLEngine*>(handle)->Get(cookie, item, key, nkey);
    }

    static ENGINE_ERROR_CODE stl_store(ENGINE_HANDLE* handle,
                                       const void *cookie,
                                       item* item,
                                       uint64_t *cas,
                                       ENGINE_STORE_OPERATION operation,
                                       uint16_t vbucket)
    {
        return reinterpret_cast<STLEngine*>(handle)->Store(cookie, item, cas, operation);
    }

    static ENGINE_ERROR_CODE stl_arithmetic(ENGINE_HANDLE* handle,
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
                                            uint16_t vbucket)
    {
        return reinterpret_cast<STLEngine*>(handle)->Arithmetic(cookie, key, nkey, increment, create, delta, initial, exptime, cas, result);
    }

    static ENGINE_ERROR_CODE stl_flush(ENGINE_HANDLE* handle,
                                       const void* cookie, time_t when)
    {
        return reinterpret_cast<STLEngine*>(handle)->Flush(cookie, when);
    }

    static ENGINE_ERROR_CODE stl_get_stats(ENGINE_HANDLE* handle,
                                           const void* cookie,
                                           const char* stat_key,
                                           int nkey,
                                           ADD_STAT add_stat)
    {
        return reinterpret_cast<STLEngine*>(handle)->GetStats(cookie, stat_key, nkey, add_stat);
    }

    static void stl_reset_stats(ENGINE_HANDLE* handle, const void *cookie)
    {
        return reinterpret_cast<STLEngine*>(handle)->ResetStats(cookie);
    }

    static bool stl_get_item_info(ENGINE_HANDLE *handle, const void *cookie,
                                  const item* item, item_info *item_info)
    {
        return reinterpret_cast<STLEngine*>(handle)->getItemInfo(reinterpret_cast<const Item*>(item),
                                                                 item_info);
    }

    static void stl_item_set_cas(ENGINE_HANDLE *handle, const void *cookie,
                                 item *item, uint64_t cas)
    {
        (void)handle;
        reinterpret_cast<Item*>(item)->setCas(cas);
    }
}

class CacheLock {
public:
    CacheLock() {
        int ret;
        while ((ret = pthread_mutex_lock(&mutex)) == -1) {
            if (errno != EINTR) {
                abort();
            }
        }
    }

    ~CacheLock() {
        int ret;
        while ((ret = pthread_mutex_unlock(&mutex)) == -1) {
            if (errno != EINTR) {
                abort();
            }
        }
    }

private:
    static pthread_mutex_t mutex;

};

pthread_mutex_t CacheLock::mutex = PTHREAD_MUTEX_INITIALIZER;


/*
 * Implementation of the engine :-)
 */

STLEngine::STLEngine(SERVER_HANDLE_V1 *api) :
    server(api), cache()
{
    interface.interface = 1;
    get_info = stl_get_info;
    initialize = stl_initialize;
    destroy = stl_destroy;
    allocate = stl_allocate;
    remove= stl_remove;
    release = stl_release;
    get = stl_get;
    store = stl_store;
    arithmetic= stl_arithmetic;
    flush = stl_flush;
    get_stats= stl_get_stats;
    reset_stats = stl_reset_stats;
    item_set_cas = stl_item_set_cas;
    get_item_info = stl_get_item_info;
}

const std::string STLEngine::Version() const
{
    return "Stl example engine v0.1";
}

ENGINE_ERROR_CODE STLEngine::Initialize(const char* config)
{
    (void)config;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE STLEngine::Allocate(const void* cookie,
                                      item **itm,
                                      const void* key,
                                      const size_t nkey,
                                      const size_t nbytes,
                                      const int flags,
                                      const rel_time_t exptime)
{
    (void)cookie;
    *itm = reinterpret_cast<item*>(new Item(key, nkey, nbytes, flags, exptime));
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE STLEngine::Remove(const void* cookie, const void *key,
                                    const size_t nkey,
                                    uint64_t cas)
{
    (void)cookie;
    CacheLock lock;
    std::string it(static_cast<const char*>(key), nkey);
    std::map<std::string, Item*>::iterator iter = cache.find(it);
    if (iter != cache.end()) {
        if (cas == iter->second->cas) {
            delete iter->second;
            cache.erase(iter);
            return ENGINE_SUCCESS;
        }
        return ENGINE_KEY_EEXISTS;
    } else {
        return ENGINE_KEY_ENOENT;
    }
}

void STLEngine::Release(const void *cookie, item* item)
{
    (void)cookie;
    Item *it = reinterpret_cast<Item*>(item);
    delete it;
}

ENGINE_ERROR_CODE STLEngine::Get(const void* cookie, item** pIt,
                                 const void* key,
                                 const int nkey)
{
    (void)cookie;
    std::string k(static_cast<const char*>(key), nkey);
    CacheLock lock;

    std::map<std::string, Item*>::iterator it = cache.find(k);
    if (it != cache.end()) {
        *pIt = it->second->clone();
        return ENGINE_SUCCESS;
    } else {
        *pIt = NULL;
        return ENGINE_KEY_ENOENT;
    }
}

ENGINE_ERROR_CODE STLEngine::Store(const void *cookie,
                                   item* item,
                                   uint64_t *cas,
                                   ENGINE_STORE_OPERATION operation)
{
    (void)cookie;
    CacheLock lock;
    Item *it = reinterpret_cast<Item*>(item);

    std::map<std::string, Item*>::iterator iter = cache.find(it->key);

    if (iter == cache.end()) {
        switch (operation) {
        case OPERATION_REPLACE:
        case OPERATION_APPEND:
        case OPERATION_PREPEND:
            return ENGINE_KEY_ENOENT;
        default:
            cache[it->key] = it->clone();
            return ENGINE_SUCCESS;
        }
    }

    if (operation == OPERATION_ADD) {
        return ENGINE_NOT_STORED;
    }

    if (it->cas != 0 && (it->cas != iter->second->cas)) {
        return ENGINE_KEY_EEXISTS;
    }

    if (operation == OPERATION_APPEND) {
        it->append(iter->second);
    } else if (operation == OPERATION_PREPEND) {
        it->prepend(iter->second);
    }

    delete iter->second;
    cache.erase(iter);
    cache[it->key] = it->clone();
    *cas = it->cas;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE STLEngine::Arithmetic(const void* cookie,
                                        const void* key,
                                        const int nkey,
                                        const bool increment,
                                        const bool create,
                                        const uint64_t delta,
                                        const uint64_t initial,
                                        const rel_time_t exptime,
                                        uint64_t *cas,
                                        uint64_t *result)
{
    (void)cookie; (void)key; (void)nkey; (void)increment; (void)create;
    (void)delta; (void)initial; (void)exptime; (void)cas; (void)result;
    return ENGINE_ENOTSUP;
}

ENGINE_ERROR_CODE STLEngine::Flush(const void* cookie, time_t when)
{
    (void)cookie;
    if (when != 0) {
        return ENGINE_ENOTSUP;
    }

    CacheLock lock;
    cache.clear();

    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE STLEngine::GetStats(const void* cookie,
                                      const char* stat_key,
                                      int nkey,
                                      ADD_STAT add_stat)
{
    // We don't have any stats ;-)
    (void)cookie; (void)stat_key; (void)nkey; (void)add_stat;
    return ENGINE_SUCCESS;
}

void STLEngine::ResetStats(const void *cookie)
{
    (void)cookie;
}
