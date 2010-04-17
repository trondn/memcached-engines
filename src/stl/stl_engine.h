/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * Definition of a small engine using std::string and std::map
 * for item storage.
 * Please note that the intentions behind this engine is to be an example
 * on how you can create an engine in C++. It will perfom poorly and use
 * a lot of memory!
 *
 * Copy: See COPYING for the status of this software.
 *
 * Author: Trond Norbye
 *
 * @todo The methods in the engine should be more C++y
 */
#ifndef STL_ENGINE_H
#define STL_ENGINE_H

#include "config.h"

#include <string>
#include <map>
#include <memcached/engine.h>
#include <cerrno>

class STLEngine;

/**
 * Holder class for each item
 */
class Item {
public:
    /**
     * Initialize a newly created object.
     * @param theKey The key in the object
     * @param numKey The number of bytes in the key
     * @param numValue The number of bytes in the value
     * @param flgs The user defined flags for the object
     * @param expt The expiry time for the object
     */
    Item(const void* theKey, uint16_t numKey, uint32_t numValue,
         uint32_t flgs, rel_time_t expt) :
        key(static_cast<const char*>(theKey), numKey),
        exptime(static_cast<rel_time_t>(expt)), flags(flgs), value(), cas(0)
    {
        value.resize(numValue);
    }

    /**
     * Initialize a newly created object from another object
     * @param other the other item to "clone"
     */
    Item(const Item &other) : key(other.key), exptime(other.exptime),
                              flags(other.flags), value(other.value), cas(other.cas)
    {
    }

    /**
     * Get the CAS value for this object
     * @return the uniqe ID for the object
     */
    uint64_t getCas() const {
        return cas;
    }

    /**
     * Set the CAS value for this object
     * @param newCas the new CAS id for the object
     */
    void setCas(uint64_t newCas) {
        cas = newCas;
    }

    /**
     * Get the key identifying this item
     * @return key the identifying key
     */
    const char *getKey() const {
        return key.data();
    }

    /**
     * Get the value in this object
     * @return a pointer to the value
     */
    char *getValue() {
        return const_cast<char*>(value.data());
    }

    /**
     * Clone this object
     * @return a copy of the object
     */
    Item *clone() {
        Item *it = new Item(*this);
        return it;
    }

    /**
     * Append my value to the content of another item
     * @param other the other object to append my data to
     */
    void append(Item *other) {
        std::string val = other->value;
        val.resize(val.length() - 2);
        val.append(value);
        value = val;
    }

    /**
     * Append the value of another item to my own value
     * @param other the other object to get the data from
     */
    void prepend(Item *other) {
        value.resize(value.length() - 2);
        value.append(other->value);
    }

private:
    /**
     * We want to let the engine access our internal data without having
     * to call all of the get/set methods
     */
    friend class STLEngine;
    /** The key identifying the object */
    std::string key;
    rel_time_t exptime; /**< When the item will expire (relative to process
                         * startup) */
    uint32_t flags; /**< Flags associated with the item (in network byte order)*/
    /** The items value */
    std::string value;
    /** The uniqe id for the item */
    uint64_t cas;
};

/**
 * Implementation of the engine interface
 */
class STLEngine : public engine_interface_v1 {
public:
    /**
     * Initialize the engine
     * @param api pointer to the server-provided API
     */
    STLEngine(SERVER_HANDLE_V1 *api);

    /**
     * Get the version information from the engine
     * @return a string containing the version information
     */
    const std::string Version() const;

    /**
     * Initialize the engine
     * @param config a textual string containing the configuration
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE Initialize(const char* config);

    /**
     * Allocate and initialize a new item
     * @param cookie not used
     * @param item where to store the result
     * @param key the key to indentify the item
     * @param nkey the number of bytes in the key
     * @param nbytes the number of bytes in the item
     * @param flags the user-specified flags for the item
     * @param exptime when should the object expire
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE Allocate(const void* cookie,
                               item **item,
                               const void* key,
                               const size_t nkey,
                               const size_t nbytes,
                               const int flags,
                               const rel_time_t exptime);

    /**
     * Remove (aka delete) an object from the cache
     * @param cookie not used
     * @param key the key for the item to remove
     * @param nkey length of key
     * @param cas cas id to remove
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE Remove(const void* cookie, const void *key, const size_t nkey,
                             uint64_t cas);

    /**
     * Release an object (the frontend doesn't need it anymore)
     * @param cookie not used
     * @param item the item to release
     */
    void Release(const void *cookie, item* item);

    /**
     * Get an object from the cache identified by a key
     * @param cookie not used
     * @param item where to store the result
     * @param key the key to look up
     * @param nkey the number of bytes in the key
     * @return ENGINE_SUCCESS if the item was found in the cache
     */
    ENGINE_ERROR_CODE Get(const void* cookie, item** item, const void* key,
                          const int nkey);

    /**
     * Store an oject in the cache
     * @param cookie not used
     * @param item the item to store in the cache
     * @param cas where to store the new CAS id for the object
     * @param operation what kind of store operation this is
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE Store(const void *cookie,
                            item* item,
                            uint64_t *cas,
                            ENGINE_STORE_OPERATION operation);

    /**
     * Perform an arithmetic operation on an item.
     * This operation is currently not implemented
     *
     * @param cookie not used
     * @param key not used
     * @param nkey not used
     * @param increment not used
     * @param create not used
     * @param delta not used
     * @param initial not used
     * @param exptime not used
     * @param cas not used
     * @param result not used
     * @return ENGINE_NOTSUP always
     */
    ENGINE_ERROR_CODE Arithmetic(const void* cookie,
                                 const void* key,
                                 const int nkey,
                                 const bool increment,
                                 const bool create,
                                 const uint64_t delta,
                                 const uint64_t initial,
                                 const rel_time_t exptime,
                                 uint64_t *cas,
                                 uint64_t *result);

    /**
     * Remove all items from the cache
     * @param cookie not used
     * @param when optional time when we should flush the cache
     * @return ENGINE_SUCCESS on success
     */
    ENGINE_ERROR_CODE Flush(const void* cookie, time_t when = 0);

    /**
     * Get statistics from the engine
     * This operation is currently not implemented
     * @param cookie not used
     * @param stat_key not used
     * @param nkey not used
     * @param add_stat not used
     * @return ENGINE_SUCCESS always
     */
    ENGINE_ERROR_CODE GetStats(const void* cookie, const char* stat_key,
                               int nkey, ADD_STAT add_stat);
    /**
     * Reset statistics
     * @param cookie not used
     */
    void ResetStats(const void *cookie);

    bool getItemInfo(const Item* it, item_info *item_info)
    {
        if (item_info->nvalue < 1) {
            return false;
        }
        item_info->cas = it->cas;
        item_info->exptime = it->exptime;
        item_info->nbytes = it->value.size();
        item_info->flags = it->flags;
        item_info->clsid = 0;
        item_info->nkey = it->key.size();
        item_info->nvalue = 1;
        item_info->key = it->key.c_str();
        item_info->value[0].iov_base = const_cast<char*>(it->value.c_str());
        item_info->value[0].iov_len = it->value.length();
        return true;

    }

private:
    /** Handle to the server API */
    SERVER_HANDLE_V1 *server;
    /** The item cache */
    std::map<std::string, Item*> cache;
};

#endif
