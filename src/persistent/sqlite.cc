/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 * This file contains implementation of the persistence layer. Please note
 * that it is not optimized for speed, but rather to create a readable example.
 *
 * Copyright (C) 2009 Trond Norbye (trond.norbye@gmail.com)
 *
 */
#include "persistent_engine.h"

#include <assert.h>
#include <sqlite3.h>
#include <cerrno>
#include <cstdlib>
#include <string>
#include <map>

class SQLite {
public:
    SQLite(struct persistent_engine* se)
        : db(NULL), engine(se)
    {
        pthread_mutex_init(&mutex, NULL);
        pthread_cond_init(&cond, NULL);
    }

    virtual ~SQLite() {
        pthread_mutex_destroy(&mutex);
        pthread_cond_destroy(&cond);
    }

    bool initialize(const std::string &dbname) {
        sqlite3_stmt *st;
        if (sqlite3_open(dbname.c_str(), &db) !=  SQLITE_OK) {
            db = NULL;
            return false;
        }

        std::string query = "CREATE TABLE IF NOT EXISTS kv"
            " (key VARCHAR(250) PRIMARY KEY,"
            "  flags INTEGER(4), "
            "  exptime INTEGER(4), "
            "  hash INTEGER(4), "
            "  value BLOB)";

        if (sqlite3_prepare_v2(db, query.c_str(),
                               query.length(), &st, NULL) != SQLITE_OK) {
            return false;
        }

        int rc = 0;
        while ((rc = sqlite3_step(st)) != SQLITE_DONE) {
            ;
        }
        sqlite3_changes(db);
        sqlite3_finalize(st);

        return true;
    }

    void finalize() {
        if (db != NULL) {
            (void)sqlite3_close(db);
        }
    }

    static void run(SQLite *thread) {
        thread->run();
    }

protected:
    virtual void run(void) = 0;
    sqlite3 *db;
    struct persistent_engine* engine;


    void lock() {
        int ret;
        while ((ret = pthread_mutex_lock(&mutex)) == -1) {
            if (errno != EINTR) {
                abort();
            }
        }
    }

    void unlock() {
        int ret;
        while ((ret = pthread_mutex_unlock(&mutex)) == -1) {
            if (errno != EINTR) {
                abort();
            }
        }
    }

    void notify() {
        int ret;
        while ((ret = pthread_cond_signal(&cond)) == -1) {
            if (errno != EINTR) {
                abort();
            }
        }
    }

    void wait() {
        int ret;
        while ((ret = pthread_cond_wait(&cond, &mutex)) == -1) {
            if (errno != EINTR) {
                abort();
            }
        }
    }

    pthread_cond_t cond;
    pthread_mutex_t mutex;
};


class SQLiteWriter : public SQLite {
public:
    SQLiteWriter(struct persistent_engine* se)
        : SQLite(se) {
    }

    ~SQLiteWriter() {

    }

    bool initialize(const std::string &dbname) {
        if (!SQLite::initialize(dbname)) {
            return false;
        }

        std::string query= "INSERT OR REPLACE INTO kv "
            "(key, flags, exptime, hash, value) "
            "values (?, ?, ?, ?, ?)";
        if (sqlite3_prepare_v2(db, query.c_str(),
                               query.length(),
                               &statement, NULL) != SQLITE_OK) {
            return false;
        }

        return true;
    }

    void finalize() {
        sqlite3_finalize(statement);
        SQLite::finalize();
    }

    void enqueue(hash_item* item) {
        std::string key(item_get_key(&item->itm), item->itm.nkey);
        ++item->refcount;

        lock();
        std::map<std::string, hash_item*>::iterator iter = queue.find(key);
        if (iter != queue.end()) {
            /* don't store the previous entry! */
            engine->engine.release(reinterpret_cast<ENGINE_HANDLE*>(&engine->engine),
                                   NULL, &iter->second->itm);
            queue.erase(iter);
        }
        queue[key] = item;
        notify();
        unlock();
    }

private:
    void storeItem(hash_item *it)
    {
        if (sqlite3_clear_bindings(statement) != SQLITE_OK ||
            sqlite3_reset(statement) != SQLITE_OK) {
            abort();
        }

        sqlite3_bind_text(statement, 1,item_get_key(&it->itm),
                          it->itm.nkey, SQLITE_STATIC);
        sqlite3_bind_int(statement, 2, it->itm.flags);
        sqlite3_bind_int(statement, 3, it->itm.exptime);
        sqlite3_bind_int(statement, 4, 0);
        sqlite3_bind_blob(statement, 5, item_get_data(&it->itm),
                          it->itm.nbytes, SQLITE_STATIC);

        int rc = 0;
        bool retry;
        bool success = true;

        do {
            retry = false;
            switch ((rc = sqlite3_step(statement))) {
                /* @todo fix the correct values here */
            case SQLITE_CONSTRAINT:
                success = false;
                break;
            case SQLITE_DONE:
                break;
            default:
                retry = true;
            }
        } while (retry);

        if (success) {
            success = sqlite3_changes(db) == 1;;
        }
    }

    virtual void run() {
        assert(engine != NULL);
        lock();
        while (true) {
            while (queue.empty()) {
                wait();
            }

            while (queue.begin() != queue.end()) {
                hash_item *item = queue.begin()->second;
                queue.erase(queue.begin());
                unlock();
                storeItem(item);
                engine->engine.release(reinterpret_cast<ENGINE_HANDLE*>(&engine->engine),
                                       NULL, &item->itm);
                lock();
            }
        }
    }

    sqlite3_stmt *statement;
    std::map<std::string, hash_item*> queue;
};


static inline hash_item* get_real_item(item* itm) {
    hash_item it;
    ptrdiff_t offset = (caddr_t)&it.itm - (caddr_t)&it;
    return (hash_item*) (((caddr_t) itm) - (offset));
}

class SQLiteReader : public SQLite {
public:
    SQLiteReader(struct persistent_engine* se)
        : SQLite(se) {
    }

    ~SQLiteReader() {

    }

    bool initialize(const std::string &dbname) {
        if (!SQLite::initialize(dbname)) {
            return false;
        }

        std::string query = "SELECT flags, exptime, hash, value FROM kv "
            "where key = ?";
        if (sqlite3_prepare_v2(db, query.c_str(),
                               query.length(),
                               &statement, NULL) != SQLITE_OK) {
            return false;
        }

        return true;
    }

    void finalize() {
        sqlite3_finalize(statement);
        SQLite::finalize();
    }

    void enqueue(const void *cookie, const std::string &key) {
        lock();
        queue[cookie] = key;
        notify();
        unlock();
    }

protected:

    bool createItem(std::string key, int flagoffset, const void *cookie) {
        item *it = NULL;
        ENGINE_ERROR_CODE r;
        size_t nbytes = sqlite3_column_bytes(statement, flagoffset + 2);
        r = engine->engine.allocate(reinterpret_cast<ENGINE_HANDLE*>(&engine->engine),
                                    cookie, &it, key.c_str(), key.length(),
                                    nbytes,
                                    sqlite3_column_int(statement, flagoffset),
                                    sqlite3_column_int(statement, flagoffset + 1));
        if (r == ENGINE_SUCCESS) {
            hash_item *itm = get_real_item(it);
            memcpy(item_get_data(&itm->itm),
                   sqlite3_column_text(statement, flagoffset + 2), nbytes);
            uint64_t cas;
            store_item(engine, itm, &cas, OPERATION_ADD, false);
            return true;

        }
        return false;
    }

    bool readItem(const std::string &key, const void *cookie)
    {
        if (sqlite3_clear_bindings(statement) != SQLITE_OK ||
            sqlite3_reset(statement) != SQLITE_OK) {
            abort();
        }

        sqlite3_bind_text(statement, 1, key.c_str(),
                          key.length(), SQLITE_STATIC);

        int rc = 0;
        bool retry;
        bool found = false;
        do {
            retry = false;
            switch ((rc = sqlite3_step(statement))) {
            case SQLITE_ROW:
                found = true;
                break;
            case SQLITE_DONE:
                break;
            }
        } while (retry);

        if (found) {
            return createItem(key, 1, cookie);
        }
        return false;
    }

    virtual void run() {
        assert(engine != NULL);
        lock();
        while (true) {
            while (queue.empty()) {
                wait();
            }

            while (queue.begin() != queue.end()) {
                const void *cookie = queue.begin()->first;
                std::string key = queue.begin()->second;
                queue.erase(queue.begin());
                unlock();
                bool success = readItem(key, cookie);
                engine->server.notify_io_complete(cookie,
                                                  success ? ENGINE_SUCCESS : ENGINE_KEY_ENOENT);
                lock();
            }
        }
    }

    sqlite3_stmt *statement;

    std::map<const void *, std::string> queue;
};

class SQLiteCacheWarmup : public SQLiteReader {
public:
    SQLiteCacheWarmup(struct persistent_engine* se)
        : SQLiteReader(se) {
    }

    ~SQLiteCacheWarmup() {

    }

    bool initialize(const std::string &dbname) {
        if (!SQLite::initialize(dbname)) {
            return false;
        }

        std::string query = "SELECT key, flags, exptime, hash, value FROM kv";
        if (sqlite3_prepare_v2(db, query.c_str(),
                               query.length(),
                               &statement, NULL) != SQLITE_OK) {
            return false;
        }

        return true;
    }

    void finalize() {
        sqlite3_finalize(statement);
        SQLite::finalize();
    }

private:

    virtual void run() {
        int rc = 0;
        bool done = false;
        do {
            switch ((rc = sqlite3_step(statement))) {
            case SQLITE_ROW:
                {
                    std::string key((char*)sqlite3_column_text(statement, 0),
                                    sqlite3_column_bytes(statement, 0));
                    createItem(key, 2, NULL);
                }
                break;
            case SQLITE_DONE:
                done = true;
                break;
            default:
                fprintf(stderr, "Got error: %d\n", rc);
                done = true;
                break;
            }
        } while (!done);

    }
};

extern "C" static void *thread_entry(void *arg) {
    SQLite::run(static_cast<SQLite *>(arg));
    return NULL;
}

ENGINE_ERROR_CODE sqlite_io_start_threads(struct persistent_engine *engine)
{
    SQLiteReader *reader = new SQLiteReader(engine);
    SQLiteWriter *writer = new SQLiteWriter(engine);

    if (!reader->initialize(engine->config.dbname) ||
        !writer->initialize(engine->config.dbname)) {
        return ENGINE_FAILED;
    }

    engine->reader = static_cast<void*>(reader);
    engine->writer = static_cast<void*>(writer);

    pthread_t tid;
    int ret;
    if ((ret = pthread_create(&tid, NULL, thread_entry, reader)) != 0 ||
        (ret = pthread_create(&tid, NULL, thread_entry, writer)) != 0) {
        return ENGINE_FAILED;
    }

    if (engine->config.warmup) {
        SQLiteCacheWarmup *warmup = new SQLiteCacheWarmup(engine);
        warmup->initialize(engine->config.dbname);
        pthread_create(&tid, NULL, thread_entry, warmup);
    }

    return ENGINE_SUCCESS;
}

void sqlite_io_get_item(struct persistent_engine* engine,
                        const void* cookie,
                        const void *key,
                        uint16_t keylen)
{
    std::string k(reinterpret_cast<const char*>(key), keylen);
    (reinterpret_cast<SQLiteReader*>(engine->reader))->enqueue(cookie, k);
}

void sqlite_io_store_item(struct persistent_engine* engine, hash_item* item) {
    (reinterpret_cast<SQLiteWriter*>(engine->writer))->enqueue(item);
}
