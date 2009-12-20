/*
 * Copyright (C) 2009 Trond Norbye (trond.norbye@gmail.com)
 */
#ifndef SQLITE_IO_H
#define SQLITE_IO_H

#ifdef __cplusplus
extern "C" {
#endif

   void sqlite_io_store_item(struct persistent_engine* engine, hash_item* item);
   void sqlite_io_get_item(struct persistent_engine* engine, const void* cookie, const void *key, uint16_t keylen);

   ENGINE_ERROR_CODE sqlite_io_start_threads(struct persistent_engine *engine);

#ifdef __cplusplus
}
#endif

#endif
