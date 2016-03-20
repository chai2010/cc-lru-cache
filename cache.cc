// Copyright 2016 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "cache.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <mutex>

// The LRU_CACHE_FALLTHROUGH_INTENDED macro can be used to annotate implicit fall-through
// between switch labels. The real definition should be provided externally.
// This one is a fallback version for unsupported compilers.
#ifndef LRU_CACHE_FALLTHROUGH_INTENDED
#define LRU_CACHE_FALLTHROUGH_INTENDED do { } while (0)
#endif

namespace {

struct MutexLock {
    std::mutex* l;
    MutexLock(std::mutex *l): l(l) { l->lock(); }
    ~MutexLock() { l->unlock(); }
};

uint32_t Hash(const char* data, size_t n, uint32_t seed) {
    // Similar to murmur hash
    const uint32_t m = 0xc6a4a793;
    const uint32_t r = 24;
    const char* limit = data + n;
    uint32_t h = seed ^ (n * m);
    
    // Pick up four bytes at a time
    while (data + 4 <= limit) {
        uint32_t w = 0;
        memcpy(&w, data, sizeof(uint32_t)); // DecodeFixed32
        
        data += 4;
        h += w;
        h *= m;
        h ^= (h >> 16);
    }

    // Pick up remaining bytes
    switch (limit - data) {
    case 3:
        h += data[2] << 16;
        LRU_CACHE_FALLTHROUGH_INTENDED;
    case 2:
        h += data[1] << 8;
        LRU_CACHE_FALLTHROUGH_INTENDED;
    case 1:
        h += data[0];
        h *= m;
        h ^= (h >> r);
        break;
    }
    return h;
}

// LRU cache implementation

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
struct LRUHandle {
    void* value;
    void (*deleter)(const char*, void* value);
    LRUHandle* next_hash;
    LRUHandle* next;
    LRUHandle* prev;
    size_t charge;      // TODO(opt): Only allow uint32_t?
    size_t key_length;
    uint32_t refs;
    uint32_t hash;      // Hash of key(); used for fast sharding and comparisons
    char key_data[1];   // Beginning of key
    
    const char* key() const {
        return &key_data[0];
    }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
public:
    HandleTable() : length_(0), elems_(0), list_(NULL) { Resize(); }
    ~HandleTable() { delete[] list_; }
    
    LRUHandle* Lookup(const char* key, uint32_t hash) {
        return *FindPointer(key, hash);
    }
    
    LRUHandle* Insert(LRUHandle* h) {
        LRUHandle** ptr = FindPointer(h->key(), h->hash);
        LRUHandle* old = *ptr;
        h->next_hash = (old == NULL ? NULL : old->next_hash);
        *ptr = h;
        if (old == NULL) {
            ++elems_;
            if (elems_ > length_) {
                // Since each cache entry is fairly large, we aim for a small
                // average linked list length (<= 1).
                Resize();
            }
        }
        return old;
    }

    LRUHandle* Remove(const char* key, uint32_t hash) {
        LRUHandle** ptr = FindPointer(key, hash);
        LRUHandle* result = *ptr;
        if (result != NULL) {
            *ptr = result->next_hash;
            --elems_;
        }
        return result;
    }
    
private:
    // The table consists of an array of buckets where each bucket is
    // a linked list of cache entries that hash into the bucket.
    uint32_t length_;
    uint32_t elems_;
    LRUHandle** list_;

    // Return a pointer to slot that points to a cache entry that
    // matches key/hash.  If there is no such cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle** FindPointer(const char* key, uint32_t hash) {
        LRUHandle** ptr = &list_[hash & (length_ - 1)];
        while (*ptr != NULL && ((*ptr)->hash != hash || strcmp(key, (*ptr)->key()) != 0)) {
            ptr = &(*ptr)->next_hash;
        }
        return ptr;
    }
    
    void Resize() {
        uint32_t new_length = 4;
        while (new_length < elems_) {
            new_length *= 2;
        }
        LRUHandle** new_list = new LRUHandle*[new_length];
        memset(new_list, 0, sizeof(new_list[0]) * new_length);
        uint32_t count = 0;
        for (uint32_t i = 0; i < length_; i++) {
            LRUHandle* h = list_[i];
            while (h != NULL) {
                LRUHandle* next = h->next_hash;
                uint32_t hash = h->hash;
                LRUHandle** ptr = &new_list[hash & (new_length - 1)];
                h->next_hash = *ptr;
                *ptr = h;
                h = next;
                count++;
            }
        }
        assert(elems_ == count);
        delete[] list_;
        list_ = new_list;
        length_ = new_length;
    }
};

// A single shard of sharded cache.
class LRUCacheImpl {
public:
    LRUCacheImpl();
    ~LRUCacheImpl();
    
    // Separate from constructor so caller can easily make an array of LRUCache
    void SetCapacity(size_t capacity) { capacity_ = capacity; }
    
    // Like Cache methods, but with an extra "hash" parameter.
    LRUCache::Handle* Insert(
        const char* key, uint32_t hash, void* value, size_t charge,
        void (*deleter)(const char* key, void* value)
    );
    LRUCache::Handle* Lookup(const char* key, uint32_t hash);
    void Release(LRUCache::Handle* handle);
    void Erase(const char* key, uint32_t hash);
    
private:
    void LRU_Remove(LRUHandle* e);
    void LRU_Append(LRUHandle* e);
    void Unref(LRUHandle* e);
    
    // Initialized before use.
    size_t capacity_;
    
    // mutex_ protects the following state.
    std::mutex mutex_;
    size_t usage_;
    
    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    LRUHandle lru_;
    
    HandleTable table_;
};

LRUCacheImpl::LRUCacheImpl(): usage_(0) {
    // Make empty circular linked list
    lru_.next = &lru_;
    lru_.prev = &lru_;
}

LRUCacheImpl::~LRUCacheImpl() {
    for (LRUHandle* e = lru_.next; e != &lru_; ) {
        LRUHandle* next = e->next;
        assert(e->refs == 1);  // Error if caller has an unreleased handle
        Unref(e);
        e = next;
    }
}

void LRUCacheImpl::Unref(LRUHandle* e) {
    assert(e->refs > 0);
    e->refs--;
    if (e->refs <= 0) {
        usage_ -= e->charge;
        (*e->deleter)(e->key(), e->value);
        free(e);
    }
}

void LRUCacheImpl::LRU_Remove(LRUHandle* e) {
    e->next->prev = e->prev;
    e->prev->next = e->next;
}

void LRUCacheImpl::LRU_Append(LRUHandle* e) {
    // Make "e" newest entry by inserting just before lru_
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
}

LRUCache::Handle* LRUCacheImpl::Lookup(const char* key, uint32_t hash) {
    MutexLock l(&mutex_);
    LRUHandle* e = table_.Lookup(key, hash);
    if (e != NULL) {
        e->refs++;
        LRU_Remove(e);
        LRU_Append(e);
    }
    return reinterpret_cast<LRUCache::Handle*>(e);
}

void LRUCacheImpl::Release(LRUCache::Handle* handle) {
    MutexLock l(&mutex_);
    Unref(reinterpret_cast<LRUHandle*>(handle));
}

LRUCache::Handle* LRUCacheImpl::Insert(
    const char* key, uint32_t hash, void* value, size_t charge,
    void (*deleter)(const char* key, void* value)
) {
    MutexLock l(&mutex_);
    
    int keyLen = strlen(key);
    LRUHandle* e = reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) + keyLen));
    e->value = value;
    e->deleter = deleter;
    e->charge = charge;
    e->key_length = keyLen;
    e->hash = hash;
    e->refs = 2;  // One from LRUCache, one for the returned handle
    strcpy(e->key_data, key);
    LRU_Append(e);
    usage_ += charge;
    
    LRUHandle* old = table_.Insert(e);
    if (old != NULL) {
        LRU_Remove(old);
        Unref(old);
    }
    
    while (usage_ > capacity_ && lru_.next != &lru_) {
        LRUHandle* old = lru_.next;
        LRU_Remove(old);
        table_.Remove(old->key(), old->hash);
        Unref(old);
    }
    
    return reinterpret_cast<LRUCache::Handle*>(e);
}

void LRUCacheImpl::Erase(const char* key, uint32_t hash) {
    MutexLock l(&mutex_);
    LRUHandle* e = table_.Remove(key, hash);
    if (e != NULL) {
        LRU_Remove(e);
        Unref(e);
    }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;

class ShardedLRUCache: public LRUCache {
private:
    LRUCacheImpl shard_[kNumShards];
    std::mutex id_mutex_;
    uint64_t last_id_;
    
    static inline uint32_t HashSlice(const char* s) {        
        return Hash(s, strlen(s), 0);
    }
    
    static uint32_t Shard(uint32_t hash) {
        return hash >> (32 - kNumShardBits);
    }
    
public:
    explicit ShardedLRUCache(size_t capacity): last_id_(0) {
        const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
        for (int s = 0; s < kNumShards; s++) {
            shard_[s].SetCapacity(per_shard);
        }
    }
    virtual ~ShardedLRUCache() { }
    
    virtual void Delete() {
        delete this;
    }
    
    virtual Handle* Insert(
        const char* key, void* value, size_t charge,
        void (*deleter)(const char* key, void* value)
    ) {
        const uint32_t hash = HashSlice(key);
        return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
    }
    virtual Handle* Lookup(const char* key) {
        const uint32_t hash = HashSlice(key);
        return shard_[Shard(hash)].Lookup(key, hash);
    }
    virtual void Release(Handle* handle) {
        LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
        shard_[Shard(h->hash)].Release(handle);
    }
    virtual void Erase(const char* key) {
        const uint32_t hash = HashSlice(key);
        shard_[Shard(hash)].Erase(key, hash);
    }
    virtual void* Value(Handle* handle) {
        return reinterpret_cast<LRUHandle*>(handle)->value;
    }
    virtual uint64_t NewId() {
        MutexLock l(&id_mutex_);
        return ++(last_id_);
    }
};

}  // end anonymous namespace

LRUCache* LRUCache::New(size_t capacity) {
    return new ShardedLRUCache(capacity);
}
