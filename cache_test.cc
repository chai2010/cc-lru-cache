// Copyright 2016 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

#include "test.h"
#include "cache.h"

#include <assert.h>

#include <string>
#include <vector>

// Conversions between numeric keys/values and the types expected by Cache.
static std::string EncodeKey(int k) {
    char buf[16] = { 0 };
    sprintf(buf, "%d", k);
    return std::string((const char*)buf, strlen(buf));
}
static int DecodeKey(const std::string& k) {
    int value;
    sscanf(k.c_str(), "%d", &value);
    return value;
}
static void* EncodeValue(uintptr_t v) { return reinterpret_cast<void*>(v); }
static int DecodeValue(void* v) { return reinterpret_cast<uintptr_t>(v); }

class LRUCacheTest {
public:
    static LRUCacheTest* current_;

    static void Deleter(const char* key, void* v) {
        current_->deleted_keys_.push_back(DecodeKey(key));
        current_->deleted_values_.push_back(DecodeValue(v));
    }

    static const int kCacheSize = 1000;
    std::vector<int> deleted_keys_;
    std::vector<int> deleted_values_;
    LRUCache* cache_;

    LRUCacheTest() : cache_(LRUCache::New(kCacheSize)) {
        current_ = this;
    }
    ~LRUCacheTest() {
        cache_->Delete();
    }

    int Lookup(int key) {
        LRUCache::Handle* handle = cache_->Lookup(EncodeKey(key).c_str());
        const int r = (handle == NULL) ? -1 : DecodeValue(cache_->Value(handle));
        if (handle != NULL) {
            cache_->Release(handle);
        }
        return r;
    }

    void Insert(int key, int value, int charge = 1) {
        cache_->Release(cache_->Insert(
            EncodeKey(key).c_str(), EncodeValue(value), charge,
            &LRUCacheTest::Deleter)
        );
    }

    void Erase(int key) {
        cache_->Erase(EncodeKey(key).c_str());
    }
};
LRUCacheTest* LRUCacheTest::current_;

TEST(LRUCache, HitAndMiss) {
    LRUCacheTest cacheTest;
    auto p = &cacheTest;

    ASSERT_EQ(-1, p->Lookup(100));

    p->Insert(100, 101);
    ASSERT_EQ(101, p->Lookup(100));
    ASSERT_EQ(-1,  p->Lookup(200));
    ASSERT_EQ(-1,  p->Lookup(300));

    p->Insert(200, 201);
    ASSERT_EQ(101, p->Lookup(100));
    ASSERT_EQ(201, p->Lookup(200));
    ASSERT_EQ(-1,  p->Lookup(300));

    p->Insert(100, 102);
    ASSERT_EQ(102, p->Lookup(100));
    ASSERT_EQ(201, p->Lookup(200));
    ASSERT_EQ(-1,  p->Lookup(300));

    ASSERT_EQ(1, p->deleted_keys_.size());
    ASSERT_EQ(100, p->deleted_keys_[0]);
    ASSERT_EQ(101, p->deleted_values_[0]);
}

TEST(LRUCache, Erase) {
    LRUCacheTest cacheTest;
    auto p = &cacheTest;

    p->Erase(200);
    ASSERT_EQ(0, p->deleted_keys_.size());

    p->Insert(100, 101);
    p->Insert(200, 201);
    p->Erase(100);
    ASSERT_EQ(-1,  p->Lookup(100));
    ASSERT_EQ(201, p->Lookup(200));
    ASSERT_EQ(1, p->deleted_keys_.size());
    ASSERT_EQ(100, p->deleted_keys_[0]);
    ASSERT_EQ(101, p->deleted_values_[0]);

    p->Erase(100);
    ASSERT_EQ(-1,  p->Lookup(100));
    ASSERT_EQ(201, p->Lookup(200));
    ASSERT_EQ(1, p->deleted_keys_.size());
}

TEST(LRUCache, EntriesArePinned) {
    LRUCacheTest cacheTest;
    auto p = &cacheTest;

    p->Insert(100, 101);
    LRUCache::Handle* h1 = p->cache_->Lookup(EncodeKey(100).c_str());
    ASSERT_EQ(101, DecodeValue(p->cache_->Value(h1)));

    p->Insert(100, 102);
    LRUCache::Handle* h2 = p->cache_->Lookup(EncodeKey(100).c_str());
    ASSERT_EQ(102, DecodeValue(p->cache_->Value(h2)));
    ASSERT_EQ(0, p->deleted_keys_.size());

    p->cache_->Release(h1);
    ASSERT_EQ(1, p->deleted_keys_.size());
    ASSERT_EQ(100, p->deleted_keys_[0]);
    ASSERT_EQ(101, p->deleted_values_[0]);

    p->Erase(100);
    ASSERT_EQ(-1, p->Lookup(100));
    ASSERT_EQ(1, p->deleted_keys_.size());

    p->cache_->Release(h2);
    ASSERT_EQ(2, p->deleted_keys_.size());
    ASSERT_EQ(100, p->deleted_keys_[1]);
    ASSERT_EQ(102, p->deleted_values_[1]);
}

TEST(LRUCache, EvictionPolicy) {
    LRUCacheTest cacheTest;
    auto p = &cacheTest;

    p->Insert(100, 101);
    p->Insert(200, 201);

    // Frequently used entry must be kept around
    for (int i = 0; i < LRUCacheTest::kCacheSize + 100; i++) {
        p->Insert(1000+i, 2000+i);
        ASSERT_EQ(2000+i, p->Lookup(1000+i));
        ASSERT_EQ(101, p->Lookup(100));
    }
    ASSERT_EQ(101, p->Lookup(100));
    ASSERT_EQ(-1, p->Lookup(200));
}

TEST(LRUCache, HeavyEntries) {
    LRUCacheTest cacheTest;
    auto p = &cacheTest;

    // Add a bunch of light and heavy entries and then count the combined
    // size of items still in the cache, which must be approximately the
    // same as the total capacity.
    const int kLight = 1;
    const int kHeavy = 10;
    int added = 0;
    int index = 0;
    while (added < 2*LRUCacheTest::kCacheSize) {
        const int weight = (index & 1) ? kLight : kHeavy;
        p->Insert(index, 1000+index, weight);
        added += weight;
        index++;
    }

    int cached_weight = 0;
    for (int i = 0; i < index; i++) {
        const int weight = (i & 1 ? kLight : kHeavy);
        int r = p->Lookup(i);
        if (r >= 0) {
            cached_weight += weight;
            ASSERT_EQ(1000+i, r);
        }
    }
    ASSERT_TRUE(cached_weight < LRUCacheTest::kCacheSize + LRUCacheTest::kCacheSize/10);
}

TEST(LRUCache, NewId) {
    LRUCacheTest cacheTest;
    auto p = &cacheTest;

    uint64_t a = p->cache_->NewId();
    uint64_t b = p->cache_->NewId();
    ASSERT_TRUE(a != b);
}
