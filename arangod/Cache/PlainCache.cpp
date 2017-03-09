////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2017 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Daniel H. Larkin
////////////////////////////////////////////////////////////////////////////////

#include "Cache/PlainCache.h"
#include "Basics/Common.h"
#include "Cache/Cache.h"
#include "Cache/CachedValue.h"
#include "Cache/Common.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/Metadata.h"
#include "Cache/PlainBucket.h"
#include "Cache/State.h"
#include "Cache/Table.h"

#include <stdint.h>
#include <atomic>
#include <chrono>
#include <list>

using namespace arangodb::cache;

Cache::Finding PlainCache::find(void const* key, uint32_t keySize) {
  TRI_ASSERT(key != nullptr);
  Finding result(nullptr);
  uint32_t hash = hashKey(key, keySize);

  bool ok;
  PlainBucket* bucket;
  std::tie(ok, bucket) = getBucket(hash, Cache::triesFast);

  if (ok) {
    result.reset(bucket->find(hash, key, keySize));
    recordStat(result.found() ? Stat::findHit : Stat::findMiss);
    bucket->unlock();
    endOperation();
  }

  return result;
}

bool PlainCache::insert(CachedValue* value) {
  TRI_ASSERT(value != nullptr);
  bool inserted = false;
  uint32_t hash = hashKey(value->key(), value->keySize);

  bool ok;
  PlainBucket* bucket;
  std::tie(ok, bucket) = getBucket(hash, Cache::triesFast);

  if (ok) {
    bool allowed = true;
    bool eviction = false;
    int64_t change = static_cast<int64_t>(value->size());
    CachedValue* candidate = bucket->find(hash, value->key(), value->keySize);

    if (candidate == nullptr && bucket->isFull()) {
      candidate = bucket->evictionCandidate();
      if (candidate == nullptr) {
        allowed = false;
      } else {
        eviction = true;
      }
    }

    if (allowed) {
      if (candidate != nullptr) {
        change -= static_cast<int64_t>(candidate->size());
      }

      _metadata.lock();
      allowed = _metadata.adjustUsageIfAllowed(change);
      _metadata.unlock();

      if (allowed) {
        if (candidate != nullptr) {
          bucket->evict(candidate, true);
          freeValue(candidate);
        }
        recordStat(eviction ? Stat::insertEviction : Stat::insertNoEviction);
        bucket->insert(hash, value);
        inserted = true;
      } else {
        requestResize();  // let function do the hard work
      }
    }

    bucket->unlock();
    if (inserted) {
      requestMigrate();  // let function do the hard work
    }
    endOperation();
  }

  return inserted;
}

bool PlainCache::remove(void const* key, uint32_t keySize) {
  TRI_ASSERT(key != nullptr);
  bool removed = false;
  uint32_t hash = hashKey(key, keySize);

  bool ok;
  PlainBucket* bucket;
  std::tie(ok, bucket) = getBucket(hash, Cache::triesSlow);

  if (ok) {
    CachedValue* candidate = bucket->remove(hash, key, keySize);

    if (candidate != nullptr) {
      int64_t change = -static_cast<int64_t>(candidate->size());

      _metadata.lock();
      bool allowed = _metadata.adjustUsageIfAllowed(change);
      TRI_ASSERT(allowed);
      _metadata.unlock();

      freeValue(candidate);
    }

    removed = true;
    bucket->unlock();
    endOperation();
  }

  return removed;
}

bool PlainCache::blacklist(void const* key, uint32_t keySize) { return false; }

uint64_t PlainCache::allocationSize(bool enableWindowedStats) {
  return sizeof(PlainCache) +
         StatBuffer::allocationSize(_evictionStatsCapacity) +
         (enableWindowedStats ? (sizeof(StatBuffer) +
                                 StatBuffer::allocationSize(_findStatsCapacity))
                              : 0);
}

std::shared_ptr<Cache> PlainCache::create(Manager* manager, Metadata metadata,
                                          std::shared_ptr<Table> table,
                                          bool enableWindowedStats) {
  return std::make_shared<PlainCache>(Cache::ConstructionGuard(), manager,
                                      metadata, table, enableWindowedStats);
}

PlainCache::PlainCache(Cache::ConstructionGuard guard, Manager* manager,
                       Metadata metadata, std::shared_ptr<Table> table,
                       bool enableWindowedStats)
    : Cache(guard, manager, metadata, table, enableWindowedStats,
            PlainCache::bucketClearer) {}

PlainCache::~PlainCache() {
  _state.lock();
  if (!_state.isSet(State::Flag::shutdown)) {
    _state.unlock();
    shutdown();
  }
  if (_state.isLocked()) {
    _state.unlock();
  }
}

uint64_t PlainCache::freeMemoryFrom(uint32_t hash) {
  uint64_t reclaimed = 0;
  bool ok;
  PlainBucket* bucket;
  std::tie(ok, bucket) = getBucket(hash, Cache::triesFast, false);

  if (ok) {
    // evict LRU freeable value if exists
    CachedValue* candidate = bucket->evictionCandidate();

    if (candidate != nullptr) {
      reclaimed = candidate->size();
      bucket->evict(candidate);
      freeValue(candidate);
    }

    bucket->unlock();
  }

  return reclaimed;
}

void PlainCache::migrateBucket(void* sourcePtr,
                               std::unique_ptr<Table::Subtable> targets) {
  // lock current bucket
  auto source = reinterpret_cast<PlainBucket*>(sourcePtr);
  source->lock(-1LL);

  // lock target bucket(s)
  targets->applyToAllBuckets([](void* ptr) -> bool {
    auto targetBucket = reinterpret_cast<PlainBucket*>(ptr);
    return targetBucket->lock(Cache::triesGuarantee);
  });

  for (size_t j = 0; j < PlainBucket::slotsData; j++) {
    size_t k = PlainBucket::slotsData - (j + 1);
    if (source->_cachedHashes[k] != 0) {
      uint32_t hash = source->_cachedHashes[k];
      CachedValue* value = source->_cachedData[k];

      auto targetBucket =
          reinterpret_cast<PlainBucket*>(targets->fetchBucket(hash));
      bool haveSpace = true;
      if (targetBucket->isFull()) {
        CachedValue* candidate = targetBucket->evictionCandidate();
        if (candidate != nullptr) {
          targetBucket->evict(candidate, true);
          uint64_t size = candidate->size();
          freeValue(candidate);
          reclaimMemory(size);
        } else {
          haveSpace = false;
        }
      }
      if (haveSpace) {
        targetBucket->insert(hash, value);
      } else {
        uint64_t size = value->size();
        freeValue(value);
        reclaimMemory(size);
      }

      source->_cachedHashes[k] = 0;
      source->_cachedData[k] = nullptr;
    }
  }

  // unlock targets
  targets->applyToAllBuckets([](void* ptr) -> bool {
    auto targetBucket = reinterpret_cast<PlainBucket*>(ptr);
    targetBucket->unlock();
    return true;
  });

  // finish up this bucket's migration
  source->_state.toggleFlag(State::Flag::migrated);
  source->unlock();
}

std::pair<bool, PlainBucket*> PlainCache::getBucket(uint32_t hash,
                                                    int64_t maxTries,
                                                    bool singleOperation) {
  PlainBucket* bucket = nullptr;

  bool ok = _state.lock(maxTries);
  if (ok) {
    bool started = false;
    ok = isOperational();
    if (ok) {
      if (singleOperation) {
        startOperation();
        started = true;
        _manager->reportAccess(shared_from_this());
      }

      bucket = reinterpret_cast<PlainBucket*>(
          _table->fetchAndLockBucket(hash, maxTries));
      ok = (bucket != nullptr);
    }
    if (!ok && started) {
      endOperation();
    }
    _state.unlock();
  }

  return std::pair<bool, PlainBucket*>(ok, bucket);
}

Table::BucketClearer PlainCache::bucketClearer(Metadata* metadata) {
  return [metadata](void* ptr) -> void {
    auto bucket = reinterpret_cast<PlainBucket*>(ptr);
    bucket->lock(Cache::triesGuarantee);
    for (size_t j = 0; j < PlainBucket::slotsData; j++) {
      if (bucket->_cachedData[j] != nullptr) {
        uint64_t size = bucket->_cachedData[j]->size();
        freeValue(bucket->_cachedData[j]);
        metadata->lock();
        metadata->adjustUsageIfAllowed(-static_cast<int64_t>(size));
        metadata->unlock();
      }
    }
    bucket->clear();
  };
}
