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

#include "Cache/Cache.h"
#include "Basics/Common.h"
#include "Basics/fasthash.h"
#include "Cache/CachedValue.h"
#include "Cache/Common.h"
#include "Cache/Manager.h"
#include "Cache/Metadata.h"
#include "Cache/State.h"
#include "Cache/Table.h"
#include "Random/RandomGenerator.h"

#include <stdint.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <list>
#include <thread>

using namespace arangodb::cache;

const uint64_t Cache::minSize = 1024;
const uint64_t Cache::minLogSize = 10;

uint64_t Cache::_evictionStatsCapacity = 1024;
uint64_t Cache::_findStatsCapacity = 16384;

Cache::ConstructionGuard::ConstructionGuard() {}

Cache::Finding::Finding(CachedValue* v) : _value(v) {
  if (_value != nullptr) {
    _value->lease();
  }
}

Cache::Finding::Finding(Finding const& other) : _value(other._value) {
  if (_value != nullptr) {
    _value->lease();
  }
}

Cache::Finding::Finding(Finding&& other) : _value(other._value) {
  other._value = nullptr;
}

Cache::Finding& Cache::Finding::operator=(Finding const& other) {
  if (&other == this) {
    return *this;
  }

  if (_value != nullptr) {
    _value->release();
  }

  _value = other._value;
  if (_value != nullptr) {
    _value->lease();
  }

  return *this;
}

Cache::Finding& Cache::Finding::operator=(Finding&& other) {
  if (&other == this) {
    return *this;
  }

  if (_value != nullptr) {
    _value->release();
  }

  _value = other._value;
  other._value = nullptr;

  return *this;
}

Cache::Finding::~Finding() {
  if (_value != nullptr) {
    _value->release();
  }
}

void Cache::Finding::reset(CachedValue* v) {
  if (_value != nullptr) {
    _value->release();
  }

  _value = v;
  if (_value != nullptr) {
    _value->lease();
  }
}

bool Cache::Finding::found() const { return (_value != nullptr); }

CachedValue const* Cache::Finding::value() const { return _value; }

CachedValue* Cache::Finding::copy() const {
  return ((_value == nullptr) ? nullptr : _value->copy());
}

Cache::Cache(ConstructionGuard guard, Manager* manager, Metadata metadata,
             std::shared_ptr<Table> table, bool enableWindowedStats,
             std::function<Table::BucketClearer(Metadata*)> bucketClearer)
    : _state(),
      _evictionStats(_evictionStatsCapacity),
      _insertionCount(0),
      _enableWindowedStats(enableWindowedStats),
      _findStats(nullptr),
      _findHits(0),
      _findMisses(0),
      _manager(manager),
      _metadata(metadata),
      _table(table),
      _bucketClearer(bucketClearer(&_metadata)),
      _openOperations(0),
      _migrateRequestTime(std::chrono::steady_clock::now()),
      _resizeRequestTime(std::chrono::steady_clock::now()),
      _lastResizeRequestStatus(true) {
  _table->registerClearer(_bucketClearer);
  _table->enable();
  if (_enableWindowedStats) {
    try {
      _findStats.reset(new StatBuffer(_findStatsCapacity));
    } catch (std::bad_alloc) {
      _findStats.reset(nullptr);
      _enableWindowedStats = false;
    }
  }
}

uint64_t Cache::limit() {
  uint64_t limit = 0;
  _state.lock();
  if (isOperational()) {
    _metadata.lock();
    limit = _metadata.softLimit();
    _metadata.unlock();
  }
  _state.unlock();
  return limit;
}

uint64_t Cache::usage() {
  uint64_t usage = 0;
  _state.lock();
  if (isOperational()) {
    _metadata.lock();
    usage = _metadata.usage();
    _metadata.unlock();
  }
  _state.unlock();
  return usage;
}

std::pair<double, double> Cache::hitRates() {
  double lifetimeRate = std::nan("");
  double windowedRate = std::nan("");

  uint64_t currentMisses = _findMisses.load();
  uint64_t currentHits = _findHits.load();
  if (currentMisses + currentHits > 0) {
    lifetimeRate = 100 * (static_cast<double>(currentHits) /
                          static_cast<double>(currentHits + currentMisses));
  }

  if (_enableWindowedStats && _findStats.get() != nullptr) {
    auto stats = _findStats->getFrequencies();
    if (stats->size() == 1) {
      if ((*stats)[0].first == static_cast<uint8_t>(Stat::findHit)) {
        windowedRate = 100.0;
      } else {
        windowedRate = 0.0;
      }
    } else if (stats->size() == 2) {
      if ((*stats)[0].first == static_cast<uint8_t>(Stat::findHit)) {
        currentHits = (*stats)[0].second;
        currentMisses = (*stats)[1].second;
      } else {
        currentHits = (*stats)[1].second;
        currentMisses = (*stats)[0].second;
      }
      if (currentHits + currentMisses > 0) {
        windowedRate = 100 * (static_cast<double>(currentHits) /
                              static_cast<double>(currentHits + currentMisses));
      }
    }
  }

  return std::pair<double, double>(lifetimeRate, windowedRate);
}

void Cache::disableGrowth() {
  _metadata.lock();
  _metadata.disableGrowth();
  _metadata.unlock();
}

void Cache::enableGrowth() {
  _metadata.lock();
  _metadata.enableGrowth();
  _metadata.unlock();
}

bool Cache::resize(uint64_t requestedLimit) {
  _state.lock();
  bool allowed = isOperational();
  bool resized = false;
  startOperation();
  _state.unlock();

  if (allowed) {
    // wait for previous resizes to finish
    while (true) {
      _metadata.lock();
      if (!_metadata.isSet(State::Flag::resizing)) {
        _metadata.unlock();
        break;
      }
      _metadata.unlock();
    }

    resized = requestResize(requestedLimit, false);
  }

  endOperation();
  return resized;
}

bool Cache::isResizing() {
  bool resizing = false;
  _state.lock();
  if (isOperational()) {
    _metadata.lock();
    resizing = _metadata.isSet(State::Flag::resizing);
    _metadata.unlock();
    _state.unlock();
  }

  return resizing;
}

void Cache::destroy(std::shared_ptr<Cache> cache) {
  if (cache.get() != nullptr) {
    cache->shutdown();
  }
}

bool Cache::isOperational() const {
  TRI_ASSERT(_state.isLocked());
  return (!_state.isSet(State::Flag::shutdown) &&
          !_state.isSet(State::Flag::shuttingDown));
}

void Cache::startOperation() { ++_openOperations; }

void Cache::endOperation() { --_openOperations; }

bool Cache::isMigrating() const {
  TRI_ASSERT(_state.isLocked());
  return _state.isSet(State::Flag::migrating);
}

bool Cache::requestResize(uint64_t requestedLimit, bool internal) {
  bool resized = false;
  int64_t lockTries = internal ? Cache::triesFast : Cache::triesGuarantee;
  bool ok = _state.lock(lockTries);
  if (ok) {
    if (!internal || (std::chrono::steady_clock::now() > _resizeRequestTime)) {
      _metadata.lock();
      uint64_t newLimit =
          (requestedLimit > 0)
              ? requestedLimit
              : (_lastResizeRequestStatus
                     ? (_metadata.hardLimit() * 2)
                     : (static_cast<uint64_t>(
                           static_cast<double>(_metadata.hardLimit()) * 1.25)));
      bool downsizing = (newLimit < _metadata.hardLimit());
      _metadata.unlock();
      if (downsizing || _metadata.canGrow()) {
        std::tie(resized, _resizeRequestTime) =
            _manager->requestResize(shared_from_this(), newLimit);
        _lastResizeRequestStatus = resized;
      }
    }
    _state.unlock();
  }
  return resized;
}

void Cache::requestMigrate(uint32_t requestedLogSize) {
  if ((++_insertionCount & 0xFFF) == 0) {
    bool ok = canMigrate();
    if (ok) {
      auto stats = _evictionStats.getFrequencies();
      if (((stats->size() == 1) &&
           ((*stats)[0].first == static_cast<uint8_t>(Stat::insertEviction))) ||
          ((stats->size() == 2) &&
           (((*stats)[0].first ==
             static_cast<uint8_t>(Stat::insertNoEviction)) ||
            ((*stats)[0].second * 16 > (*stats)[1].second)))) {
        ok = _state.lock(Cache::triesGuarantee);
        if (ok) {
          if (!isMigrating() &&
              (std::chrono::steady_clock::now() > _migrateRequestTime)) {
            _metadata.lock();
            uint32_t newLogSize = (requestedLogSize > 0)
                                      ? requestedLogSize
                                      : (_table->logSize() + 1);
            _metadata.unlock();
            std::tie(ok, _resizeRequestTime) =
                _manager->requestMigrate(shared_from_this(), newLogSize);
            if (ok) {
              _evictionStats.clear();
            }
          }
          _state.unlock();
        }
      }
    }
  }
}

void Cache::freeValue(CachedValue* value) {
  while (value->refCount.load() > 0) {
    std::this_thread::yield();
  }

  delete value;
}

bool Cache::reclaimMemory(uint64_t size) {
  _metadata.lock();
  _metadata.adjustUsageIfAllowed(-static_cast<int64_t>(size));
  bool underLimit = (_metadata.softLimit() >= _metadata.usage());
  _metadata.unlock();

  return underLimit;
}

uint32_t Cache::hashKey(void const* key, uint32_t keySize) const {
  return (std::max)(static_cast<uint32_t>(1),
                    fasthash32(key, keySize, 0xdeadbeefUL));
}

void Cache::recordStat(Stat stat) {
  switch (stat) {
    case Stat::insertEviction:
    case Stat::insertNoEviction: {
      _evictionStats.insertRecord(static_cast<uint8_t>(stat));
      break;
    }
    case Stat::findHit: {
      _findHits++;
      if (_enableWindowedStats && _findStats.get() != nullptr) {
        _findStats->insertRecord(static_cast<uint8_t>(Stat::findHit));
      }
      _manager->reportHitStat(Stat::findHit);
      break;
    }
    case Stat::findMiss: {
      _findMisses++;
      if (_enableWindowedStats && _findStats.get() != nullptr) {
        _findStats->insertRecord(static_cast<uint8_t>(Stat::findMiss));
      }
      _manager->reportHitStat(Stat::findMiss);
      break;
    }
    default: { break; }
  }
}

Metadata* Cache::metadata() { return &_metadata; }

std::shared_ptr<Table> Cache::table() { return _table; }

void Cache::beginShutdown() {
  _state.lock();
  if (!_state.isSet(State::Flag::shutdown) &&
      !_state.isSet(State::Flag::shuttingDown)) {
    _state.toggleFlag(State::Flag::shuttingDown);
  }
  _state.unlock();
}

void Cache::shutdown() {
  _state.lock();
  auto handle = shared_from_this();  // hold onto self-reference to prevent
                                     // pre-mature shared_ptr destruction
  TRI_ASSERT(handle.get() == this);
  if (!_state.isSet(State::Flag::shutdown)) {
    if (!_state.isSet(State::Flag::shuttingDown)) {
      _state.toggleFlag(State::Flag::shuttingDown);
    }

    while (_openOperations.load() > 0) {
      _state.unlock();
      std::this_thread::yield();
      _state.lock();
    }

    _state.clear();
    _state.toggleFlag(State::Flag::shutdown);
    std::shared_ptr<Table> extra =
        _table->setAuxiliary(std::shared_ptr<Table>(nullptr));
    if (extra.get() != nullptr) {
      extra->clear();
      _manager->reclaimTable(extra);
    }
    _table->clear();
    _manager->reclaimTable(_table);
    _manager->unregisterCache(shared_from_this());
  }
  _state.unlock();
}

bool Cache::canResize() {
  bool allowed = true;
  _state.lock();
  if (isOperational()) {
    _metadata.lock();
    if (_metadata.isSet(State::Flag::resizing)) {
      allowed = false;
    }
    _metadata.unlock();
  } else {
    allowed = false;
  }
  _state.unlock();

  return allowed;
}

bool Cache::canMigrate() {
  bool allowed = (_manager->ioService() != nullptr);
  if (allowed) {
    _state.lock();
    if (isOperational()) {
      if (_state.isSet(State::Flag::migrating)) {
        allowed = false;
      } else {
        _metadata.lock();
        if (_metadata.isSet(State::Flag::migrating)) {
          allowed = false;
        }
        _metadata.unlock();
      }
    } else {
      allowed = false;
    }
    _state.unlock();
  }

  return allowed;
}

bool Cache::freeMemory() {
  _state.lock();
  if (!isOperational()) {
    _state.unlock();
    return false;
  }
  startOperation();
  _state.unlock();

  bool underLimit = reclaimMemory(0ULL);
  uint64_t failures = 0;
  while (!underLimit) {
    // pick a random bucket
    uint32_t randomHash = RandomGenerator::interval(UINT32_MAX);
    uint64_t reclaimed = freeMemoryFrom(randomHash);

    if (reclaimed > 0) {
      failures = 0;
      underLimit = reclaimMemory(reclaimed);
    } else {
      failures++;
      if (failures > 100) {
        _state.lock();
        bool shouldQuit = !isOperational();
        _state.unlock();

        if (shouldQuit) {
          break;
        } else {
          failures = 0;
        }
      }
    }
  }

  endOperation();
  return true;
}

bool Cache::migrate(std::shared_ptr<Table> newTable) {
  _state.lock();
  if (!isOperational()) {
    _state.unlock();
    return false;
  }
  startOperation();
  newTable->registerClearer(_bucketClearer);
  newTable->enable();
  _table->setAuxiliary(newTable);
  TRI_ASSERT(!_state.isSet(State::Flag::migrating));
  _state.toggleFlag(State::Flag::migrating);
  _state.unlock();

  // do the actual migration
  for (uint32_t i = 0; i < _table->size(); i++) {
    migrateBucket(_table->primaryBucket(i), _table->auxiliaryBuckets(i));
  }

  // swap tables
  _state.lock();
  std::shared_ptr<Table> oldTable = _table;
  _table = newTable;
  _state.unlock();

  // clear out old table and release it
  std::shared_ptr<Table> confirm =
      oldTable->setAuxiliary(std::shared_ptr<Table>(nullptr));
  TRI_ASSERT(confirm.get() == newTable.get());
  oldTable->clear();
  _manager->reclaimTable(oldTable);

  // unmarking migrating flags
  _state.lock();
  _state.toggleFlag(State::Flag::migrating);
  _state.unlock();
  _metadata.lock();
  _metadata.toggleFlag(State::Flag::migrating);
  _metadata.unlock();

  endOperation();
  return true;
}
