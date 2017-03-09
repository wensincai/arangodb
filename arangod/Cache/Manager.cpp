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

#include "Cache/Manager.h"
#include "Basics/Common.h"
#include "Basics/asio-helper.h"
#include "Cache/Cache.h"
#include "Cache/CachedValue.h"
#include "Cache/Common.h"
#include "Cache/FrequencyBuffer.h"
#include "Cache/ManagerTasks.h"
#include "Cache/Metadata.h"
#include "Cache/PlainCache.h"
#include "Cache/State.h"
#include "Cache/Table.h"
#include "Cache/Transaction.h"
#include "Cache/TransactionalCache.h"

#include <stdint.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <memory>
#include <set>
#include <stack>
#include <utility>

using namespace arangodb::cache;

const uint64_t Manager::minSize = 1024 * 1024;

bool Manager::cmp_weak_ptr::operator()(
    std::weak_ptr<Cache> const& left, std::weak_ptr<Cache> const& right) const {
  return !left.owner_before(right) && !right.owner_before(left);
}

size_t Manager::hash_weak_ptr::operator()(
    const std::weak_ptr<Cache>& wp) const {
  auto sp = wp.lock();
  return std::hash<decltype(sp)>()(sp);
}

Manager::Manager(boost::asio::io_service* ioService, uint64_t globalLimit,
                 bool enableWindowedStats)
    : _state(),
      _accessStats((globalLimit >= (1024ULL * 1024ULL * 1024ULL))
                       ? ((1024ULL * 1024ULL) / sizeof(std::shared_ptr<Cache>))
                       : (globalLimit / 8192ULL)),
      _accessCounter(0),
      _enableWindowedStats(enableWindowedStats),
      _findStats(nullptr),
      _findHits(0),
      _findMisses(0),
      _caches(),
      _globalSoftLimit(globalLimit),
      _globalHardLimit(globalLimit),
      _globalAllocation(sizeof(Manager) + Manager::tableListsOverhead +
                        _accessStats.memoryUsage()),
      _transactions(),
      _ioService(ioService),
      _resizeAttempt(0),
      _outstandingTasks(0),
      _rebalancingTasks(0),
      _resizingTasks(0) {
  TRI_ASSERT(_globalAllocation < _globalSoftLimit);
  TRI_ASSERT(_globalAllocation < _globalHardLimit);
  try {
    _findStats.reset(new Manager::FindStatBuffer(16384));
  } catch (std::bad_alloc) {
    _findStats.reset(nullptr);
    _enableWindowedStats = false;
  }
}

Manager::~Manager() { shutdown(); }

std::shared_ptr<Cache> Manager::createCache(CacheType type,
                                            uint64_t requestedLimit,
                                            bool allowGrowth,
                                            bool enableWindowedStats) {
  std::shared_ptr<Cache> result(nullptr);
  _state.lock();
  bool allowed = isOperational();
  Metadata metadata;
  std::shared_ptr<Table> table(nullptr);

  if (allowed) {
    uint64_t fixedSize = 0;
    switch (type) {
      case CacheType::Plain:
        fixedSize = PlainCache::allocationSize(enableWindowedStats);
        break;
      case CacheType::Transactional:
        fixedSize = TransactionalCache::allocationSize(enableWindowedStats);
        break;
      default:
        break;
    }
    std::tie(allowed, metadata, table) =
        registerCache(requestedLimit, allowGrowth, fixedSize);
  }

  if (allowed) {
    switch (type) {
      case CacheType::Plain:
        result = PlainCache::create(this, metadata, table, enableWindowedStats);
        break;
      case CacheType::Transactional:
        result = TransactionalCache::create(this, metadata, table,
                                            enableWindowedStats);
        break;
      default:
        break;
    }
  }

  if (result.get() != nullptr) {
    _caches.emplace(result);
  }
  _state.unlock();

  return result;
}

void Manager::destroyCache(std::shared_ptr<Cache> cache) {
  Cache::destroy(cache);
}

void Manager::beginShutdown() {
  _state.lock();
  if (isOperational()) {
    _state.toggleFlag(State::Flag::shuttingDown);
    for (auto it = _caches.begin(); it != _caches.end(); it++) {
      std::shared_ptr<Cache> cache = *it;
      cache->beginShutdown();
    }
  }
  _state.unlock();
}

void Manager::shutdown() {
  _state.lock();
  if (!_state.isSet(State::Flag::shutdown)) {
    if (!_state.isSet(State::Flag::shuttingDown)) {
      _state.toggleFlag(State::Flag::shuttingDown);
    }
    while (!_caches.empty()) {
      std::shared_ptr<Cache> cache = *_caches.begin();
      _state.unlock();
      cache->shutdown();
      _state.lock();
    }
    freeUnusedTables();
    _state.clear();
    _state.toggleFlag(State::Flag::shutdown);
  }
  _state.unlock();
}

// change global cache limit
bool Manager::resize(uint64_t newGlobalLimit) {
  if (newGlobalLimit < Manager::minSize) {
    return false;
  }

  bool success = true;
  _state.lock();

  if (!isOperational() || globalProcessRunning()) {
    // shut(ting) down or still have another global process running already
    success = false;
  } else {
    // otherwise we need to actually resize
    _state.toggleFlag(State::Flag::resizing);
    internalResize(newGlobalLimit, true);
  }

  _state.unlock();
  return success;
}

uint64_t Manager::globalLimit() {
  _state.lock();
  uint64_t limit =
      _state.isSet(State::Flag::resizing) ? _globalSoftLimit : _globalHardLimit;
  _state.unlock();

  return limit;
}

uint64_t Manager::globalAllocation() {
  _state.lock();
  uint64_t allocation = _globalAllocation;
  _state.unlock();

  return allocation;
}

std::pair<double, double> Manager::globalHitRates() {
  double lifetimeRate = std::nan("");
  double windowedRate = std::nan("");

  uint64_t currentHits = _findHits.load();
  uint64_t currentMisses = _findMisses.load();
  if (currentHits + currentMisses > 0) {
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

  return std::make_pair(lifetimeRate, windowedRate);
}

Transaction* Manager::beginTransaction(bool readOnly) {
  return _transactions.begin(readOnly);
}

void Manager::endTransaction(Transaction* tx) { _transactions.end(tx); }

std::tuple<bool, Metadata, std::shared_ptr<Table>> Manager::registerCache(
    uint64_t requestedLimit, bool allowGrowth, uint64_t fixedSize) {
  TRI_ASSERT(_state.isLocked());
  bool ok = true;
  uint32_t logSize = 0;
  uint32_t tableLogSize = Table::minLogSize;
  for (; (static_cast<uint64_t>(1) << logSize) < requestedLimit; logSize++) {
  }
  uint64_t grantedLimit = static_cast<uint64_t>(1) << logSize;
  if (logSize > (Table::minLogSize + Table::standardLogSizeAdjustment)) {
    tableLogSize = logSize - Table::standardLogSizeAdjustment;
  }

  while (logSize >= Cache::minLogSize) {
    uint64_t tableAllocation =
        _tables[tableLogSize].empty() ? Table::allocationSize(tableLogSize) : 0;
    if (increaseAllowed(grantedLimit + tableAllocation +
                        Manager::cacheRecordOverhead + fixedSize)) {
      break;
    }

    grantedLimit >>= 1U;
    logSize--;
    if (tableLogSize > Table::minLogSize) {
      tableLogSize--;
    }
  }

  if (logSize < Cache::minLogSize) {
    ok = false;
  }

  Metadata metadata;
  std::shared_ptr<Table> table(nullptr);
  if (ok) {
    _globalAllocation +=
        (grantedLimit + Manager::cacheRecordOverhead + fixedSize);
    metadata = Metadata(grantedLimit, allowGrowth);
    table = leaseTable(tableLogSize);
  }

  return std::make_tuple(ok, metadata, table);
}

void Manager::unregisterCache(std::shared_ptr<Cache> cache) {
  _state.lock();
  _caches.erase(cache);
  _state.unlock();
}

std::pair<bool, Manager::time_point> Manager::requestResize(
    std::shared_ptr<Cache> cache, uint64_t requestedLimit) {
  Manager::time_point nextRequest = futureTime(100);
  bool allowed = false;

  bool ok = _state.lock(Manager::triesSlow);
  if (ok) {
    if (isOperational() && !_state.isSet(State::Flag::resizing)) {
      Metadata* metadata = cache->metadata();
      metadata->lock();

      if (!metadata->isSet(State::Flag::resizing) &&
          ((requestedLimit < metadata->hardLimit()) ||
           increaseAllowed(requestedLimit - metadata->hardLimit()))) {
        allowed = true;
        if (requestedLimit > metadata->hardLimit()) {
          // if cache is growing, let it keep growing quickly
          nextRequest = std::chrono::steady_clock::now();
        }
        resizeCache(TaskEnvironment::none, cache,
                    requestedLimit);  // unlocks metadata
      } else {
        metadata->unlock();
      }
    }
    _state.unlock();
  }

  return std::make_pair(allowed, nextRequest);
}

std::pair<bool, Manager::time_point> Manager::requestMigrate(
    std::shared_ptr<Cache> cache, uint32_t requestedLogSize) {
  Manager::time_point nextRequest = futureTime(100);
  bool allowed = false;

  bool ok = _state.lock(Manager::triesSlow);
  if (ok) {
    if (isOperational() && !_state.isSet(State::Flag::resizing)) {
      if (!_tables[requestedLogSize].empty() ||
          increaseAllowed(Table::allocationSize(requestedLogSize))) {
        allowed = true;
      }
      if (allowed) {
        Metadata* metadata = cache->metadata();
        metadata->lock();
        if (metadata->isSet(State::Flag::migrating)) {
          allowed = false;
        }
        if (allowed) {
          nextRequest = std::chrono::steady_clock::now();
          migrateCache(TaskEnvironment::none, cache,
                       requestedLogSize);  // unlocks metadata
        } else {
          metadata->unlock();
        }
      }
    }
    _state.unlock();
  }

  return std::make_pair(allowed, nextRequest);
}

void Manager::reportAccess(std::shared_ptr<Cache> cache) {
  // if (((++_accessCounter) & static_cast<uint64_t>(7)) == 0) {  // record 1 in
  // 8
  _accessStats.insertRecord(cache);
  //}
}

void Manager::reportHitStat(Stat stat) {
  switch (stat) {
    case Stat::findHit: {
      _findHits++;
      if (_enableWindowedStats && _findStats.get() != nullptr) {
        _findStats->insertRecord(static_cast<uint8_t>(Stat::findHit));
      }
      break;
    }
    case Stat::findMiss: {
      _findMisses++;
      if (_enableWindowedStats && _findStats.get() != nullptr) {
        _findStats->insertRecord(static_cast<uint8_t>(Stat::findMiss));
      }
      break;
    }
    default: { break; }
  }
}

bool Manager::isOperational() const {
  TRI_ASSERT(_state.isLocked());
  return (!_state.isSet(State::Flag::shutdown) &&
          !_state.isSet(State::Flag::shuttingDown));
}

bool Manager::globalProcessRunning() const {
  TRI_ASSERT(_state.isLocked());
  return (_state.isSet(State::Flag::rebalancing) ||
          _state.isSet(State::Flag::resizing));
}

boost::asio::io_service* Manager::ioService() { return _ioService; }

void Manager::prepareTask(Manager::TaskEnvironment environment) {
  _outstandingTasks++;
  switch (environment) {
    case TaskEnvironment::rebalancing: {
      _rebalancingTasks++;
      break;
    }
    case TaskEnvironment::resizing: {
      _resizingTasks++;
      break;
    }
    case TaskEnvironment::none:
    default: { break; }
  }
}

void Manager::unprepareTask(Manager::TaskEnvironment environment) {
  switch (environment) {
    case TaskEnvironment::rebalancing: {
      if ((--_rebalancingTasks) == 0) {
        _state.lock();
        _state.toggleFlag(State::Flag::rebalancing);
        _state.unlock();
      };
      break;
    }
    case TaskEnvironment::resizing: {
      if ((--_resizingTasks) == 0) {
        _state.lock();
        internalResize(_globalSoftLimit, false);
        _state.unlock();
      };
      break;
    }
    case TaskEnvironment::none:
    default: { break; }
  }

  _outstandingTasks--;
}

bool Manager::rebalance() {
  _state.lock();
  if (!isOperational() || globalProcessRunning()) {
    _state.unlock();
    return false;
  }

  // start rebalancing
  _state.toggleFlag(State::Flag::rebalancing);

  // determine strategy

  // allow background tasks if more than 7/8ths full
  bool allowTasks =
      _globalAllocation >
      static_cast<uint64_t>(0.875 * static_cast<double>(_globalHardLimit));

  // be aggressive if more than 3/4ths full
  bool beAggressive =
      _globalAllocation >
      static_cast<uint64_t>(0.75 * static_cast<double>(_globalHardLimit));

  // aim for 3/8th with background tasks, 1/4th if no tasks but aggressive, no
  // goal otherwise
  uint64_t goal =
      beAggressive
          ? (allowTasks ? static_cast<uint64_t>(
                              0.375 * static_cast<double>(_globalHardLimit))
                        : static_cast<uint64_t>(
                              0.25 * static_cast<double>(_globalHardLimit)))
          : 0;

  if (goal > 0) {
    // get stats on cache access to prioritize freeing from less frequently used
    // caches first, so more frequently used ones stay large
    std::shared_ptr<PriorityList> cacheList = priorityList();

    // just adjust limits
    uint64_t reclaimed =
        resizeAllCaches(TaskEnvironment::rebalancing, cacheList, allowTasks,
                        beAggressive, goal);
    _globalAllocation -= reclaimed;
  }

  if (_rebalancingTasks.load() == 0) {
    _state.toggleFlag(State::Flag::rebalancing);
  }

  _state.unlock();
  return true;
}

void Manager::internalResize(uint64_t newGlobalLimit, bool firstAttempt) {
  TRI_ASSERT(_state.isLocked());
  bool done = false;
  std::shared_ptr<PriorityList> cacheList(nullptr);

  if (firstAttempt) {
    _resizeAttempt = 0;
  }

  if (!isOperational()) {
    // abort resizing process so we can shutdown
    done = true;
  }

  // if limit is safe, just set it
  if (!done) {
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // see if we can free enough from unused tables
  if (!done) {
    freeUnusedTables();
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // must resize individual caches
  if (!done) {
    _globalSoftLimit = newGlobalLimit;

    // get stats on cache access to prioritize freeing from less frequently used
    // caches first, so more frequently used ones stay large
    cacheList = priorityList();

    // first just adjust limits down to usage
    uint64_t reclaimed =
        resizeAllCaches(TaskEnvironment::resizing, cacheList, true, true,
                        _globalAllocation - _globalSoftLimit);
    _globalAllocation -= reclaimed;
    done = adjustGlobalLimitsIfAllowed(newGlobalLimit);
  }

  // still haven't freed enough, now try cutting allocations more aggressively
  // by allowing use of background tasks to actually free memory from caches
  if (!done) {
    if ((_resizeAttempt % 2) == 0) {
      resizeAllCaches(TaskEnvironment::resizing, cacheList, false, true,
                      _globalAllocation - _globalSoftLimit);
    } else {
      migrateAllCaches(TaskEnvironment::resizing, cacheList,
                       _globalAllocation - _globalSoftLimit);
    }
  }

  if ((_resizingTasks.load() == 0)) {
    _state.toggleFlag(State::Flag::resizing);
  }
}

uint64_t Manager::resizeAllCaches(Manager::TaskEnvironment environment,
                                  std::shared_ptr<PriorityList> cacheList,
                                  bool noTasks, bool aggressive,
                                  uint64_t goal) {
  TRI_ASSERT(_state.isLocked());
  uint64_t reclaimed = 0;

  for (std::shared_ptr<Cache> cache : *cacheList) {
    // skip this cache if it is already resizing or shutdown!
    if (!cache->canResize()) {
      continue;
    }

    Metadata* metadata = cache->metadata();
    metadata->lock();

    uint64_t newLimit;
    if (aggressive) {
      newLimit =
          (noTasks ? metadata->usage()
                   : (std::min)(metadata->usage(), metadata->hardLimit() / 2));
    } else {
      newLimit = std::max(metadata->usage(),
                          (metadata->hardLimit() + metadata->usage()) / 2);
    }
    newLimit = std::max(newLimit, Cache::minSize);

    reclaimed += metadata->hardLimit() - newLimit;
    resizeCache(environment, cache, newLimit);  // unlocks cache

    if (goal > 0 && reclaimed >= goal) {
      break;
    }
  }

  return reclaimed;
}

uint64_t Manager::migrateAllCaches(Manager::TaskEnvironment environment,
                                   std::shared_ptr<PriorityList> cacheList,
                                   uint64_t goal) {
  TRI_ASSERT(_state.isLocked());
  uint64_t reclaimed = 0;

  for (std::shared_ptr<Cache> cache : *cacheList) {
    // skip this cache if it is already migrating or shutdown!
    if (!cache->canMigrate()) {
      continue;
    }

    Metadata* metadata = cache->metadata();
    metadata->lock();
    std::shared_ptr<Table> table = cache->table();

    uint32_t logSize = table->logSize();
    if ((logSize > Table::minLogSize) &&
        increaseAllowed(Table::allocationSize(logSize - 1))) {
      reclaimed +=
          (Table::allocationSize(logSize) - Table::allocationSize(logSize - 1));
      migrateCache(environment, cache, logSize - 1);  // unlocks metadata
    } else {
      metadata->unlock();
    }

    if (goal > 0 && reclaimed >= goal) {
      break;
    }
  }

  return reclaimed;
}

void Manager::freeUnusedTables() {
  TRI_ASSERT(_state.isLocked());
  for (size_t i = 0; i < 32; i++) {
    while (!_tables[i].empty()) {
      _tables[i].pop();
    }
  }
}

bool Manager::adjustGlobalLimitsIfAllowed(uint64_t newGlobalLimit) {
  TRI_ASSERT(_state.isLocked());
  if (newGlobalLimit < _globalAllocation) {
    return false;
  }

  _globalSoftLimit = newGlobalLimit;
  _globalHardLimit = newGlobalLimit;

  return true;
}

void Manager::resizeCache(Manager::TaskEnvironment environment,
                          std::shared_ptr<Cache> cache, uint64_t newLimit) {
  TRI_ASSERT(_state.isLocked());
  Metadata* metadata = cache->metadata();
  TRI_ASSERT(metadata->isLocked());

  if (metadata->usage() <= newLimit) {
    bool success = metadata->adjustLimits(newLimit, newLimit);
    TRI_ASSERT(success);
    metadata->unlock();
    return;
  }

  bool success = metadata->adjustLimits(newLimit, metadata->hardLimit());
  TRI_ASSERT(success);
  TRI_ASSERT(!metadata->isSet(State::Flag::resizing));
  metadata->toggleFlag(State::Flag::resizing);
  metadata->unlock();

  auto task = std::make_shared<FreeMemoryTask>(environment, this, cache);
  bool dispatched = task->dispatch();
  if (!dispatched) {
    // TODO: decide what to do if we don't have an io_service
  }
}

void Manager::migrateCache(Manager::TaskEnvironment environment,
                           std::shared_ptr<Cache> cache, uint32_t logSize) {
  TRI_ASSERT(_state.isLocked());
  Metadata* metadata = cache->metadata();
  TRI_ASSERT(metadata->isLocked());

  std::shared_ptr<Table> table = leaseTable(logSize);
  TRI_ASSERT(!metadata->isSet(State::Flag::migrating));
  metadata->toggleFlag(State::Flag::migrating);
  metadata->unlock();

  auto task = std::make_shared<MigrateTask>(environment, this, cache, table);
  bool dispatched = task->dispatch();
  if (!dispatched) {
    // TODO: decide what to do if we don't have an io_service
  }
}

std::shared_ptr<Table> Manager::leaseTable(uint32_t logSize) {
  TRI_ASSERT(_state.isLocked());

  std::shared_ptr<Table> table(nullptr);
  if (_tables[logSize].empty()) {
    try {
      table = std::make_shared<Table>(logSize);
      _globalAllocation += Table::allocationSize(logSize);
    } catch (std::bad_alloc) {
      table.reset();
    }
  } else {
    table = _tables[logSize].top();
    _tables[logSize].pop();
  }

  return table;
}

void Manager::reclaimTable(std::shared_ptr<Table> table) {
  TRI_ASSERT(table.get() != nullptr);
  _state.lock();

  uint32_t logSize = table->logSize();
  size_t maxTables = (logSize < 16) ? 20 - logSize : 4;
  if (_tables[logSize].size() < maxTables) {
    _tables[logSize].emplace(table);
  } else {
    _globalAllocation -= table->memoryUsage();
    table.reset();
  }
  _state.unlock();
}

bool Manager::increaseAllowed(uint64_t increase) const {
  TRI_ASSERT(_state.isLocked());
  if (_state.isSet(State::Flag::resizing) &&
      (_globalAllocation <= _globalSoftLimit)) {
    return (increase <= (_globalSoftLimit - _globalAllocation));
  }

  return (increase <= (_globalHardLimit - _globalAllocation));
}

std::shared_ptr<Manager::PriorityList> Manager::priorityList() {
  TRI_ASSERT(_state.isLocked());
  std::shared_ptr<PriorityList> list(new PriorityList());
  list->reserve(_caches.size());

  // catalog accessed caches
  auto stats = _accessStats.getFrequencies();
  std::set<std::shared_ptr<Cache>> accessed;
  for (auto s : *stats) {
    if (auto cache = s.first.lock()) {
      accessed.emplace(cache);
    }
  }

  // gather all unaccessed caches at beginning of list
  for (auto it = _caches.begin(); it != _caches.end(); it++) {
    auto found = accessed.find(*it);
    if (found == accessed.end()) {
      list->emplace_back(*it);
    }
  }

  // gather all accessed caches in order
  for (auto s : *stats) {
    if (auto cache = s.first.lock()) {
      list->emplace_back(cache);
    }
  }

  return list;
}

Manager::time_point Manager::futureTime(uint64_t millisecondsFromNow) {
  return (std::chrono::steady_clock::now() +
          std::chrono::milliseconds(millisecondsFromNow));
}
