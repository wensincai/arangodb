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

#include "Cache/Metadata.h"
#include "Cache/Cache.h"
#include "Cache/State.h"

#include <atomic>
#include <cstdint>

using namespace arangodb::cache;

Metadata::Metadata()
    : _state(), _usage(0), _softLimit(0), _hardLimit(0), _allowGrowth(false) {}

Metadata::Metadata(uint64_t limit, bool allowGrowth)
    : _state(),
      _usage(0),
      _softLimit(limit),
      _hardLimit(limit),
      _allowGrowth(allowGrowth) {}

Metadata::Metadata(Metadata const& other)
    : _state(other._state),
      _usage(other._usage),
      _softLimit(other._softLimit),
      _hardLimit(other._hardLimit),
      _allowGrowth(other._allowGrowth) {}

Metadata& Metadata::operator=(Metadata const& other) {
  if (this != &other) {
    _state = other._state;
    _usage = other._usage;
    _softLimit = other._softLimit;
    _hardLimit = other._hardLimit;
    _allowGrowth = other._allowGrowth;
  }

  return *this;
}

void Metadata::lock() { _state.lock(); }

void Metadata::unlock() {
  TRI_ASSERT(isLocked());
  _state.unlock();
}

bool Metadata::isLocked() const { return _state.isLocked(); }

uint64_t Metadata::usage() const {
  TRI_ASSERT(isLocked());
  return _usage;
}

uint64_t Metadata::softLimit() const {
  TRI_ASSERT(isLocked());
  return _softLimit;
}

uint64_t Metadata::hardLimit() const {
  TRI_ASSERT(isLocked());
  return _hardLimit;
}

bool Metadata::adjustUsageIfAllowed(int64_t usageChange) {
  TRI_ASSERT(isLocked());

  if (usageChange < 0) {
    _usage -= static_cast<uint64_t>(-usageChange);
    return true;
  }

  if ((static_cast<uint64_t>(usageChange) + _usage <= _softLimit) ||
      ((_usage > _softLimit) &&
       (static_cast<uint64_t>(usageChange) + _usage <= _hardLimit))) {
    _usage += static_cast<uint64_t>(usageChange);
    return true;
  }

  return false;
}

bool Metadata::adjustLimits(uint64_t softLimit, uint64_t hardLimit) {
  TRI_ASSERT(isLocked());

  if (!_allowGrowth && (hardLimit > _hardLimit)) {
    return false;
  }

  if (hardLimit < _usage) {
    return false;
  }

  _softLimit = softLimit;
  _hardLimit = hardLimit;

  return true;
}

void Metadata::enableGrowth() {
  TRI_ASSERT(_state.isLocked());
  _allowGrowth = true;
}

void Metadata::disableGrowth() {
  TRI_ASSERT(_state.isLocked());
  _allowGrowth = false;
}

bool Metadata::canGrow() {
  _state.lock();
  bool allowed = _allowGrowth;
  _state.unlock();
  return allowed;
}

bool Metadata::isSet(State::Flag flag) const {
  TRI_ASSERT(isLocked());
  return _state.isSet(flag);
}

void Metadata::toggleFlag(State::Flag flag) {
  TRI_ASSERT(isLocked());
  _state.toggleFlag(flag);
}
