////////////////////////////////////////////////////////////////////////////////
/// @brief test suite for arangodb::cache::Metadata
///
/// @file
///
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "Cache/Metadata.h"
#include "Basics/Common.h"

#include "catch.hpp"

#include <stdint.h>
#include <memory>

using namespace arangodb::cache;

TEST_CASE("cache::Metadata", "[cache]") {
  SECTION("test basic constructor") {
    uint64_t limit = 1024;
    Metadata metadata(limit, true);
  }

  SECTION("test various getters") {
    uint64_t dummy;
    std::shared_ptr<Cache> dummyCache(reinterpret_cast<Cache*>(&dummy),
                                      [](Cache* p) -> void {});
    uint64_t limit = 1024;

    Metadata metadata(limit, true);

    REQUIRE(metadata.canGrow());

    metadata.lock();

    REQUIRE(limit == metadata.softLimit());
    REQUIRE(limit == metadata.hardLimit());
    REQUIRE(static_cast<uint64_t>(0) == metadata.usage());

    metadata.disableGrowth();
    metadata.unlock();

    REQUIRE(!metadata.canGrow());
  }

  SECTION("verify usage limits are adjusted and enforced correctly") {
    bool success;

    Metadata metadata(1024, true);

    metadata.lock();

    success = metadata.adjustUsageIfAllowed(512);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512);
    REQUIRE(!success);

    success = metadata.adjustLimits(2048, 2048);
    REQUIRE(success);

    success = metadata.adjustUsageIfAllowed(1024);
    REQUIRE(success);

    success = metadata.adjustLimits(1024, 2048);
    REQUIRE(success);

    success = metadata.adjustUsageIfAllowed(512);
    REQUIRE(!success);
    success = metadata.adjustUsageIfAllowed(-512);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(-1024);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512);
    REQUIRE(!success);

    success = metadata.adjustLimits(1024, 1024);
    REQUIRE(success);
    success = metadata.adjustLimits(512, 512);
    REQUIRE(!success);

    metadata.unlock();
  }
}
