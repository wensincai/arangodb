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
    Metadata metadata(limit);
  }

  SECTION("test various getters") {
    uint64_t dummy;
    std::shared_ptr<Cache> dummyCache(reinterpret_cast<Cache*>(&dummy),
                                      [](Cache* p) -> void {});
    uint64_t limit = 1024;

    Metadata metadata(limit);
    metadata.link(dummyCache);

    metadata.lock();

    REQUIRE(dummyCache == metadata.cache());

    REQUIRE(limit == metadata.softLimit());
    REQUIRE(limit == metadata.hardLimit());
    REQUIRE(0UL == metadata.usage());

    metadata.unlock();
  }

  SECTION("verify usage limits are adjusted and enforced correctly") {
    bool success;

    Metadata metadata(1024ULL);

    metadata.lock();

    success = metadata.adjustUsageIfAllowed(512LL);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512LL);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512LL);
    REQUIRE(!success);

    success = metadata.adjustLimits(2048ULL, 2048ULL);
    REQUIRE(success);

    success = metadata.adjustUsageIfAllowed(1024LL);
    REQUIRE(success);

    success = metadata.adjustLimits(1024ULL, 2048ULL);
    REQUIRE(success);

    success = metadata.adjustUsageIfAllowed(512LL);
    REQUIRE(!success);
    success = metadata.adjustUsageIfAllowed(-512LL);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512LL);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(-1024LL);
    REQUIRE(success);
    success = metadata.adjustUsageIfAllowed(512LL);
    REQUIRE(!success);

    success = metadata.adjustLimits(1024ULL, 1024ULL);
    REQUIRE(success);
    success = metadata.adjustLimits(512ULL, 512ULL);
    REQUIRE(!success);

    metadata.unlock();
  }

  SECTION("test migration-related methods") {
    uint8_t dummyTable;
    uint8_t dummyAuxiliaryTable;
    uint32_t logSize = 1;
    uint32_t auxiliaryLogSize = 2;
    uint64_t limit = 1024;

    Metadata metadata(limit);

    metadata.lock();

    metadata.grantAuxiliaryTable(&dummyTable, logSize);
    metadata.swapTables();

    metadata.grantAuxiliaryTable(&dummyAuxiliaryTable, auxiliaryLogSize);
    REQUIRE(auxiliaryLogSize == metadata.auxiliaryLogSize());
    REQUIRE(&dummyAuxiliaryTable == metadata.auxiliaryTable());

    metadata.swapTables();
    REQUIRE(logSize == metadata.auxiliaryLogSize());
    REQUIRE(auxiliaryLogSize == metadata.logSize());
    REQUIRE(&dummyTable == metadata.auxiliaryTable());
    REQUIRE(&dummyAuxiliaryTable == metadata.table());

    uint8_t* result = metadata.releaseAuxiliaryTable();
    REQUIRE(0UL == metadata.auxiliaryLogSize());
    REQUIRE(nullptr == metadata.auxiliaryTable());
    REQUIRE(result == &dummyTable);

    result = metadata.releaseTable();
    REQUIRE(0UL == metadata.logSize());
    REQUIRE(nullptr == metadata.table());
    REQUIRE(result == &dummyAuxiliaryTable);

    metadata.unlock();
  }
}
