////////////////////////////////////////////////////////////////////////////////
/// @brief test case for FailedLeader job
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
/// @author Andreas Streichardt
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////
#include "catch.hpp"
#include "fakeit.hpp"

#include "Agency/AddFollower.h"
#include "Agency/FailedLeader.h"
#include "Agency/MoveShard.h"
#include "Agency/AgentInterface.h"
#include "Agency/Node.h"
#include "lib/Basics/StringUtils.h"

#include <iostream>
#include <velocypack/Parser.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::consensus;
using namespace fakeit;

namespace arangodb {
namespace tests {
namespace failed_leader_test {

const std::string PREFIX = "arango";
const std::string DATABASE = "database";
const std::string COLLECTION = "collection";
const std::string SHARD = "shard";
const std::string SHARD_LEADER = "leader";
const std::string SHARD_FOLLOWER1 = "follower1";
const std::string SHARD_FOLLOWER2 = "follower2";
const std::string FREE_SERVER = "free";
const std::string FREE_SERVER2 = "free2";

const char *agency =
#include "FailedLeaderTest.json"
;

Node createNodeFromBuilder(VPackBuilder const& builder) {

  VPackBuilder opBuilder;
  { VPackObjectBuilder a(&opBuilder);
    opBuilder.add("new", builder.slice()); }
  
  Node node("");
  node.handle<SET>(opBuilder.slice());
  return node;

}

Node createRootNode() {

  VPackOptions options;
  options.checkAttributeUniqueness = true;
  VPackParser parser(&options);
  parser.parse(agency);
  
  VPackBuilder builder;
  builder.add(parser.steal()->slice());

  return createNodeFromBuilder(builder);
  
}

TEST_CASE("FailedLeader", "[agency][supervision]") {

  auto baseStructure = createRootNode();

  VPackBuilder builder;
  baseStructure.toBuilder(builder);

    
  write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};
  trans_ret_t fakeTransResult {true, "", 1, 0, std::make_shared<Builder>()};
  
SECTION("creating a job should create a job in todo") {
  Mock<AgentInterface> mockAgent;

  std::string jobId = "1";
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO(q->slice().toJson());
    auto expectedJobKey = "/arango/Target/ToDo/" + jobId;
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");
    REQUIRE(q->slice()[0][0].length() == 1); // should ONLY do an entry in todo
    REQUIRE(std::string(q->slice()[0][0].get(expectedJobKey).typeName()) == "object");

    auto job = q->slice()[0][0].get(expectedJobKey);
    REQUIRE(std::string(job.get("creator").typeName()) == "string");
    REQUIRE(std::string(job.get("type").typeName()) == "string");
    CHECK(job.get("type").copyString() == "failedLeader");
    REQUIRE(std::string(job.get("database").typeName()) == "string");
    CHECK(job.get("database").copyString() == DATABASE);
    REQUIRE(std::string(job.get("collection").typeName()) == "string");
    CHECK(job.get("collection").copyString() == COLLECTION);
    REQUIRE(std::string(job.get("shard").typeName()) == "string");
    CHECK(job.get("shard").copyString() == SHARD);
    REQUIRE(std::string(job.get("fromServer").typeName()) == "string");
    CHECK(job.get("fromServer").copyString() == SHARD_LEADER);
    CHECK(std::string(job.get("jobId").typeName()) == "string");
    CHECK(std::string(job.get("timeCreated").typeName()) == "string");

    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);
  AgentInterface &agent = mockAgent.get();

  auto failedLeader = FailedLeader(
    baseStructure,
    &agent,
    jobId,
    "unittest",
    DATABASE,
    COLLECTION,
    SHARD,
    SHARD_LEADER
  );
  failedLeader.create();
}

SECTION("if we want to start and the collection went missing from plan (our truth) the job should just finish") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION) {
      return builder;
    }

    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO(q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");

    auto writes = q->slice()[0][0];
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").typeName()) == "object");
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").get("op").typeName()) == "string");
    CHECK(writes.get("/arango/Target/ToDo/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Finished/1").typeName()) == "object");
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("if we are supposed to fail a distributeShardsLike job we immediately fail because this should be done by a job running on the master shard") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION) {
        builder->add("distributeShardsLike", VPackValue("PENG"));
      }

      if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO(q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");

    auto writes = q->slice()[0][0];
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").typeName()) == "object");
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").get("op").typeName()) == "string");
    CHECK(writes.get("/arango/Target/ToDo/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Failed/1").typeName()) == "object");
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("if we are supposed to fail a distributeShardsLike job we immediately fail because this should be done by a job running on the master shard") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION) {
        builder->add("distributeShardsLike", VPackValue("PENG"));
      }

      if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO(q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");

    auto writes = q->slice()[0][0];
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").typeName()) == "object");
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").get("op").typeName()) == "string");
    CHECK(writes.get("/arango/Target/ToDo/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Failed/1").typeName()) == "object");
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("if the leader is healthy again we fail the job") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Supervision/Health/" + SHARD_LEADER) {
        builder->add("Status", VPackValue("GOOD"));
      }

      if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  AgentInterface &agent = mockAgent.get();
  //agent.write();
  /*auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  Mock<Job> spy(failedLeader);
  Fake(Method(spy, abort));

  Job& spyFailedLeader = spy.get();
  spyFailedLeader.start();
  Verify(Method(spy, abort));
  */
}

SECTION("the job must not be started if there is no server that is in sync for every shard") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  INFO(builder->toJson());
  Node agency = createNodeFromBuilder(*builder);
  
  // nothing should happen
  Mock<AgentInterface> mockAgent;
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("the job must not be started if there if one of the linked shards (distributeShardsLike) is not in sync") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Current/Collections/" + DATABASE) {
        // we fake that follower2 is in sync
        builder->add(VPackValue("linkedcollection1"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue("linkedshard1"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("servers"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER2));
            }
          }
        }
        // for the other shard there is only follower1 in sync
        builder->add(VPackValue("linkedcollection2"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue("linkedshard2"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("servers"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
            }
          }
        }
      } else if (path == "/arango/Plan/Collections/" + DATABASE) {
        builder->add(VPackValue("linkedcollection1"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add("distributeShardsLike", VPackValue(COLLECTION));
          builder->add(VPackValue("shards"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("linkedshard1"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
              builder->add(VPackValue(SHARD_FOLLOWER2));
            }
          }
        }
        builder->add(VPackValue("linkedcollection2"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add("distributeShardsLike", VPackValue(COLLECTION));
          builder->add(VPackValue("shards"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("linkedshard2"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
              builder->add(VPackValue(SHARD_FOLLOWER2));
            }
          }
        }
      } else if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  INFO("Agency: " << builder->toJson());
  Node agency = createNodeFromBuilder(*builder);

  // nothing should happen
  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, transact)).AlwaysDo([&](query_t const& q) -> trans_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    REQUIRE(false);
    return trans_ret_t();
  });
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("abort any moveShard job blocking the shard and start") {
  Mock<AgentInterface> moveShardMockAgent;
  AgentInterface &moveShardAgent = moveShardMockAgent.get();

  auto moveShardBuilder = std::make_shared<VPackBuilder>();
  moveShardBuilder->openObject();
  auto moveShard = MoveShard(baseStructure("arango"), &moveShardAgent, "2", "strunz", DATABASE, COLLECTION, SHARD, SHARD_LEADER, SHARD_FOLLOWER1, true);
  moveShard.create(moveShardBuilder);
  moveShardBuilder->close();

  std::string jobId = "1";
  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }
      if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("2"));
      } else if (path == "/arango/Target/Pending") {
        builder->add("2", moveShardBuilder->slice());
      } else if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER2));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  INFO("Teststructure: " << builder->toJson());
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    // check that moveshard is being moved to failed
    INFO("WriteTransaction: " << q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");
    REQUIRE(std::string(q->slice()[0][0].get("/arango/Target/Failed/2").typeName()) == "object");
    return fakeWriteResult;
  });
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("abort any addFollower job blocking the shard and start") {
  Mock<AgentInterface> addFollowerMockAgent;

  VPackBuilder addFollowerBuilder;
  When(Method(addFollowerMockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction(create): " << q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() > 0); // preconditions!
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");

    REQUIRE(std::string(q->slice()[0][0].get("/arango/Target/ToDo/2").typeName()) == "object");
    addFollowerBuilder.add(q->slice()[0][0].get("/arango/Target/ToDo/2"));

    return fakeWriteResult;
  });
  When(Method(addFollowerMockAgent, waitFor)).Return();
  AgentInterface &addFollowerAgent = addFollowerMockAgent.get();
  auto addFollower = AddFollower(baseStructure("arango"), &addFollowerAgent, "2", "strunz", DATABASE, COLLECTION, SHARD);
  addFollower.create();

  std::string jobId = "1";
  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }
      if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("2"));
      } else if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
        builder->add("2", addFollowerBuilder.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER2));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  INFO("Teststructure: " << builder->toJson());
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    // check that moveshard is being moved to failed
    INFO("WriteTransaction: " << q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");
    REQUIRE(std::string(q->slice()[0][0].get("/arango/Target/Failed/2").typeName()) == "object");
    return fakeWriteResult;
  });
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}

SECTION("if everything is fine than the job should be written to pending, adding the toServer") {
  std::string jobId = "1";

  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }
      if (path == "/arango/Target/ToDo") {
        VPackBuilder jobBuilder;
        jobBuilder.add(VPackValue(VPackValueType::Object));
        jobBuilder.add("creator", VPackValue("1"));
        jobBuilder.add("type", VPackValue("failedLeader"));
        jobBuilder.add("database", VPackValue(DATABASE));
        jobBuilder.add("collection", VPackValue(COLLECTION));
        jobBuilder.add("shard", VPackValue(SHARD));
        jobBuilder.add("fromServer", VPackValue(SHARD_LEADER));
        jobBuilder.add("jobId", VPackValue(jobId));
        jobBuilder.add("timeCreated", VPackValue("2017-01-01 00:00:00"));
        jobBuilder.close();
        builder->add("1", jobBuilder.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER2));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  INFO("Teststructure: " << builder->toJson());
  Node agency = createNodeFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, transact)).AlwaysDo([&](query_t const& q) -> trans_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    REQUIRE(std::string(q->slice().typeName()) == "array" );
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() == 2); // preconditions!
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");
    REQUIRE(std::string(q->slice()[0][1].typeName()) == "object");

    auto writes = q->slice()[0][0];
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").typeName()) == "object");
    REQUIRE(std::string(writes.get("/arango/Target/ToDo/1").get("op").typeName()) == "string");
    CHECK(writes.get("/arango/Target/ToDo/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/ToDo/1").typeName()) == "object");
    REQUIRE(std::string(writes.get("/arango/Target/Pending/1").typeName()) == "object");

    auto job = writes.get("/arango/Target/Pending/1");
    REQUIRE(std::string(job.get("toServer").typeName()) == "string");
    CHECK(job.get("toServer").copyString() == SHARD_FOLLOWER2);
    CHECK(std::string(job.get("timeStarted").typeName()) == "string");

    REQUIRE(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName()) == "array");
    // also the free server should be added to the list so the replicationFactor stays ok
    REQUIRE(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() == 4);
    REQUIRE(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].typeName()) == "string");
    REQUIRE(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[1].typeName()) == "string");
    REQUIRE(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[2].typeName()) == "string");
    REQUIRE(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[3].typeName()) == "string");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == SHARD_FOLLOWER2);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[1].copyString() == SHARD_LEADER);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[2].copyString() == SHARD_FOLLOWER1);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[3].copyString() == FREE_SERVER);

    auto preconditions = q->slice()[0][1];
    REQUIRE(std::string(preconditions.get("/arango/Supervision/Shards/" + SHARD).typeName()) == "object");
    REQUIRE(std::string(preconditions.get("/arango/Supervision/Shards/" + SHARD).get("oldEmpty").typeName()) == "bool");
    CHECK(preconditions.get("/arango/Supervision/Shards/" + SHARD).get("oldEmpty").getBool() == true);

    REQUIRE(std::string(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName()) == "object");
    REQUIRE(std::string(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").typeName()) == "array");
    REQUIRE(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").length() == 3);
    REQUIRE(std::string(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[0].typeName()) == "string");
    REQUIRE(std::string(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[1].typeName()) == "string");
    REQUIRE(std::string(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[2].typeName()) == "string");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[0].copyString() == SHARD_LEADER);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[1].copyString() == SHARD_FOLLOWER1);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[2].copyString() == SHARD_FOLLOWER2);

    auto result = std::make_shared<VPackBuilder>();
    result->openArray();
    result->add(VPackValue((uint64_t)1));
    result->close();
    return trans_ret_t(true, "1", 1, 0, result);
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);
  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    agency("arango"),
    &agent,
    JOB_STATUS::TODO,
    jobId
  );
  failedLeader.start();
}


/*
TEST_CASE( "FailedLeader should fill FailedServers with failed shards", "[agency][supervision]" ) {
  Node root = createRootNode();
  auto prefixRoot = root.children().find("arango")->second;

  write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    REQUIRE(q->slice().isArray() );
    REQUIRE(q->slice().length() > 0);
    REQUIRE(q->slice()[0].isArray() > 0);
    REQUIRE(q->slice()[0].length() > 0);
    REQUIRE(q->slice()[0][0].isObject() > 0);
    REQUIRE(q->slice()[0][0].get(PREFIX + "/Target/FailedServers/" + FROM).isObject());

    auto failedServers = q->slice()[0][0].get(PREFIX + "/Target/FailedServers/" + FROM);

    REQUIRE(failedServers.isObject());
    REQUIRE(failedServers.get("new").isString());
    REQUIRE(failedServers.get("new").copyString() == SHARD);
    REQUIRE(failedServers.get("op").isString());
    REQUIRE(failedServers.get("op").copyString() == "push");

    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    root("arango"),
    &agent,
    "1",
    "unittest",
    DATABASE,
    COLLECTION,
    SHARD,
    FROM,
    TO
  );
  failedLeader.run();
}

TEST_CASE("A FailedLeader job should be queued", "[agency][supervision]") {
  Node root = createRootNode();
  write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    REQUIRE(q->slice().isArray() );
    REQUIRE(q->slice().length() > 0);
    REQUIRE(q->slice()[0].isArray() > 0);
    REQUIRE(q->slice()[0].length() > 0);
    REQUIRE(q->slice()[0][0].isObject() > 0);
    REQUIRE(q->slice()[0][0].get(PREFIX + "/Target/ToDo/1").isObject());

    auto job = q->slice()[0][0].get(PREFIX + "/Target/ToDo/1");
    REQUIRE(job.get("collection").isString());
    REQUIRE(job.get("collection").copyString() == COLLECTION);
    REQUIRE(job.get("database").isString());
    REQUIRE(job.get("database").copyString() == DATABASE);
    REQUIRE(job.get("type").isString());
    REQUIRE(job.get("type").copyString() == "failedLeader");

    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    root("arango"),
    &agent,
    "1",
    "unittest",
    DATABASE,
    COLLECTION,
    SHARD,
    FROM,
    TO
  );
  failedLeader.run();
}

TEST_CASE("FailedLeader should set a new leader and set the new one as a follower", "[agency][supervision]") {
  write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};
  Node root = createRootNode();
  {
    Mock<AgentInterface> mockAgent;
    When(Method(mockAgent, write)).Return(fakeWriteResult);
    When(Method(mockAgent, write)).Return(fakeWriteResult);
    When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
      REQUIRE(q->slice().isArray());
      REQUIRE(q->slice().length() == 1);
      REQUIRE(q->slice()[0].isArray());
      REQUIRE(q->slice()[0].length() == 1);
      REQUIRE(q->slice()[0][0].isObject());
      for (auto const& i : VPackObjectIterator(q->slice()[0][0])) {
        std::string key = i.key.copyString();
        std::vector<std::string> keys = StringUtils::split(key, '/', '\0');

        if (i.value.isObject() && i.value.hasKey("op")) {
          root(keys).applieOp(i.value);
        } else {
          root(keys).applies(i.value);
        }
      }
      auto arangoNode = root.children().find("arango");

      return fakeWriteResult;
    });

    When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

    AgentInterface &agent = mockAgent.get();
    auto failedLeader = FailedLeader(
      root("arango"),
      &agent,
      "1",
      "unittest",
      DATABASE,
      COLLECTION,
      SHARD,
      FROM,
      TO
    );
    failedLeader.run();
  }

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).Return(fakeWriteResult);
  When(Method(mockAgent, write)).Return(fakeWriteResult);
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    REQUIRE(q->slice().isArray());
    REQUIRE(q->slice().length() == 1);
    REQUIRE(q->slice()[0].isArray());
    REQUIRE(q->slice()[0].length() == 2); // preconditions should be present
    REQUIRE(q->slice()[0][0].isObject());

    bool shardPlanFound = false;
    for (auto const& i : VPackObjectIterator(q->slice()[0][0])) {
      std::string key = i.key.copyString();
      std::vector<std::string> keys = StringUtils::split(key, '/', '\0');

      if (i.key.copyString() == PREFIX + "/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        shardPlanFound = true;
        REQUIRE(i.value.toJson() == "[\"to\",\"from\"]");
      }
    }
    REQUIRE(shardPlanFound);

    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

  AgentInterface &agent = mockAgent.get();
  auto failedLeader = FailedLeader(
    root("arango"),
    &agent,
    "1",
    "unittest",
    DATABASE,
    COLLECTION,
    SHARD,
    FROM,
    TO
  );
  failedLeader.run();
}*/

}
}
}
}
