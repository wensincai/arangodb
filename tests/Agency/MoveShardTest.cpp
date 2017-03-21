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

#include "Agency/MoveShard.h"
#include "Agency/AgentInterface.h"
#include "Agency/Node.h"

#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::consensus;
using namespace fakeit;

const std::string PREFIX = "arango";
const std::string DATABASE = "database";
const std::string COLLECTION = "collection";
const std::string SHARD = "shard";
const std::string SHARD_LEADER = "leader";
const std::string SHARD_FOLLOWER1 = "follower1";
const std::string FREE_SERVER = "free";
const std::string FREE_SERVER2 = "free2";

using namespace arangodb;
using namespace arangodb::basics;
using namespace arangodb::consensus;
using namespace fakeit;

namespace arangodb {
namespace tests {
namespace move_shard_test {

Node createAgencyFromBuilder(VPackBuilder const& builder) {
  Node node("");

  VPackBuilder opBuilder;
  {
    VPackObjectBuilder a(&opBuilder);
    opBuilder.add("new", builder.slice());
  }

  node.handle<SET>(opBuilder.slice());
  return node(PREFIX);
}

#define CHECK_FAILURE(source, query) \
    std::string sourceKey = "/arango/Target/";\
    sourceKey += source;\
    sourceKey += "/1"; \
    REQUIRE(std::string(q->slice().typeName()) == "array"); \
    REQUIRE(q->slice().length() == 1); \
    REQUIRE(std::string(q->slice()[0].typeName()) == "array"); \
    REQUIRE(q->slice()[0].length() == 1); \
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object"); \
    auto writes = q->slice()[0][0]; \
    REQUIRE(std::string(writes.get(sourceKey).typeName()) == "object"); \
    REQUIRE(std::string(writes.get(sourceKey).get("op").typeName()) == "string"); \
    CHECK(writes.get(sourceKey).get("op").copyString() == "delete"); \
    CHECK(std::string(writes.get("/arango/Target/Failed/1").typeName()) == "object");

Node createRootNode() {
  Node root("ROOT");

  VPackBuilder builder;
  {
    VPackObjectBuilder a(&builder);
    builder.add(VPackValue("new"));
    {
      VPackObjectBuilder a(&builder);
      builder.add(VPackValue(PREFIX));
      {
        VPackObjectBuilder b(&builder);
        builder.add(VPackValue("Target"));
        {
          VPackObjectBuilder c(&builder);
          builder.add(VPackValue("ToDo"));
          {
            VPackObjectBuilder d(&builder);
          }
          builder.add(VPackValue("Pending"));
          {
            VPackObjectBuilder d(&builder);
          }
          builder.add(VPackValue("Finished"));
          {
            VPackObjectBuilder d(&builder);
          }
          builder.add(VPackValue("Failed"));
          {
            VPackObjectBuilder d(&builder);
          }
          builder.add(VPackValue("FailedServers"));
          {
            VPackObjectBuilder d(&builder);
          }
          builder.add(VPackValue("CleanedServers"));
          {
            VPackArrayBuilder d(&builder);
          }
        }
        builder.add(VPackValue("Current"));
        {
          VPackObjectBuilder c(&builder);
          builder.add(VPackValue("Collections"));
          {
            VPackObjectBuilder d(&builder);
            builder.add(VPackValue(DATABASE));
            {
              VPackObjectBuilder e(&builder);
              builder.add(VPackValue(COLLECTION));
              {
                VPackObjectBuilder f(&builder);
                builder.add(VPackValue(SHARD));
                {
                  VPackObjectBuilder f(&builder);
                  builder.add(VPackValue("servers"));
                  {
                    VPackArrayBuilder g(&builder);
                    builder.add(VPackValue(SHARD_LEADER));
                    builder.add(VPackValue(SHARD_FOLLOWER1));
                  }
                }
              }
            }
          }
        }
        builder.add(VPackValue("Plan"));
        {
          VPackObjectBuilder c(&builder);
          builder.add(VPackValue("Collections"));
          {
            VPackObjectBuilder d(&builder);
            builder.add(VPackValue(DATABASE));
            {
              VPackObjectBuilder e(&builder);
              builder.add(VPackValue(COLLECTION));
              {
                VPackObjectBuilder f(&builder);
                builder.add(VPackValue("shards"));
                {
                  VPackObjectBuilder f(&builder);
                  builder.add(VPackValue(SHARD));
                  {
                    VPackArrayBuilder g(&builder);
                    builder.add(VPackValue(SHARD_LEADER));
                    builder.add(VPackValue(SHARD_FOLLOWER1));
                  }
                }
              }
            }
          }
          builder.add(VPackValue("DBServers"));
          {
            VPackObjectBuilder d(&builder);
            builder.add(SHARD_LEADER, VPackValue("none"));
            builder.add(SHARD_FOLLOWER1, VPackValue("none"));
            builder.add(FREE_SERVER, VPackValue("none"));
            builder.add(FREE_SERVER2, VPackValue("none"));
          }
        }
        builder.add(VPackValue("Supervision"));
        {
          VPackObjectBuilder c(&builder);
          builder.add(VPackValue("Health"));
          {
            VPackObjectBuilder c(&builder);
            builder.add(VPackValue(SHARD_LEADER));
            {
              VPackObjectBuilder e(&builder);
              builder.add("Status", VPackValue("GOOD"));
            }
            builder.add(VPackValue(SHARD_FOLLOWER1));
            {
              VPackObjectBuilder e(&builder);
              builder.add("Status", VPackValue("GOOD"));
            }
            builder.add(VPackValue(FREE_SERVER));
            {
              VPackObjectBuilder e(&builder);
              builder.add("Status", VPackValue("GOOD"));
            }
            builder.add(VPackValue(FREE_SERVER2));
            {
              VPackObjectBuilder e(&builder);
              builder.add("Status", VPackValue("GOOD"));
            }
          }
          builder.add(VPackValue("DBServers"));
          {
            VPackObjectBuilder c(&builder);
          }
          builder.add(VPackValue("Shards"));
          {
            VPackObjectBuilder c(&builder);
          }
        }
      }
    }
  }
  root.handle<SET>(builder.slice());
  return root;
}

VPackBuilder createJob(std::string const& collection, std::string const& from, std::string const& to) {
  VPackBuilder builder;
  {
    VPackObjectBuilder b(&builder);
    builder.add("jobId", VPackValue("1"));
    builder.add("creator", VPackValue("unittest"));
    builder.add("type", VPackValue("moveShard"));
    builder.add("database", VPackValue(DATABASE));
    builder.add("collection", VPackValue(collection));
    builder.add("shard", VPackValue(SHARD));
    builder.add("fromServer", VPackValue(from));
    builder.add("toServer", VPackValue(to));
    builder.add("isLeader", VPackValue(from == SHARD_LEADER));
  }
  return builder;
}

TEST_CASE("MoveShard", "[agency][supervision]") {
auto baseStructure = createRootNode();
write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};
std::string const jobId = "1";

SECTION("the job should fail if toServer does not exist") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, "unfug").slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    CHECK_FAILURE("ToDo", q);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should fail to start if toServer is already in plan") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, SHARD_FOLLOWER1).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    CHECK_FAILURE("ToDo", q);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should fail if fromServer does not exist") {
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
        builder->add(jobId, createJob(COLLECTION, "unfug", FREE_SERVER).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  Mock<Job> spy(moveShard);
  Fake(Method(spy, finish));

  Job& spyMoveShard = spy.get();
  spyMoveShard.start();

  Verify(Method(spy, finish).Matching([](std::string const& server, std::string const& shard, bool success, std::string const& reason){return !success;}));
}

SECTION("the job should fail if fromServer is not in plan of the shard") {
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
        builder->add(jobId, createJob(COLLECTION, FREE_SERVER, FREE_SERVER2).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    CHECK_FAILURE("ToDo", q);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should fail if fromServer does not exist") {
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
        builder->add(jobId, createJob(COLLECTION, "unfug", FREE_SERVER).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
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
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should remain in todo if the shard is currently locked") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("2"));
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  // nothing should be called (job remains in ToDo)
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
}

SECTION("the job should remain in todo if the target server is currently locked") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("2"));
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  // nothing should be called (job remains in ToDo)
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
}

SECTION("the job should fail if the target server was cleaned out") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Target/CleanedServers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    CHECK_FAILURE("ToDo", q);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should fail if the target server is failed") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      }

      if (path == "/arango/Target/FailedServers") {
        builder->add(FREE_SERVER, VPackValue(true));
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    CHECK_FAILURE("ToDo", q);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should wait until the target server is good") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Supervision/Health/" + FREE_SERVER + "/Status") {
        builder->add(VPackValue("FAILED"));
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
}

SECTION("the job should fail if the shard distributes its shards like some other") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION) {
        builder->add("distributeShardsLike", VPackValue("PENG"));
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    CHECK_FAILURE("ToDo", q);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("the job should be moved to pending when everything is ok") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    std::string sourceKey = "/arango/Target/ToDo/1";
    REQUIRE(std::string(q->slice().typeName()) == "array");
    REQUIRE(q->slice().length() == 1);
    REQUIRE(std::string(q->slice()[0].typeName()) == "array");
    REQUIRE(q->slice()[0].length() == 2);
    REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");
    REQUIRE(std::string(q->slice()[0][1].typeName()) == "object");

    auto writes = q->slice()[0][0];
    REQUIRE(std::string(writes.get(sourceKey).typeName()) == "object");
    REQUIRE(std::string(writes.get(sourceKey).get("op").typeName()) == "string");
    CHECK(writes.get(sourceKey).get("op").copyString() == "delete");
    CHECK(writes.get("/arango/Supervision/Shards/" + SHARD).copyString() == "1");
    CHECK(writes.get("/arango/Supervision/DBServers/" + FREE_SERVER).copyString() == "1");
    CHECK(writes.get("/arango/Plan/Version").get("op").copyString() == "increment");
    CHECK(std::string(writes.get("/arango/Target/Pending/1").typeName()) == "object");
    CHECK(std::string(writes.get("/arango/Target/Pending/1").get("timeStarted").typeName()) == "string");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() == 3); // leader, oldFollower, newLeader
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == SHARD_LEADER);

    // order not really relevant ... assume it might appear anyway
    auto followers = writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD);
    bool found = false;
    for (auto const& server: VPackArrayIterator(followers)) {
      if (server.copyString() == FREE_SERVER) {
        found = true;
      }
    }
    CHECK(found == true);

    auto preconditions = q->slice()[0][1];
    CHECK(preconditions.get("/arango/Target/CleanedServers").get("old").toJson() == "[]");
    CHECK(preconditions.get("/arango/Target/FailedServers").get("old").toJson() == "{}");
    CHECK(preconditions.get("/arango/Supervision/Health/" + FREE_SERVER + "/Status").get("old").copyString() == "GOOD");
    CHECK(preconditions.get("/arango/Supervision/DBServers/" + FREE_SERVER).get("oldEmpty").getBool() == true);
    CHECK(preconditions.get("/arango/Supervision/Shards/" + SHARD).get("oldEmpty").getBool() == true);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").toJson() == "[\"" + SHARD_LEADER + "\",\"" + SHARD_FOLLOWER1 + "\"]");

    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("moving from a follower should be possible") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_FOLLOWER1, FREE_SERVER).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());

    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() == 3); // leader, oldFollower, newLeader
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == SHARD_LEADER);

    // order not really relevant ... assume it might appear anyway
    auto followers = writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD);
    bool found = false;
    for (auto const& server: VPackArrayIterator(followers)) {
      if (server.copyString() == FREE_SERVER) {
        found = true;
      }
    }
    CHECK(found == true);
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("when moving a shard that is a distributeShardsLike leader move the rest as well") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      } else if (path == "/arango/Current/Collections/" + DATABASE) {
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
              builder->add(VPackValue(SHARD_FOLLOWER1));
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
            }
          }
        }
        builder->add(VPackValue("unrelatedcollection"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue("shards"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("unrelatedshard"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
            }
          }
        }
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() == 3); // leader, oldFollower, newLeader

    auto json = writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).toJson();
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/linkedcollection1/shards/linkedshard1").toJson() == json);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/linkedcollection2/shards/linkedshard2").toJson() == json);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/unrelatedcollection/shards/unrelatedshard").isNone());
    CHECK(writes.get("/arango/Supervision/Shards/" + SHARD).copyString() == "1");
    CHECK(writes.get("/arango/Supervision/Shards/unrelatedshard").isNone());

    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn();

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.start();
  Verify(Method(mockAgent,write));
}

SECTION("if the job is too old it should be aborted to prevent a deadloop") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue("2015-01-03T20:00:00Z"));
        }
        builder->add(jobId, pendingJob.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  Node agency = createAgencyFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();

  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  Mock<Job> spy(moveShard);
  Fake(Method(spy, abort));

  Job& spyMoveShard = spy.get();
  spyMoveShard.run();

  Verify(Method(spy, abort));
}

SECTION("if the job is too old (leader case) it should be aborted to prevent a deadloop") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_LEADER, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue("2015-01-03T20:00:00Z"));
        }
        builder->add(jobId, pendingJob.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  Node agency = createAgencyFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();

  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  Mock<Job> spy(moveShard);
  Fake(Method(spy, abort));

  Job& spyMoveShard = spy.get();
  spyMoveShard.run();

  Verify(Method(spy, abort));
}

SECTION("if the collection was dropped while moving finish the job") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob("BOGUS", SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  Node agency = createAgencyFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  Mock<Job> spy(moveShard);
  Fake(Method(spy, finish));

  Job& spyMoveShard = spy.get();
  spyMoveShard.run();

  Verify(Method(spy, finish).Matching([](std::string const& server, std::string const& shard, bool success, std::string const& reason){
    return success;
  }));
}

SECTION("if the collection was dropped before the job could be started just finish the job") {
  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder && !(path == "/arango/Plan/Collections/" + DATABASE && it.key.copyString() == COLLECTION)) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Target/ToDo") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob("ANUNKNOWNCOLLECTION", SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  Node agency = createAgencyFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  Mock<Job> spy(moveShard);
  Fake(Method(spy, finish));

  Job& spyMoveShard = spy.get();
  spyMoveShard.start();

  Verify(Method(spy, finish).Matching([](std::string const& server, std::string const& shard, bool success, std::string const& reason){return success;}));

}

SECTION("the job should wait until the planned shard situation has been created in Current") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  Node agency = createAgencyFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  // should not write anything because we are not yet in sync
  AgentInterface& agent = mockAgent.get();

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
}

SECTION("if the job is done it should properly finish itself") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  Node agency = createAgencyFromBuilder(*builder);

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Target/Pending/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Finished/1").typeName()) == "object");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).toJson() == "[\"leader\",\"free\"]");
    CHECK(writes.get("/arango/Supervision/Shards/" + SHARD).get("op").copyString() == "delete");
    CHECK(writes.get("/arango/Supervision/DBServers/" + FREE_SERVER).get("op").copyString() == "delete");

    auto preconditions = q->slice()[0][1];
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").length() == 3);

    return fakeWriteResult;
  });
  AgentInterface& agent = mockAgent.get();

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
  Verify(Method(mockAgent,write));
}

SECTION("the job should not finish itself when only parts of distributeShardsLike have adapted") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Current/Collections/" + DATABASE) {
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
              builder->add(VPackValue(SHARD_FOLLOWER1));
              builder->add(VPackValue(FREE_SERVER));
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
              builder->add(VPackValue(FREE_SERVER));
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
              builder->add(VPackValue(FREE_SERVER));
            }
          }
        }
        builder->add(VPackValue("unrelatedcollection"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue("shards"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("unrelatedshard"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
            }
          }
        }
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  // nothing should happen...child shards not yet in sync
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
}

SECTION("the job should finish when all distributeShardsLike shards have adapted") {
  std::function<std::unique_ptr<VPackBuilder>(VPackSlice const&, std::string const&)> createTestStructure = [&](VPackSlice const& s, std::string const& path) {
    std::unique_ptr<VPackBuilder> builder;
    builder.reset(new VPackBuilder());
    if (s.isObject()) {
      builder->add(VPackValue(VPackValueType::Object));
      for (auto const& it: VPackObjectIterator(s)) {
        auto childBuilder = createTestStructure(it.value, path + "/" + it.key.copyString());
        if (childBuilder && it.key.copyString() != COLLECTION && path != "/arango/Current/Collections/" + DATABASE) {
          builder->add(it.key.copyString(), childBuilder->slice());
        }
      }

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_FOLLOWER1, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Current/Collections/" + DATABASE) {
        builder->add(VPackValue(COLLECTION));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue(SHARD));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("servers"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
              builder->add(VPackValue(FREE_SERVER));
            }
          }
        }
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
              builder->add(VPackValue(SHARD_FOLLOWER1));
              builder->add(VPackValue(FREE_SERVER));
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
              builder->add(VPackValue(FREE_SERVER));
            }
          }
        }
      } else if (path == "/arango/Plan/Collections/" + DATABASE) {
        builder->add(VPackValue(COLLECTION));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue("shards"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue(SHARD));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
              builder->add(VPackValue(FREE_SERVER));
            }
          }
        }
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
              builder->add(VPackValue(FREE_SERVER));
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
              builder->add(VPackValue(FREE_SERVER));
            }
          }
        }
        builder->add(VPackValue("unrelatedcollection"));
        {
          VPackObjectBuilder f(builder.get());
          builder->add(VPackValue("shards"));
          {
            VPackObjectBuilder f(builder.get());
            builder->add(VPackValue("unrelatedshard"));
            {
              VPackArrayBuilder g(builder.get());
              builder->add(VPackValue(SHARD_LEADER));
              builder->add(VPackValue(SHARD_FOLLOWER1));
            }
          }
        }
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue(1));
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue(1));
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Target/Pending/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Finished/1").typeName()) == "object");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).toJson() == "[\"leader\",\"free\"]");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/linkedcollection1/shards/linkedshard1").toJson() == "[\"leader\",\"free\"]");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/linkedcollection2/shards/linkedshard2").toJson() == "[\"leader\",\"free\"]");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/unrelatedcollection/shards/unrelatedshard").isNone());
    CHECK(writes.get("/arango/Supervision/Shards/linkedshard1").isNone());

    auto preconditions = q->slice()[0][1];
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").length() == 3);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/linkedcollection1/shards/linkedshard1").get("old").length() == 3);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/linkedcollection2/shards/linkedshard2").get("old").length() == 3);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/unrelatedcollection/shards/unrelatedshard").isNone());

    return fakeWriteResult;
  });
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
  Verify(Method(mockAgent,write));
}

SECTION("a moveshard job that just made it to ToDo can simply be aborted") {
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
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      }
      builder->close();
    } else {
      builder->add(s);
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Target/ToDo/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Finished/1").typeName()) == "object");

    return fakeWriteResult;
  });

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, TODO, jobId);
  moveShard.abort();
  Verify(Method(mockAgent,write));
}

SECTION("a pending moveshard job should also put the original server back into place when aborted") {
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

      if (path == "/arango/Target/Pending") {
        builder->add(jobId, createJob(COLLECTION, SHARD_LEADER, FREE_SERVER).slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("1"));
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("1"));
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Target/Pending/1").get("op").copyString() == "delete");
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    CHECK(writes.get("/arango/Supervision/DBServers/" + FREE_SERVER).get("op").copyString() == "delete");
    CHECK(writes.get("/arango/Supervision/Shards/" + SHARD).get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName()) == "array");
    // apparently we are not cleaning up our mess. this is done somewhere else :S (>=2)
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() >= 2);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == SHARD_LEADER);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[1].copyString() == SHARD_FOLLOWER1);
    CHECK(std::string(writes.get("/arango/Target/Failed/1").typeName()) == "object");

    return fakeWriteResult;
  });

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.abort();
  Verify(Method(mockAgent,write));
}

SECTION("after the new leader has synchronized the new leader should resign") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_LEADER, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("1"));
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("1"));
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName()) == "array");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() == 3);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == "_" + SHARD_LEADER);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[1].copyString() == SHARD_FOLLOWER1);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[2].copyString() == FREE_SERVER);

    REQUIRE(q->slice()[0].length() == 2);
    auto preconditions = q->slice()[0][1];
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName() == "object");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").typeName() == "array");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").length() == 3);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[0].copyString() == SHARD_LEADER);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[1].copyString() == SHARD_FOLLOWER1);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[2].copyString() == FREE_SERVER);
    return fakeWriteResult;
  });

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
  Verify(Method(mockAgent,write));
}

SECTION("when the old leader is not yet ready for resign nothing should happen") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_LEADER, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("1"));
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("1"));
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue("_" + SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  // nothing should happen so nothing should be called
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
}

SECTION("aborting the job while a leader transition is in progress (for example when job is timing out) should make the old leader leader again") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_LEADER, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("1"));
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("1"));
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue("_" + SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(writes.get("/arango/Target/Pending/1").get("op").copyString() == "delete");
    REQUIRE(q->slice()[0].length() == 1); // we always simply override! no preconditions...
    CHECK(writes.get("/arango/Supervision/DBServers/" + FREE_SERVER).get("op").copyString() == "delete");
    CHECK(writes.get("/arango/Supervision/Shards/" + SHARD).get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName()) == "array");
    // well apparently this job is not responsible to cleanup its mess
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() >= 2);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == SHARD_LEADER);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[1].copyString() == SHARD_FOLLOWER1);
    CHECK(std::string(writes.get("/arango/Target/Failed/1").typeName()) == "object");

    return fakeWriteResult;
  });
  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.abort();
  Verify(Method(mockAgent,write));
}

SECTION("if we are ready to resign the old server then finally move to the new leader") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_LEADER, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("1"));
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("1"));
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue("_" + SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue("_" + SHARD_LEADER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->add(VPackValue(FREE_SERVER));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(std::string(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName()) == "array");
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).length() == 2);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[0].copyString() == FREE_SERVER);
    CHECK(writes.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD)[1].copyString() == SHARD_FOLLOWER1);

    REQUIRE(q->slice()[0].length() == 2);
    auto preconditions = q->slice()[0][1];
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName() == "object");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").typeName() == "array");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").length() == 3);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[0].copyString() == "_" + SHARD_LEADER);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[1].copyString() == SHARD_FOLLOWER1);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[2].copyString() == FREE_SERVER);
    return fakeWriteResult;
  });

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
  Verify(Method(mockAgent,write));
}

SECTION("if the new leader took over finish the job") {
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

      if (path == "/arango/Target/Pending") {
        VPackBuilder pendingJob;
        {
          VPackObjectBuilder b(&pendingJob);
          auto plainJob = createJob(COLLECTION, SHARD_LEADER, FREE_SERVER);
          for (auto const& it: VPackObjectIterator(plainJob.slice())) {
            pendingJob.add(it.key.copyString(), it.value);
          }
          pendingJob.add("timeCreated", VPackValue(timepointToString(std::chrono::system_clock::now())));
        }
        builder->add(jobId, pendingJob.slice());
      } else if (path == "/arango/Supervision/DBServers") {
        builder->add(FREE_SERVER, VPackValue("1"));
      } else if (path == "/arango/Supervision/Shards") {
        builder->add(SHARD, VPackValue("1"));
      }
      builder->close();
    } else {
      if (path == "/arango/Current/Collections/" + DATABASE + "/" + COLLECTION + "/" + SHARD + "/servers") {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(FREE_SERVER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->close();
      } else if (path == "/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD) {
        builder->add(VPackValue(VPackValueType::Array));
        builder->add(VPackValue(FREE_SERVER));
        builder->add(VPackValue(SHARD_FOLLOWER1));
        builder->close();
      } else {
        builder->add(s);
      }
    }
    return builder;
  };

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, waitFor)).AlwaysReturn();
  When(Method(mockAgent, write)).Do([&](query_t const& q) -> write_ret_t {
    INFO("WriteTransaction: " << q->slice().toJson());
    auto writes = q->slice()[0][0];
    CHECK(writes.length() == 4);
    CHECK(writes.get("/arango/Target/Pending/1").get("op").copyString() == "delete");
    CHECK(std::string(writes.get("/arango/Target/Finished/1").typeName()) == "object");
    CHECK(writes.get("/arango/Supervision/DBServers/" + FREE_SERVER).get("op").copyString() == "delete");
    CHECK(writes.get("/arango/Supervision/Shards/" + SHARD).get("op").copyString() == "delete");

    REQUIRE(q->slice()[0].length() == 2);
    auto preconditions = q->slice()[0][1];
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).typeName() == "object");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").typeName() == "array");
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old").length() == 2);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[0].copyString() == FREE_SERVER);
    CHECK(preconditions.get("/arango/Plan/Collections/" + DATABASE + "/" + COLLECTION + "/shards/" + SHARD).get("old")[1].copyString() == SHARD_FOLLOWER1);
    return fakeWriteResult;
  });

  AgentInterface& agent = mockAgent.get();

  auto builder = createTestStructure(baseStructure.toBuilder().slice(), "");
  REQUIRE(builder);
  Node agency = createAgencyFromBuilder(*builder);

  INFO("Agency: " << agency);
  auto moveShard = MoveShard(agency, &agent, PENDING, jobId);
  moveShard.run();
  Verify(Method(mockAgent,write));
}

SECTION("calling an unknown job should be possible without throwing exceptions or so") {
  Mock<AgentInterface> mockAgent;
  AgentInterface& agent = mockAgent.get();
  Node agency = createAgencyFromBuilder(baseStructure.toBuilder());
  INFO("Agency: " << agency);

  CHECK_NOTHROW(MoveShard(agency, &agent, PENDING, "666"));
}

}
}
}
}
