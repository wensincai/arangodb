#include "catch.hpp"
#include "fakeit.hpp"

#include "Agency/FailedLeader.h"
#include "Agency/AgentInterface.h"
#include "Agency/Node.h"
#include "lib/Basics/StringUtils.h"

#include <velocypack/Parser.h>
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
const std::string FROM = "from";
const std::string TO = "to";

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
                    builder.add(VPackValue(FROM));
                    builder.add(VPackValue(TO));
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
                    builder.add(VPackValue(FROM));
                    builder.add(VPackValue(TO));
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  root.handle<SET>(builder.slice());
  return root;
}

TEST_CASE( "FailedLeader should fill FailedServers with failed shards", "[agency, supervision]" ) {
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
    PREFIX,
    DATABASE,
    COLLECTION,
    SHARD,
    FROM,
    TO
  );
}

TEST_CASE("A FailedLeader job should be queued", "[agency, supervision]") {
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
    PREFIX,
    DATABASE,
    COLLECTION,
    SHARD,
    FROM,
    TO
  );
}

TEST_CASE("FailedLeader should set a new leader and set the new one as a follower", "[agency, supervision]") {
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
      PREFIX,
      DATABASE,
      COLLECTION,
      SHARD,
      FROM,
      TO
    );
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
    PREFIX,
    DATABASE,
    COLLECTION,
    SHARD,
    FROM,
    TO
  );
}
