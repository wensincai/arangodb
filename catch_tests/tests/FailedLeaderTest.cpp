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

TEST_CASE( "Failed leader", "[agency, supervision]" ) {
  Node root("ROOT");

  std::string prefix {"arango"};
  std::string database {"database"};
  std::string collection {"collection"};
  std::string shard {"shard"};
  std::string from {"from"};
  std::string to {"to"};

  VPackBuilder builder;
  {
    VPackObjectBuilder a(&builder);
    builder.add(VPackValue("new"));
    {
      VPackObjectBuilder a(&builder);
      builder.add(VPackValue("arango"));
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
            builder.add(VPackValue(database));
            {
              VPackObjectBuilder e(&builder);
              builder.add(VPackValue(collection));
              {
                VPackObjectBuilder f(&builder);
                builder.add(VPackValue(shard));
                {
                  VPackObjectBuilder f(&builder);
                  builder.add(VPackValue("servers"));
                  {
                    VPackArrayBuilder g(&builder);
                    builder.add(VPackValue(from));
                    builder.add(VPackValue(to));
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
            builder.add(VPackValue(database));
            {
              VPackObjectBuilder e(&builder);
              builder.add(VPackValue(collection));
              {
                VPackObjectBuilder f(&builder);
                builder.add(VPackValue("shards"));
                {
                  VPackObjectBuilder f(&builder);
                  builder.add(VPackValue(shard));
                  {
                    VPackArrayBuilder g(&builder);
                    builder.add(VPackValue(from));
                    builder.add(VPackValue(to));
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

  auto prefixRoot = root.children().find("arango")->second;

  SECTION("FailedServers should be filled with a shards") {
    write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};

    Mock<AgentInterface> mockAgent;
    When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
      REQUIRE(q->slice().isArray() );
      REQUIRE(q->slice().length() > 0);
      REQUIRE(q->slice()[0].isArray() > 0);
      REQUIRE(q->slice()[0].length() > 0);
      REQUIRE(q->slice()[0][0].isObject() > 0);
      REQUIRE(q->slice()[0][0].get(prefix + "/Target/FailedServers/" + from).isObject());

      auto failedServers = q->slice()[0][0].get(prefix + "/Target/FailedServers/" + from);

      REQUIRE(failedServers.isObject());
      REQUIRE(failedServers.get("new").isString());
      REQUIRE(failedServers.get("new").copyString() == shard);
      REQUIRE(failedServers.get("op").isString());
      REQUIRE(failedServers.get("op").copyString() == "push");

      return fakeWriteResult;
    });
    When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

    AgentInterface &agent = mockAgent.get();
    auto failedLeader = FailedLeader(
        *prefixRoot,
        &agent,
        "1",
        "unittest",
        prefix,
        database,
        collection,
        shard,
        from,
        to
        );
  }

  SECTION("FailedServers should queue a new job") {
    write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};

    Mock<AgentInterface> mockAgent;
    When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
      REQUIRE(q->slice().isArray() );
      REQUIRE(q->slice().length() > 0);
      REQUIRE(q->slice()[0].isArray() > 0);
      REQUIRE(q->slice()[0].length() > 0);
      REQUIRE(q->slice()[0][0].isObject() > 0);
      REQUIRE(q->slice()[0][0].get(prefix + "/Target/ToDo/1").isObject());

      auto job = q->slice()[0][0].get(prefix + "/Target/ToDo/1");
      REQUIRE(job.get("collection").isString());
      REQUIRE(job.get("collection").copyString() == collection);
      REQUIRE(job.get("database").isString());
      REQUIRE(job.get("database").copyString() == database);
      REQUIRE(job.get("type").isString());
      REQUIRE(job.get("type").copyString() == "failedLeader");

      return fakeWriteResult;
    });
    When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

    AgentInterface &agent = mockAgent.get();
    auto failedLeader = FailedLeader(
        *prefixRoot,
        &agent,
        "1",
        "unittest",
        prefix,
        database,
        collection,
        shard,
        from,
        to
        );
  }

  SECTION("FailedLeader should remove itself from the Plan") {
    write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};
    // [{"/arango/Plan/Collections/_system/100058/shards/s100060":["PRMR-d7d9bf18-d90d-4d78-864d-f560f1f6a58c","PRMR-d214fbc8-d4a6-49e5-8b60-838d0f5b91ab","PRMR-d214fbc8-d4a6-49e5-8b60-838d0f5b91ab"],"/arango/Plan/Version":{"op":"increment"},"/arango/Supervision/Shards/s100060":{"jobId":"1-1"},"/arango/Target/Pending/1-1":{"collection":"100058","creator":"1","database":"_system","fromServer":"PRMR-d214fbc8-d4a6-49e5-8b60-838d0f5b91ab","isLeader":true,"jobId":"1-1","shard":"s100060","timeCreated":"2017-02-14T18:02:28Z","timeStarted":"2017-02-14T18:02:33Z","toServer":"PRMR-d7d9bf18-d90d-4d78-864d-f560f1f6a58c","type":"failedLeader"},"/arango/Target/ToDo/1-1":{"op":"delete"}},{"/arango/Current/Collections/_system/100058/s100060/servers":{"old":["PRMR-d214fbc8-d4a6-49e5-8b60-838d0f5b91ab","PRMR-d7d9bf18-d90d-4d78-864d-f560f1f6a58c"]},"/arango/Plan/Collections/_system/100058/shards/s100060":{"old":["PRMR-d214fbc8-d4a6-49e5-8b60-838d0f5b91ab","PRMR-d7d9bf18-d90d-4d78-864d-f560f1f6a58c"]},"/arango/Supervision/Shards/s100060":{"oldEmpty":true}}]
    Node copyRoot(root);
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
            copyRoot(keys).applieOp(i.value);
          } else {
            copyRoot(keys).applies(i.value);
          }
        }
        auto arangoNode = root.children().find("arango");

        return fakeWriteResult;
      });

      When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

      AgentInterface &agent = mockAgent.get();
      auto failedLeader = FailedLeader(
          *prefixRoot,
          &agent,
          "1",
          "unittest",
          prefix,
          database,
          collection,
          shard,
          from,
          to
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

        if (i.key.copyString() == prefix + "/Plan/Collections/" + database + "/" + collection + "/shards/" + shard) {
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
      copyRoot("arango"),
      &agent,
      "1",
      "unittest",
      prefix,
      database,
      collection,
      shard,
      from,
      to
    );
  }
}
