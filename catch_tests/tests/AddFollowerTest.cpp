#include "catch.hpp"
#include "fakeit.hpp"

#include "Agency/AddFollower.h"
#include "Agency/AgentConfiguration.h"
#include "Agency/Agent.h"
#include "Agency/AgentInterface.h"
#include "Agency/Node.h"

#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

using namespace arangodb;
using namespace arangodb::consensus;
using namespace fakeit;

TEST_CASE( "Add follower", "[agency]" ) {
  Node node("ROOT");

  std::string prefix {"/arango"};
  std::string database {"database"};
  std::string collection {"collection"};
  std::string shard {"shard"};

  VPackBuilder builder;
  {
    VPackObjectBuilder a(&builder);
    builder.add(VPackValue("new"));
    {
      VPackObjectBuilder b(&builder);
      builder.add(VPackValue("Target"));
      {
        VPackObjectBuilder c(&builder);
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
                }
              }
            }
          }
        }
      }
    }
  }
  node.handle<SET>(builder.slice());

  write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};

  Mock<AgentInterface> mockAgent;
  When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
    REQUIRE(1 == 1);
    LOG_TOPIC(WARN, Logger::AGENCY) << "CHECKUNG " << q->toJson();
    return fakeWriteResult;
  });
  When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);

  AgentInterface &agent = mockAgent.get();
  auto addFollower = AddFollower(
      node,
      &agent,
      "1",
      "unittest",
      prefix,
      database,
      collection,
      shard,
      {"newfollower"}
      );

  //auto status = addFollower.status();
  REQUIRE(1 == 1);
}
