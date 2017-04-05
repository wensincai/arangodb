////////////////////////////////////////////////////////////////////////////////
/// @brief test case for FailedServer job
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
/// @author Kaveh Vahedipour
/// @author Copyright 2017, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////
#include "catch.hpp"
#include "fakeit.hpp"

#include "Agency/AddFollower.h"
#include "Agency/FailedServer.h"
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
namespace failed_server_test {

const std::string PREFIX = "/arango";
const std::string DATABASE = "database";
const std::string COLLECTION = "collection";
const std::string SHARD = "shard";
const std::string SHARD_LEADER = "leader";
const std::string SHARD_FOLLOWER1 = "follower1";
const std::string SHARD_FOLLOWER2 = "follower2";
const std::string FREE_SERVER = "free";
const std::string FREE_SERVER2 = "free2";

typedef std::function<std::unique_ptr<Builder>(
  Slice const&, std::string const&)>TestStructureType;

const char *agency =
#include "FailedServerTest.json"
;

VPackBuilder createJob() {
  VPackBuilder builder;
  VPackObjectBuilder a(&builder);
  {
    builder.add("creator", VPackValue("1"));
    builder.add("type", VPackValue("failedServer"));
    builder.add("database", VPackValue("database"));
    builder.add("collection", VPackValue("collection"));
    builder.add("shard", VPackValue("shard"));
    builder.add("fromServer", VPackValue("follower1"));
    builder.add("jobId", VPackValue("1"));
    builder.add("timeCreated",
                VPackValue(timepointToString(std::chrono::system_clock::now())));
  }
  return builder;
}

Node createNodeFromBuilder(VPackBuilder const& builder) {

  VPackBuilder opBuilder;
  { VPackObjectBuilder a(&opBuilder);
    opBuilder.add("new", builder.slice()); }
  
  Node node("");
  node.handle<SET>(opBuilder.slice());
  return node;

}

Builder createBuilder(char const* c) {

  VPackOptions options;
  options.checkAttributeUniqueness = true;
  VPackParser parser(&options);
  parser.parse(c);
  
  VPackBuilder builder;
  builder.add(parser.steal()->slice());
  return builder;
  
}

Node createNode(char const* c) {
  return createNodeFromBuilder(createBuilder(c));
}

Node createRootNode() {
  return createNode(agency);
}


TEST_CASE("FailedServer", "[agency][supervision]") {

  auto transBuilder = std::make_shared<Builder>();
  { VPackArrayBuilder a(transBuilder.get());
    transBuilder->add(VPackValue((uint64_t)1)); }
    
  auto agency = createRootNode();
  write_ret_t fakeWriteResult {true, "", std::vector<bool> {true}, std::vector<index_t> {1}};
  trans_ret_t fakeTransResult {true, "", 1, 0, transBuilder};
  
  SECTION("creating a job should create a job in todo") {
    Mock<AgentInterface> mockAgent;
    
    std::string jobId = "1";
    When(Method(mockAgent, write)).AlwaysDo([&](query_t const& q) -> write_ret_t {
        INFO(q->slice().toJson());
        auto expectedJobKey = PREFIX + toDoPrefix + jobId;
        REQUIRE(std::string(q->slice().typeName()) == "array" );
        REQUIRE(q->slice().length() == 1);
        REQUIRE(std::string(q->slice()[0].typeName()) == "array");
        REQUIRE(q->slice()[0].length() == 2); // we always simply override! no preconditions...
        REQUIRE(std::string(q->slice()[0][0].typeName()) == "object");
        REQUIRE(q->slice()[0][0].length() == 2); // should ONLY do an entry in todo
        std::cout << expectedJobKey << std::endl;
        REQUIRE(std::string(q->slice()[0][0].get(expectedJobKey).typeName()) == "object");
        
        auto job = q->slice()[0][0].get(expectedJobKey);
        REQUIRE(std::string(job.get("creator").typeName()) == "string");
        REQUIRE(std::string(job.get("type").typeName()) == "string");
        CHECK(job.get("type").copyString() == "failedServer");
        REQUIRE(std::string(job.get("server").typeName()) == "string");
        CHECK(job.get("server").copyString() == SHARD_LEADER);
        CHECK(std::string(job.get("jobId").typeName()) == "string");
        CHECK(std::string(job.get("timeCreated").typeName()) == "string");

        return fakeWriteResult;
      });
    When(Method(mockAgent, waitFor)).AlwaysReturn(AgentInterface::raft_commit_t::OK);
    AgentInterface &agent = mockAgent.get();

    Builder builder = agency.toBuilder();
    std::cout << builder.toJson() << std::endl;
    FailedServer(agency(PREFIX), &agent, jobId, "unittest",SHARD_LEADER).create();
    Verify(Method(mockAgent, write));
    
  } // SECTION

} // TEST_CASE

}}} // namespaces 
