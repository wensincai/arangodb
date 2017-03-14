////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2016 ArangoDB GmbH, Cologne, Germany
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
/// @author Kaveh Vahedipour
////////////////////////////////////////////////////////////////////////////////

#include "FailedLeader.h"

#include "Agency/Agent.h"
#include "Agency/Job.h"
#include "Agency/JobContext.h"

#include <algorithm>
#include <vector>

using namespace arangodb::consensus;

FailedLeader::FailedLeader(Node const& snapshot, AgentInterface* agent,
                           std::string const& jobId, std::string const& creator,
                           std::string const& database,
                           std::string const& collection,
                           std::string const& shard, std::string const& from)
    : Job(NOTFOUND, snapshot, agent, jobId, creator),
      _database(database),
      _collection(collection),
      _shard(shard),
      _from(from) {}

FailedLeader::FailedLeader(Node const& snapshot, AgentInterface* agent,
                           JOB_STATUS status, std::string const& jobId)
    : Job(status, snapshot, agent, jobId) {
  // Get job details from agency:
  try {
    std::string path = pos[status] + _jobId + "/";
    _database = _snapshot(path + "database").getString();
    _collection = _snapshot(path + "collection").getString();
    _from = _snapshot(path + "fromServer").getString();
    try {
      // set only if already started
      _to = _snapshot(path + "toServer").getString();
    } catch (...) {}
    _shard = _snapshot(path + "shard").getString();
    _creator = _snapshot(path + "creator").getString();
  } catch (std::exception const& e) {
    std::stringstream err;
    err << "Failed to find job " << _jobId << " in agency: " << e.what();
    LOG_TOPIC(ERR, Logger::SUPERVISION) << err.str();
    finish("", _shard, false, err.str());
    _status = FAILED;
  }
}

FailedLeader::~FailedLeader() {}

void FailedLeader::run() {
  runHelper("", _shard);
}

bool FailedLeader::create(std::shared_ptr<VPackBuilder> b) {

  using namespace std::chrono;
  LOG_TOPIC(INFO, Logger::SUPERVISION)
    << "Create failedLeader for " + _shard + " from " + _from;
  
  _jb = std::make_shared<Builder>();
  { VPackArrayBuilder transaction(_jb.get());
    { VPackObjectBuilder operations(_jb.get());
      // Todo entry
      _jb->add(VPackValue(toDoPrefix + _jobId));
      { VPackObjectBuilder todo(_jb.get());
        _jb->add("creator", VPackValue(_creator));
        _jb->add("type", VPackValue("failedLeader"));
        _jb->add("database", VPackValue(_database));
        _jb->add("collection", VPackValue(_collection));
        _jb->add("shard", VPackValue(_shard));
        _jb->add("fromServer", VPackValue(_from));
        _jb->add("jobId", VPackValue(_jobId));
        _jb->add(
          "timeCreated", VPackValue(timepointToString(system_clock::now())));
      }}}
  
  write_ret_t res = transact(_agent, *_jb);
  
  return (res.accepted && res.indices.size() == 1 && res.indices[0]);
  
}

bool FailedLeader::start() {

  std::vector<std::string> existing =
    _snapshot.exists(planColPrefix + _database + "/" + _collection + "/" +
                     "distributeShardsLike");
  
  // Fail if got distributeShardsLike
  if (existing.size() == 5) {
    finish("", _shard, false, "Collection has distributeShardsLike");
  }
  // Fail if collection gone
  else if (existing.size() < 4) { 
    finish("", _shard, true, "Collection " + _collection + " gone");
  }

  // Get healthy in Sync follower common to all prototype + clones
  auto commonHealthyInSync =
    findCommonHealthyInSyncFollower(_snapshot, _database, _collection, _shard);
  if (commonHealthyInSync.empty()) {
    return false;
  } else {
    _to = commonHealthyInSync;
  }

  LOG_TOPIC(INFO, Logger::SUPERVISION)
    << "Start failedLeader for " + _shard + " from " + _from + " to " + _to;  
  
  using namespace std::chrono;

  // Current servers vector
  auto const& current =
    _snapshot(
      curColPrefix + _database + "/" + _collection + "/" + _shard + "/servers")
    .slice();
  // Planned servers vector
  auto const& planned =
    _snapshot(
      planColPrefix + _database + "/" + _collection + "/shards/" + _shard)
    .slice();

  // Get todo entry
  Builder todo;
  { VPackArrayBuilder t(&todo);
    if (_jb == nullptr) {
      try {
        _snapshot(toDoPrefix + _jobId).toBuilder(todo);
      } catch (std::exception const&) {
        LOG_TOPIC(INFO, Logger::SUPERVISION)
          << "Failed to get key " + toDoPrefix + _jobId
          + " from agency snapshot";
        return false;
      }
    } else {
      todo.add(_jb->slice()[0].get(toDoPrefix + _jobId));
    }}

  // New plan vector excluding _to and _from
  std::vector<std::string> planv;
  for (auto const& i : VPackArrayIterator(planned)) {
    auto s = i.copyString();
    if (s != _from && s != _to) {
      planv.push_back(s);
    }
  }

  // Additional follower, if applicable
  auto additionalFollower = randomIdleGoodAvailableServer(_snapshot, planned);
  if (!additionalFollower.empty()) {
    planv.push_back(additionalFollower);
  }

  // Transactions
  auto pending = std::make_shared<Builder>();
  
  { VPackArrayBuilder transactions(pending.get());
    
    { VPackArrayBuilder stillThere(pending.get()); // Collection still there?
      pending->add(
        VPackValue(
          agencyPrefix + planColPrefix + _database + "/" + _collection));}
    
    { VPackArrayBuilder stillThere(pending.get()); // Still failing?
      pending->add(VPackValue(agencyPrefix + healthPrefix + _from + "/Status"));}
    
    { VPackArrayBuilder transaction(pending.get());
      
      // Operations ----------------------------------------------------------
      { VPackObjectBuilder operations(pending.get());
        // Add pending entry
        pending->add(VPackValue(agencyPrefix + pendingPrefix + _jobId));
        { VPackObjectBuilder ts(pending.get());
          pending->add("timeStarted", // start
                       VPackValue(timepointToString(system_clock::now())));
          pending->add("toServer", VPackValue(_to)); // toServer
          for (auto const& obj : VPackObjectIterator(todo.slice()[0])) {
            pending->add(obj.key.copyString(), obj.value);
          }
        }
        // Remove todo entry ------
        pending->add(VPackValue(agencyPrefix + toDoPrefix + _jobId));
        { VPackObjectBuilder rem(pending.get());
          pending->add("op", VPackValue("delete")); }
        // DB server vector -------
        Builder ns;
        { VPackArrayBuilder servers(&ns);
          ns.add(VPackValue(_to));  
          for (auto const& i : VPackArrayIterator(current)) {
            std::string s = i.copyString();
            if (s != _from && s != _to) {
              ns.add(i);
              planv.erase(
                std::remove(planv.begin(), planv.end(), s), planv.end());
            }
          }
          ns.add(VPackValue(_from));
          for (auto const& i : planv) {
            ns.add(VPackValue(i));
          }
        }
        for (auto const& clone :
               clones(_snapshot, _database, _collection, _shard)) {
          pending->add(
            agencyPrefix + planColPrefix + _database + "/"
            + clone.collection + "/shards/" + clone.shard, ns.slice());
        }
        // Block shard ------------
        pending->add(VPackValue(agencyPrefix + blockedShardsPrefix + _shard));
        { VPackObjectBuilder block(pending.get());
          pending->add("jobId", VPackValue(_jobId)); }
        // Increment Plan/Version -
        pending->add(VPackValue(agencyPrefix + planVersion));
        { VPackObjectBuilder version(pending.get());
          pending->add("op", VPackValue("increment")); }} // Operations ------
      // Preconditions -------------------------------------------------------
      { VPackObjectBuilder preconditions(pending.get());
       // Collection should not have been deleted in the mt
        pending->add( 
          VPackValue(
            agencyPrefix + planColPrefix + _database + "/" + _collection));
        { VPackObjectBuilder stillExists(pending.get());
          pending->add("oldEmpty", VPackValue(false)); }
        // Status should still be failed
        pending->add( 
          VPackValue(agencyPrefix + healthPrefix + _from + "/Status"));
        { VPackObjectBuilder stillExists(pending.get());
          pending->add("old", VPackValue("FAILED")); }

      } // Preconditions -----------------------------------------------------
    }}
  
  // Abort job blocking server if abortable
  try {
    std::string jobId = _snapshot(blockedServersPrefix + _from).getString();
    if (!abortable(_snapshot, jobId)) {
      return false;
    } else {
      JobContext(PENDING, jobId, _snapshot, _agent).abort();
    }
  } catch (...) {}
  
  LOG_TOPIC(DEBUG, Logger::SUPERVISION)
    << "FailedLeader transaction: " << pending->toJson();
  
  trans_ret_t res = _agent->transact(pending);

  LOG_TOPIC(DEBUG, Logger::SUPERVISION)
    << "FailedLeader result: " << pending->toJson();
  
  try {
    auto exist = res.result->slice()[0].get(
      std::vector<std::string>(
        {"arango", "Plan", "Collections", _database, _collection}
        )).isObject();
    if (!exist) {
      finish("", _shard, false, "Collection " + _collection + " gone");
    }
  } catch (std::exception const& e) {
    LOG_TOPIC(ERR, Logger::SUPERVISION)
      << "Failed to acquire find " << _from << " in job IDs from agency: "
      << e.what() << __FILE__ << __LINE__; 
  }
  
  try {
    auto state = res.result->slice()[1].get(
      std::vector<std::string>(
        {"arango", "Supervision", "Health", _from, "Status"})).copyString();
    if (state != "FAILED") {
      finish("", _shard, false, _from + " is no longer 'FAILED'");
    }
  } catch (std::exception const& e) {
    LOG_TOPIC(ERR, Logger::SUPERVISION)
      << "Failed to acquire find " << _from << " in job IDs from agency: "
      << e.what() << __FILE__ << __LINE__; 
  }
  
  return (res.accepted && res.result->slice()[2].getUInt());
  
}

JOB_STATUS FailedLeader::status() {

  if(!_snapshot.has(planColPrefix + _database + "/" + _collection)) {
    finish("", _shard, true, "Collection " + _collection + " gone");
    return FINISHED;
  }

  if (_status != PENDING) {
    return _status;
  }

  Node const& job = _snapshot(pendingPrefix + _jobId);
  std::string database = job("database").toJson(),
    collection = job("collection").toJson(),
    shard = job("shard").toJson();
  
  bool done = false;
  for (auto const& clone : clones(_snapshot, _database, _collection, _shard)) {
    auto sub = database + "/" + clone.collection;
    if(_snapshot(planColPrefix + sub + "/shards/" + clone.shard).slice()[0] !=
       _snapshot(curColPrefix + sub + "/" + clone.shard + "/servers").slice()[0]) {
      LOG_TOPIC(DEBUG, Logger::SUPERVISION)
        << "FailedLeader waiting for " << sub + "/" + shard;
      break;
    }
    done = true;
  }
  
  if (done) {
    // Remove shard to /arango/Target/FailedServers/<server> array
    Builder del;
    { VPackArrayBuilder a(&del);
      { VPackObjectBuilder o(&del);
        del.add(VPackValue(failedServersPrefix + "/" + _from));
        { VPackObjectBuilder erase(&del);
          del.add("op", VPackValue("erase"));
          del.add("val", VPackValue(_shard));
        }}}
    
    write_ret_t res = transact(_agent, del);
    if (finish("", shard)) {
      LOG_TOPIC(INFO, Logger::SUPERVISION)
        << "Finished failedLeader for " + _shard + " from " + _from + " to " + _to;  
        return FINISHED;
    }
  }
  
  return _status;
}


arangodb::Result FailedLeader::abort() {

  Builder builder;
  arangodb::Result result;

  { VPackArrayBuilder a(&builder);      
    // Oper: Delete job from todo ONLY!
    { VPackObjectBuilder oper(&builder);
      builder.add(VPackValue(toDoPrefix + _jobId));
      { VPackObjectBuilder del(&builder);
        builder.add("op", VPackValue("delete")); }}
    // Precond: Just so that we can report?
    { VPackObjectBuilder prec(&builder);
      builder.add(VPackValue(toDoPrefix + _jobId));
      { VPackObjectBuilder old(&builder);
        builder.add("oldEmpty", VPackValue(false)); }}
  }

  auto ret = transact(_agent, builder);

  if (!ret.accepted) {
    result = arangodb::Result(TRI_ERROR_SUPERVISION_GENERAL_FAILURE, "Lost leadership.");
  } else if (ret.indices[0] == 0) {
    result = arangodb::Result(
      1, std::string("Cannot abort failedLeader job ")
      + _jobId + " beyond todo stage");
  }
  
  return result;
  
}


