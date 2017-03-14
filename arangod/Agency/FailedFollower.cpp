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

#include "FailedFollower.h"

#include "Agency/Agent.h"
#include "Agency/Job.h"
#include "Agency/JobContext.h"

using namespace arangodb::consensus;

FailedFollower::FailedFollower(Node const& snapshot, AgentInterface* agent,
                               std::string const& jobId,
                               std::string const& creator,
                               std::string const& database,
                               std::string const& collection,
                               std::string const& shard,
                               std::string const& from,
                               std::string const& to)
    : Job(NOTFOUND, snapshot, agent, jobId, creator),
      _database(database),
      _collection(collection),
      _shard(shard),
      _from(from),
      _to(to) {}

FailedFollower::FailedFollower(Node const& snapshot, AgentInterface* agent,
                               JOB_STATUS status, std::string const& jobId)
    : Job(status, snapshot, agent, jobId) {

  // Get job details from agency:
  try {
    std::string path = pos[status] + _jobId;
    _database = _snapshot(path + "database").getString();
    _collection = _snapshot(path + "collection").getString();
    _from = _snapshot(path + "fromServer").getString();
    try {
      // set only if already started
      _to = _snapshot(path + "toServer").getString();
    } catch (...) {}
    _shard = _snapshot(path + "shard").getString();
    _creator = _snapshot(path + "creator").slice().copyString();
  } catch (std::exception const& e) {
    std::stringstream err;
    err << "Failed to find job " << _jobId << " in agency: " << e.what();
    LOG_TOPIC(ERR, Logger::SUPERVISION) << err.str();
    finish("", _shard, false, err.str());
    _status = FAILED;
  }
}

FailedFollower::~FailedFollower() {}

void FailedFollower::run() {
  runHelper("", _shard);
}

bool FailedFollower::create(std::shared_ptr<VPackBuilder> envelope) {

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
        _jb->add("type", VPackValue("failedFollower"));
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


bool FailedFollower::start() {

  using namespace std::chrono;

  std::vector<std::string> existing =
    _snapshot.exists(planColPrefix + _database + "/" + _collection + "/" +
                     "distributeShardsLike");
  
  // Fail if got distributeShardsLike
  if (existing.size() == 5) {
    finish("", _shard, false, "Collection has distributeShardsLike");
  } else if (existing.size() < 4) {
    finish("", _shard, true, "Collection " + _collection + " gone");
  }

  auto const& planned =
    _snapshot(
      planColPrefix + _database + "/" + _collection + "/shards/" + _shard)
    .slice();

  // Copy todo to pending
  Builder todo;
  { VPackArrayBuilder a(&todo);
    if (_jb == nullptr) {
      try {
        _snapshot(toDoPrefix + _jobId).toBuilder(todo);
      } catch (std::exception const&) {
        LOG_TOPIC(INFO, Logger::SUPERVISION)
          << "Failed to get key " + toDoPrefix + _jobId + " from agency snapshot";
        return false;
      }
    } else {
    todo.add(_jb->slice().get(toDoPrefix + _jobId).valueAt(0));
    }}

  // Replace from by to in plan and that's it
  std::vector<std::string> planv;
  for (auto const& i : VPackArrayIterator(planned)) {
    auto s = i.copyString();
    planv.push_back((s != _from) ? s : _to);
  }

  std::string _to = randomIdleGoodAvailableServer(_snapshot, planv);
  if (_to.empty()) {
    return false;
  }

  // Transaction
  auto job = std::make_shared<Builder>();

  { VPackArrayBuilder transactions(job.get());
    
    { VPackArrayBuilder stillThere(job.get()); // Collection still there?
      job->add(
        VPackValue(
          agencyPrefix + planColPrefix + _database + "/" + _collection));}

    { VPackArrayBuilder stillThere(job.get()); // Still failing?
      job->add(VPackValue(agencyPrefix + healthPrefix + _from + "/Status"));}
    
    // Operations ----------------------------------------------------------
    { VPackArrayBuilder transaction(job.get());
      // Add finished entry
      job->add(VPackValue(agencyPrefix + finishedPrefix + _jobId));
      { VPackObjectBuilder ts(job.get());
        job->add("timeStarted", // start
                     VPackValue(timepointToString(system_clock::now())));
        job->add("timeFinished", // same same :)
                     VPackValue(timepointToString(system_clock::now())));
        job->add("toServer", VPackValue(_to)); // toServer
        for (auto const& obj : VPackObjectIterator(todo.slice()[0])) {
          job->add(obj.key.copyString(), obj.value);
        }
      }
      // Remove todo entry ------
      job->add(VPackValue(agencyPrefix + toDoPrefix + _jobId));
      { VPackObjectBuilder rem(job.get());
        job->add("op", VPackValue("delete")); }
      // DB server vector -------
      Builder ns;
      { VPackArrayBuilder servers(&ns);
        ns.add(VPackValue(_to));  
        for (auto const& i : planv) {
          ns.add(VPackValue(i));
        }
      }
      for (auto const& clone :
             clones(_snapshot, _database, _collection, _shard)) {
        job->add(
          agencyPrefix + planColPrefix + _database + "/"
          + clone.collection + "/shards/" + clone.shard, ns.slice());
      }
      // Preconditions -------------------------------------------------------
      { VPackObjectBuilder preconditions(job.get());
        // Collection should not have been deleted in the mt
        job->add( 
          VPackValue(
            agencyPrefix + planColPrefix + _database + "/" + _collection));
        { VPackObjectBuilder stillExists(job.get());
          job->add("oldEmpty", VPackValue(false)); }
        // Status should still be failed
        job->add( 
          VPackValue(agencyPrefix + healthPrefix + _from + "/Status"));
        { VPackObjectBuilder stillExists(job.get());
          job->add("old", VPackValue("FAILED")); }
        
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
    << "FailedLeader transaction: " << job->toJson();
  
  trans_ret_t res = _agent->transact(job);

  LOG_TOPIC(DEBUG, Logger::SUPERVISION)
    << "FailedLeader result: " << job->toJson();
  
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

JOB_STATUS FailedFollower::status() {
  if (_status != PENDING) {
    return _status;
  }
  // FIXME: this is no more needed because all was done in start()
  // FIXME: do not wait for in sync any more, so status == PENDING
  // FIXME: has nothing to do any more
  // FIXME: do we need a list of shards in FailedServers???

  Node const& job = _snapshot(pendingPrefix + _jobId);
  std::string database = job("database").toJson(),
              collection = job("collection").toJson(),
              shard = job("shard").toJson();

  std::string planPath = planColPrefix + database + "/" + collection +
                         "/shards/" + shard,
              curPath = curColPrefix + database + "/" + collection + "/" +
                        shard + "/servers";

  Node const& planned = _snapshot(planPath);
  Node const& current = _snapshot(curPath);

  if (compareServerLists(planned.slice(), current.slice())) {
    // Remove shard from /arango/Target/FailedServers/<server> array
    Builder del;
    del.openArray();
    del.openObject();
    std::string path = failedServersPrefix + "/" + _from;
    del.add(path, VPackValue(VPackValueType::Object));
    del.add("op", VPackValue("erase"));
    del.add("val", VPackValue(_shard));
    del.close();
    del.close();
    del.close();
    write_ret_t res = transact(_agent, del);

    if (finish("", shard)) {
      return FINISHED;
    }
  }

  return _status;
}

arangodb::Result FailedFollower::abort() {
  arangodb::Result result;
  return result;
  // FIXME: TO BE IMPLEMENTED
}

