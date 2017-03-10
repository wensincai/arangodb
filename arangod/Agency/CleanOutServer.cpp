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

#include "CleanOutServer.h"

#include "Agency/AgentInterface.h"
#include "Agency/Job.h"
#include "Agency/MoveShard.h"

using namespace arangodb::consensus;

CleanOutServer::CleanOutServer(Node const& snapshot, AgentInterface* agent,
                               std::string const& jobId,
                               std::string const& creator,
                               std::string const& server)
    : Job(NOTFOUND, snapshot, agent, jobId, creator), _server(id(server)) {}

CleanOutServer::CleanOutServer(Node const& snapshot, AgentInterface* agent,
                               JOB_STATUS status, std::string const& jobId)
    : Job(status, snapshot, agent, jobId) {
  // Get job details from agency:
  try {
    std::string path = pos[status] + _jobId + "/";
    _server = _snapshot(path + "server").getString();
    _creator = _snapshot(path + "creator").getString();
  } catch (std::exception const& e) {
    std::stringstream err;
    err << "Failed to find job " << _jobId << " in agency: " << e.what();
    LOG_TOPIC(ERR, Logger::SUPERVISION) << err.str();
    finish("DBServers/" + _server, false, err.str());
    _status = FAILED;
  }
}

CleanOutServer::~CleanOutServer() {}

void CleanOutServer::run() {
  runHelper("DBServers/" + _server);
}

JOB_STATUS CleanOutServer::status() {
  if (_status != PENDING) {
    return _status;
  }

  Node::Children const todos = _snapshot(toDoPrefix).children();
  Node::Children const pends = _snapshot(pendingPrefix).children();
  size_t found = 0;

  for (auto const& subJob : todos) {
    if (!subJob.first.compare(0, _jobId.size() + 1, _jobId + "-")) {
      found++;
    }
  }
  for (auto const& subJob : pends) {
    if (!subJob.first.compare(0, _jobId.size() + 1, _jobId + "-")) {
      found++;
    }
  }

  // FIXME: implement timeout here?

  if (found == 0) {
    // Put server in /Target/CleanedServers:
    Builder reportTrx;
    {
      VPackArrayBuilder guard(&reportTrx);
      {
        VPackObjectBuilder guard3(&reportTrx);
        reportTrx.add(VPackValue("/Target/CleanedServers"));
        {
          VPackObjectBuilder guard4(&reportTrx);
          reportTrx.add("op", VPackValue("push"));
          reportTrx.add("new", VPackValue(_server));
        }
      }
    }
    // Transact to agency
    write_ret_t res = transact(_agent, reportTrx);

    if (res.accepted && res.indices.size() == 1 && res.indices[0] != 0) {
      LOG_TOPIC(DEBUG, Logger::SUPERVISION) << "Have reported " << _server
                                      << " in /Target/CleanedServers";
    } else {
      LOG_TOPIC(ERR, Logger::SUPERVISION) << "Failed to report " << _server
                                     << " in /Target/CleanedServers";
    }

    if (finish("DBServers/" + _server)) {
      return FINISHED;
    }
  }

  return _status;
}

// Only through shrink cluster
bool CleanOutServer::create(std::shared_ptr<VPackBuilder> envelope) {

  LOG_TOPIC(DEBUG, Logger::SUPERVISION)
      << "Todo: Clean out server " + _server + " for shrinkage";

  bool selfCreate = (envelope == nullptr); // Do we create ourselves?

  if (selfCreate) {
    _jb = std::make_shared<Builder>();
  } else {
    _jb = envelope;
  }

  std::string path = toDoPrefix + _jobId;

  { VPackArrayBuilder guard(_jb.get());
    VPackObjectBuilder guard2(_jb.get());
    _jb->add(VPackValue(path));
    { VPackObjectBuilder guard3(_jb.get());
      _jb->add("type", VPackValue("cleanOutServer"));
      _jb->add("server", VPackValue(_server));
      _jb->add("jobId", VPackValue(_jobId));
      _jb->add("creator", VPackValue(_creator));
      _jb->add("timeCreated",
             VPackValue(timepointToString(std::chrono::system_clock::now())));
    }
  }

  _status = TODO;

  if (!selfCreate) {
    return true;
  }

  write_ret_t res = transact(_agent, *_jb);

  if (res.accepted && res.indices.size() == 1 && res.indices[0]) {
    return true;
  }

  _status = NOTFOUND;

  LOG_TOPIC(INFO, Logger::SUPERVISION) << "Failed to insert job " + _jobId;
  return false;
}

bool CleanOutServer::start() {
  // If anything throws here, the run() method catches it and finishes
  // the job.
 
  // Check if the server exists:
  if (!_snapshot.has(plannedServers + "/" + _server)) {
    finish("", false, "server does not exist as DBServer in Plan");
    return false;
  }

  // Check that the server is not locked:
  if (_snapshot.has(blockedServersPrefix + _server)) {
    LOG_TOPIC(DEBUG, Logger::SUPERVISION) << "server " << _server
      << " is currently locked, not starting CleanOutServer job " << _jobId;
    return false;
  }

  // Check that the server is in state "GOOD":
  std::string health = checkServerGood(_snapshot, _server);
  if (health != "GOOD") {
    LOG_TOPIC(DEBUG, Logger::SUPERVISION) << "server " << _server
      << " is currently " << health << ", not starting CleanOutServer job "
      << _jobId;
      return false;
  }

  // Check that _to is not in `Target/CleanedServers`:
  VPackBuilder cleanedServersBuilder;
  try {
    auto cleanedServersNode = _snapshot(cleanedPrefix);
    cleanedServersNode.toBuilder(cleanedServersBuilder);
  }
  catch (...) {
    // ignore this check
    cleanedServersBuilder.clear();
    {
      VPackArrayBuilder guard(&cleanedServersBuilder); 
    }
  }
  VPackSlice cleanedServers = cleanedServersBuilder.slice();
  if (cleanedServers.isArray()) {
    for (auto const& x : VPackArrayIterator(cleanedServers)) {
      if (x.isString() && x.copyString() == _server) {
        finish("", false, "server must not be in `Target/CleanedServers`");
        return false;
      }
    }
  }

  // Check that _to is not in `Target/FailedServers`:
  VPackBuilder failedServersBuilder;
  try {
    auto failedServersNode = _snapshot(failedServersPrefix);
    failedServersNode.toBuilder(failedServersBuilder);
  }
  catch (...) {
    // ignore this check
    failedServersBuilder.clear();
    { VPackObjectBuilder guard(&failedServersBuilder); 
    }
  }
  VPackSlice failedServers = failedServersBuilder.slice();
  if (failedServers.isObject()) {
    Slice found = failedServers.get(_server);
    if (!found.isNone()) {
      finish("", false, "server must not be in `Target/FailedServers`");
      return false;
    }
  }

  // Check if we can get things done in the first place
  if (!checkFeasibility()) {
    finish("", false, "server " + _server + " cannot be cleaned out");
    return false;
  }

  // Copy todo to pending
  Builder todo, pending;

  // Get todo entry
  { VPackArrayBuilder guard(&todo);
    // When create() was done with the current snapshot, then the job object
    // will not be in the snapshot under ToDo, but in this case we find it
    // in _jb:
    if (_jb == nullptr) {
      try {
        _snapshot(toDoPrefix + _jobId).toBuilder(todo);
      } catch (std::exception const&) {
        // Just in case, this is never going to happen, since we will only
        // call the start() method if the job is already in ToDo.
        LOG_TOPIC(INFO, Logger::SUPERVISION) << "Failed to get key " +
          toDoPrefix + _jobId + " from agency snapshot";
        return false;
      }
    } else {
      try {
        todo.add(_jb->slice()[0].get(toDoPrefix + _jobId));
      } catch (std::exception const& e) {
        // Just in case, this is never going to happen, since when _jb is
        // set, then the current job is stored under ToDo.
        LOG_TOPIC(WARN, Logger::SUPERVISION) << e.what() << ": " 
          << __FILE__ << ":" << __LINE__;
        return false;
      }
    }
  }

  // Enter pending, remove todo, block toserver
  { VPackArrayBuilder listOfTransactions(&pending);

    { VPackObjectBuilder objectForMutation(&pending);

      addPutJobIntoSomewhere(pending, "Pending", todo.slice()[0]);
      addRemoveJobFromSomewhere(pending, "ToDo", _jobId);

      addBlockServer(pending, _server, _jobId);

      // Schedule shard relocations
      if (!scheduleMoveShards()) {
        finish("", false, "Could not schedule MoveShard.");
        return false;
      }

    }  // mutation part of transaction done

    // Preconditions
    { VPackObjectBuilder objectForPrecondition(&pending);
      addPreconditionServerNotBlocked(pending, _server);
      addPreconditionServerGood(pending, _server);
      addPreconditionUnchanged(pending, failedServersPrefix, failedServers);
      addPreconditionUnchanged(pending, cleanedPrefix, cleanedServers);
    }
  }  // array for transaction done

  // Transact to agency
  write_ret_t res = transact(_agent, pending);

  if (res.accepted && res.indices.size() == 1 && res.indices[0]) {
    LOG_TOPIC(DEBUG, Logger::SUPERVISION) << "Pending: Clean out server "
      + _server;

    return true;
  }

  LOG_TOPIC(INFO, Logger::SUPERVISION)
      << "Precondition failed for starting job " + _jobId;

  return false;
}

bool CleanOutServer::scheduleMoveShards() {

  // Use create with builder to schedule jobs in same transaction
 
  std::vector<std::string> servers = availableServers(_snapshot);

  // Minimum 1 DB server must remain
  if (servers.size() == 1) {
    LOG_TOPIC(ERR, Logger::SUPERVISION) << "DB server " << _server
                                   << " is the last standing db server.";
    return false;
  }

  Node::Children const& databases = _snapshot("/Plan/Collections").children();
  size_t sub = 0;

  for (auto const& database : databases) {
    
    // Find shardsLike dependencies
    for (auto const& collptr : database.second->children()) {
      
      auto const& collection = *(collptr.second);
      
      try { // distributeShardsLike entry means we only follow
        if (collection("distributeShardsLike").slice().copyString() != "") {
          continue;
        }
      } catch (...) {}

      for (auto const& shard : collection("shards").children()) {
        
        int found = -1;
        VPackArrayIterator dbsit(shard.second->slice());

        // Only shards, which are affected
        int count = 0;
        for (auto const& dbserver : dbsit) {
          if (dbserver.copyString() == _server) {
            found = count;
            break;
          }
          count++;
        }
        if (found == -1) {
          continue;
        }

        // Only destinations, which are not already holding this shard
        for (auto const& dbserver : dbsit) {
          servers.erase(
            std::remove(servers.begin(), servers.end(), dbserver.copyString()),
            servers.end());
        }

        // Among those a random destination
        std::string toServer;
        if (servers.empty()) {
          LOG_TOPIC(DEBUG, Logger::SUPERVISION)
            << "No servers remain as target for MoveShard";
          return false;
        }

        // FIXME: use RandomGenerator here
        try {
          toServer = servers.at(rand() % servers.size());
        } catch (...) {
          LOG_TOPIC(ERR, Logger::SUPERVISION)
            << "Range error picking destination for shard " + shard.first;
        }

        // FIXME: check conditions: server healthy, not cleaned, not failed,
        // FIXME: is this doen in checkFeasibility?
        // FIXME: check shards being locked
        // FIXME: check servers being locked
        // FIXME: is it necessary to create all MoveShard jobs in one 
        // FIXME: transaction? Do we need to check the precondition that
        // FIXME: the server is not blocked?
        // FIXME: What if some MoveShard job does not work?

        // Schedule move
        MoveShard(_snapshot, _agent, _jobId + "-" + std::to_string(sub++),
                  _jobId, database.first, collptr.first,
                  shard.first, _server, toServer, found == 0).run();
        
      }
    }
  }

  return true;
}

bool CleanOutServer::checkFeasibility() {
  // TODO: Describe the following checks in design doc:
 
  std::vector<std::string> availServers;

  // Get servers from plan
  Node::Children const& dbservers = _snapshot("/Plan/DBServers").children();
  for (auto const& srv : dbservers) {
    availServers.push_back(srv.first);
  }

  // Remove cleaned from ist
  if (_snapshot.has("/Target/CleanedServers")) {
    for (auto const& srv :
           VPackArrayIterator(_snapshot("/Target/CleanedServers").slice())) {
      availServers.erase(
        std::remove(
          availServers.begin(), availServers.end(), srv.copyString()),
        availServers.end());
    }
  }

  // Minimum 1 DB server must remain
  if (availServers.size() == 1) {
    LOG_TOPIC(ERR, Logger::SUPERVISION)
      << "DB server " << _server << " is the last standing db server.";
    return false;
  }

  // Remaning after clean out
  uint64_t numRemaining = availServers.size() - 1;

  // Find conflictings collections
  uint64_t maxReplFact = 1;
  std::vector<std::string> tooLargeCollections;
  std::vector<uint64_t> tooLargeFactors;
  Node::Children const& databases = _snapshot("/Plan/Collections").children();
  for (auto const& database : databases) {
    for (auto const& collptr : database.second->children()) {
      try {
        uint64_t replFact = (*collptr.second)("replicationFactor").getUInt();
        if (replFact > numRemaining) {
          tooLargeCollections.push_back(collptr.first);
          tooLargeFactors.push_back(replFact);
        }
        if (replFact > maxReplFact) {
          maxReplFact = replFact;
        }
      } catch (...) {
      }
    }
  }

  // Report if problems exist
  if (maxReplFact > numRemaining) {
    std::stringstream collections;
    std::stringstream factors;

    for (auto const collection : tooLargeCollections) {
      collections << collection << " ";
    }
    for (auto const factor : tooLargeFactors) {
      factors << std::to_string(factor) << " ";
    }

    LOG_TOPIC(ERR, Logger::SUPERVISION)
        << "Cannot accomodate shards " << collections.str()
        << "with replication factors " << factors.str()
        << "after cleaning out server " << _server;
    return false;
  }

  return true;
}

void CleanOutServer::abort() {
  // TO BE IMPLEMENTED
}

