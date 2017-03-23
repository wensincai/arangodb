'use strict';

// //////////////////////////////////////////////////////////////////////////////
// / @brief server initialization
// /
// / @file
// /
// / DISCLAIMER
// /
// / Copyright 2014 ArangoDB GmbH, Cologne, Germany
// / Copyright 2011-2014 triAGENS GmbH, Cologne, Germany
// /
// / Licensed under the Apache License, Version 2.0 (the "License")
// / you may not use this file except in compliance with the License.
// / You may obtain a copy of the License at
// /
// /     http://www.apache.org/licenses/LICENSE-2.0
// /
// / Unless required by applicable law or agreed to in writing, software
// / distributed under the License is distributed on an "AS IS" BASIS,
// / WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// / See the License for the specific language governing permissions and
// / limitations under the License.
// /
// / Copyright holder is ArangoDB GmbH, Cologne, Germany
// /
// / @author Dr. Frank Celler
// / @author Copyright 2014, ArangoDB GmbH, Cologne, Germany
// / @author Copyright 2011-2014, triAGENS GmbH, Cologne, Germany
// //////////////////////////////////////////////////////////////////////////////

// //////////////////////////////////////////////////////////////////////////////
// / @brief server start
// /
// / Note that all the initialization has been done. E. g. "upgrade-database.js"
// / has been executed.
// //////////////////////////////////////////////////////////////////////////////

(function () {
  var internal = require('internal');

  // in the cluster we run the startup scripts from elsewhere!

  // statistics can be turned off
  if (internal.enableStatistics && internal.threadNumber === 0) {
    require('@arangodb/statistics').startup();
  }

  // reload routing information
  internal.loadStartup('server/bootstrap/autoload.js').startup();
  internal.loadStartup('server/bootstrap/routing.js').startup();

  // startup the foxx manager once
  if (internal.threadNumber === 0) {
    require('@arangodb/foxx/manager')._startup(true);
    require('@arangodb/foxx/manager')._selfHeal();
  }

  // start the queue manager once
  if (internal.threadNumber === 0) {
    require('@arangodb/foxx/queues/manager').run();
  }

  // check available versions
  if (internal.threadNumber === 0 && internal.quiet !== true) {
    require('@arangodb').checkAvailableVersions();
  }

  return true;
}());
