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
/// @author Michael Hackstein
////////////////////////////////////////////////////////////////////////////////

#include "NeighborsEnumerator.h"

#include "Basics/VelocyPackHelper.h"
#include "Graph/EdgeCursor.h"
#include "VocBase/Traverser.h"
#include "VocBase/TraverserCache.h"

using namespace arangodb;
using namespace arangodb::traverser;
using namespace arangodb::graph;

NeighborsEnumerator::NeighborsEnumerator(Traverser* traverser,
                                         VPackSlice const& startVertex,
                                         TraverserOptions* opts)
    : PathEnumerator(traverser, startVertex.copyString(), opts),
      _searchDepth(0) {
  StringRef vId = _traverser->traverserCache()->persistString(StringRef(startVertex));
  _allFound.insert(vId);
  _currentDepth.insert(vId);
  _iterator = _currentDepth.begin();
}

bool NeighborsEnumerator::next() {
  if (_isFirst) {
    _isFirst = false;
    if (_opts->minDepth == 0) {
      return true;
    }
  }

  if (_iterator == _currentDepth.end() || ++_iterator == _currentDepth.end()) {
    do {
      // This depth is done. Get next
      if (_opts->maxDepth == _searchDepth) {
        // We are finished.
        return false;
      }

      _lastDepth.swap(_currentDepth);
      _currentDepth.clear();
      StringRef v;
      for (auto const& nextVertex : _lastDepth) {
        auto callback = [&](StringRef const& edgeId, VPackSlice e, size_t& cursorId) {
          // Counting should be done in readAll
          if (_traverser->getSingleVertex(e, nextVertex, _searchDepth, v)) {
            StringRef otherId = _traverser->traverserCache()->persistString(v);
            if (_allFound.find(otherId) == _allFound.end()) {
              _currentDepth.emplace(otherId);
              _allFound.emplace(otherId);
            }
          }
        };
        std::unique_ptr<arangodb::graph::EdgeCursor> cursor(
            _opts->nextCursor(_traverser->mmdr(), nextVertex, _searchDepth));
        cursor->readAll(callback);
      }
      if (_currentDepth.empty()) {
        // Nothing found. Cannot do anything more.
        return false;
      }
      ++_searchDepth;
    } while (_searchDepth < _opts->minDepth);
    _iterator = _currentDepth.begin();
  }
  TRI_ASSERT(_iterator != _currentDepth.end());
  return true;
}

arangodb::aql::AqlValue NeighborsEnumerator::lastVertexToAqlValue() {
  TRI_ASSERT(_iterator != _currentDepth.end());
  return _traverser->fetchVertexData(*_iterator);
}

arangodb::aql::AqlValue NeighborsEnumerator::lastEdgeToAqlValue() {
  // If we get here the optimizer decided we do NOT need edges.
  // But the Block asks for it.
  TRI_ASSERT(false);
  THROW_ARANGO_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
}

arangodb::aql::AqlValue NeighborsEnumerator::pathToAqlValue(arangodb::velocypack::Builder& result) {
  // If we get here the optimizer decided we do NOT need paths
  // But the Block asks for it.
  TRI_ASSERT(false);
  THROW_ARANGO_EXCEPTION(TRI_ERROR_NOT_IMPLEMENTED);
}
