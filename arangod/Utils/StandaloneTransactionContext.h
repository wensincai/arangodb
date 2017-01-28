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
/// @author Jan Steemann
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_UTILS_STANDALONE_TRANSACTION_CONTEXT_H
#define ARANGOD_UTILS_STANDALONE_TRANSACTION_CONTEXT_H 1

#include "Basics/Common.h"
#include "Utils/TransactionContext.h"

struct TRI_vocbase_t;

namespace arangodb {
struct TransactionState;

class StandaloneTransactionContext final : public TransactionContext {

 public:

  //////////////////////////////////////////////////////////////////////////////
  /// @brief create the context
  //////////////////////////////////////////////////////////////////////////////

  explicit StandaloneTransactionContext(TRI_vocbase_t*);

  //////////////////////////////////////////////////////////////////////////////
  /// @brief destroy the context
  //////////////////////////////////////////////////////////////////////////////

  ~StandaloneTransactionContext() = default;
 
 public:

  //////////////////////////////////////////////////////////////////////////////
  /// @brief order a custom type handler
  //////////////////////////////////////////////////////////////////////////////

  std::shared_ptr<VPackCustomTypeHandler> orderCustomTypeHandler() override final;
  
  //////////////////////////////////////////////////////////////////////////////
  /// @brief return the resolver
  //////////////////////////////////////////////////////////////////////////////

  CollectionNameResolver const* getResolver() override final;
  
  //////////////////////////////////////////////////////////////////////////////
  /// @brief return the parent transaction (none in our case)
  //////////////////////////////////////////////////////////////////////////////

  TransactionState* getParentTransaction() const override;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief register the transaction, does nothing
  //////////////////////////////////////////////////////////////////////////////

  int registerTransaction(TransactionState*) override;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief unregister the transaction
  //////////////////////////////////////////////////////////////////////////////

  void unregisterTransaction() override;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief whether or not the transaction is embeddable
  //////////////////////////////////////////////////////////////////////////////

  bool isEmbeddable() const override;
  
  //////////////////////////////////////////////////////////////////////////////
  /// @brief create a context, returned in a shared ptr
  //////////////////////////////////////////////////////////////////////////////

  static std::shared_ptr<StandaloneTransactionContext> Create(TRI_vocbase_t*);

};
}

#endif
