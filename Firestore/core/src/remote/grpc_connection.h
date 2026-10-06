/*
 * Copyright 2018 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FIRESTORE_CORE_SRC_REMOTE_GRPC_CONNECTION_H_
#define FIRESTORE_CORE_SRC_REMOTE_GRPC_CONNECTION_H_

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "Firestore/core/src/core/database_info.h"
#include "Firestore/core/src/credentials/auth_token.h"
#include "Firestore/core/src/remote/connectivity_monitor.h"
#include "Firestore/core/src/remote/grpc_call.h"
#include "Firestore/core/src/remote/grpc_stream.h"
#include "Firestore/core/src/remote/grpc_stream_observer.h"
#include "Firestore/core/src/remote/grpc_streaming_reader.h"
#include "Firestore/core/src/remote/grpc_unary_call.h"
#include "Firestore/core/src/util/path.h"
#include "Firestore/core/src/util/status.h"
#include "Firestore/core/src/util/warnings.h"
#include "absl/strings/string_view.h"
#include "grpcpp/channel.h"
#include "grpcpp/client_context.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/support/channel_arguments.h"

SUPPRESS_DOCUMENTATION_WARNINGS_BEGIN()
#include "grpcpp/generic/generic_stub.h"
SUPPRESS_END()

namespace firebase {
namespace firestore {
namespace remote {

class FirebaseMetadataProvider;

/**
 * One extra gRPC channel argument installed through
 * `GrpcConnection::SetGlobalExtraChannelArgs`. Owns its key and value.
 */
struct GrpcExtraChannelArg {
  enum class Type { kString, kInt };

  static GrpcExtraChannelArg String(std::string key, std::string value) {
    return GrpcExtraChannelArg{std::move(key), Type::kString, std::move(value),
                               0};
  }
  static GrpcExtraChannelArg Int(std::string key, int value) {
    return GrpcExtraChannelArg{std::move(key), Type::kInt, std::string{},
                               value};
  }

  std::string key;
  Type type = Type::kString;
  std::string string_value;
  int int_value = 0;
};

using GrpcExtraChannelArgs = std::vector<GrpcExtraChannelArg>;

// PORTING NOTE: this class has limited resemblance to `GrpcConnection` in Web
// client. However, unlike Web client, it's not meant to hide different
// implementations of a `Connection` under a single interface.

/**
 * Creates and owns gRPC objects (channel and stub) necessary to produce a
 * `GrpcStream`.
 */
class GrpcConnection {
 public:
  GrpcConnection(const core::DatabaseInfo& database_info,
                 const std::shared_ptr<util::AsyncQueue>& worker_queue,
                 grpc::CompletionQueue* grpc_queue,
                 ConnectivityMonitor* connectivity_monitor,
                 FirebaseMetadataProvider* firebase_metadata_provider);

  void Shutdown();

  /**
   * Creates a stream to the given stream RPC endpoint. The resulting stream
   * needs to be `Start`ed before it can be used.
   */
  // PORTING NOTE: unlike Web client, the created stream is not open and has to
  // be started manually.
  std::unique_ptr<GrpcStream> CreateStream(
      absl::string_view rpc_name,
      const credentials::AuthToken& auth_token,
      const std::string& app_check_token,
      GrpcStreamObserver* observer);

  std::unique_ptr<GrpcUnaryCall> CreateUnaryCall(
      absl::string_view rpc_name,
      const credentials::AuthToken& auth_token,
      const std::string& app_check_token,
      const grpc::ByteBuffer& message);

  std::unique_ptr<GrpcStreamingReader> CreateStreamingReader(
      absl::string_view rpc_name,
      const credentials::AuthToken& auth_token,
      const std::string& app_check_token,
      const grpc::ByteBuffer& message);

  void Register(GrpcCall* call);
  void Unregister(GrpcCall* call);

  static void SetClientLanguage(std::string language_token);

  /**
   * Don't use SSL, send all traffic unencrypted. Call before creating any
   * streams or calls.
   */
  static void UseInsecureChannel(const std::string& host);

  /**
   * For tests only: use a custom root certificate file and the given SSL
   * target name for all connections. Call before creating any streams or calls.
   */
  static void UseTestCertificate(const std::string& host,
                                 const util::Path& certificate_path,
                                 const std::string& target_name);

  /**
   * Replaces the extra channel arguments applied to every secure (normal TLS
   * or test-certificate TLS) channel created after this call returns. Existing
   * channels are not changed, and insecure channels never receive them. An
   * empty vector clears the extra arguments.
   *
   * All-or-nothing: if any entry is invalid (empty key, key or string value
   * containing NUL, duplicate key, or a key Firestore itself sets on the
   * channel, see `IsReservedChannelArgKey`), returns an `InvalidArgument`
   * status naming the key and leaves the previously installed arguments in
   * effect.
   */
  static util::Status SetGlobalExtraChannelArgs(GrpcExtraChannelArgs args);

  /**
   * Whether `key` is a channel argument that Firestore (or the
   * `grpc::ChannelArguments` it constructs) sets itself and therefore cannot
   * be overridden through `SetGlobalExtraChannelArgs`.
   */
  static bool IsReservedChannelArgKey(const std::string& key);

  /** The kind of channel `CreateChannel` builds. */
  enum class ChannelKind { kSecure, kInsecure, kTestSecure };

  using ChannelArgsObserver =
      std::function<void(ChannelKind, const grpc::ChannelArguments&)>;

  /**
   * For tests only: `observer` is invoked with the final channel arguments
   * right before each channel is created. Pass an empty function to remove.
   */
  static void SetChannelArgsObserverForTesting(ChannelArgsObserver observer);

  /** For tests only: the currently installed extra channel arguments. */
  static GrpcExtraChannelArgs GetGlobalExtraChannelArgsForTesting();

 private:
  std::unique_ptr<grpc::ClientContext> CreateContext(
      const credentials::AuthToken& auth_token,
      const std::string& app_check_token) const;
  std::shared_ptr<grpc::Channel> CreateChannel() const;
  void EnsureActiveStub();

  void RegisterConnectivityMonitor();

  const core::DatabaseInfo* database_info_ = nullptr;
  std::shared_ptr<util::AsyncQueue> worker_queue_;
  grpc::CompletionQueue* grpc_queue_ = nullptr;

  std::shared_ptr<grpc::Channel> grpc_channel_;
  std::unique_ptr<grpc::GenericStub> grpc_stub_;

  ConnectivityMonitor* connectivity_monitor_ = nullptr;
  std::vector<GrpcCall*> active_calls_;

  FirebaseMetadataProvider* firebase_metadata_provider_ = nullptr;
};

}  // namespace remote
}  // namespace firestore
}  // namespace firebase

#endif  // FIRESTORE_CORE_SRC_REMOTE_GRPC_CONNECTION_H_
