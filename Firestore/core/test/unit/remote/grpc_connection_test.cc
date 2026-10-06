/*
 * Copyright 2018 Google
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

#include "Firestore/core/src/remote/grpc_connection.h"

#include <fstream>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "Firestore/core/src/credentials/auth_token.h"
#include "Firestore/core/src/model/database_id.h"
#include "Firestore/core/src/remote/connectivity_monitor.h"
#include "Firestore/core/src/util/async_queue.h"
#include "Firestore/core/src/util/status.h"
#include "Firestore/core/src/util/statusor.h"
#include "Firestore/core/test/unit/remote/grpc_stream_tester.h"
#include "Firestore/core/test/unit/testutil/async_testing.h"
#include "Firestore/core/test/unit/testutil/filesystem_testing.h"
#include "absl/memory/memory.h"
#include "gtest/gtest.h"

namespace firebase {
namespace firestore {
namespace remote {

using core::DatabaseInfo;
using credentials::AuthToken;
using credentials::User;
using model::DatabaseId;
using util::AsyncQueue;
using util::Status;
using util::StatusOr;

using NetworkStatus = ConnectivityMonitor::NetworkStatus;
using ResponsesT = grpc::ByteBuffer;

namespace {

class FakeConnectivityMonitor : public ConnectivityMonitor {
 public:
  explicit FakeConnectivityMonitor(
      const std::shared_ptr<AsyncQueue>& worker_queue)
      : ConnectivityMonitor{worker_queue} {
    SetInitialStatus(NetworkStatus::Available);
  }

  void set_status(NetworkStatus new_status) {
    MaybeInvokeCallbacks(new_status);
  }
};

bool IsConnectivityChange(const Status& status) {
  return status.code() == Error::kErrorUnavailable;
}

class ConnectivityObserver : public GrpcStreamObserver {
 public:
  void OnStreamStart() override {
  }
  void OnStreamRead(const grpc::ByteBuffer&) override {
  }
  void OnStreamFinish(const util::Status& status) override {
    if (IsConnectivityChange(status)) {
      ++connectivity_change_count_;
    }
  }

  int connectivity_change_count() const {
    return connectivity_change_count_;
  }
  int connectivity_change_count_ = 0;
};

}  // namespace

class GrpcConnectionTest : public testing::Test {
 public:
  GrpcConnectionTest()
      : worker_queue{testutil::AsyncQueueForTesting()},
        connectivity_monitor{
            absl::make_unique<FakeConnectivityMonitor>(worker_queue)},
        tester{worker_queue, connectivity_monitor.get()} {
  }

  void SetNetworkStatus(NetworkStatus new_status) {
    worker_queue->EnqueueBlocking(
        [&] { connectivity_monitor->set_status(new_status); });
    // Make sure the callback executes.
    worker_queue->EnqueueBlocking([] {});
  }

  std::shared_ptr<AsyncQueue> worker_queue;
  std::unique_ptr<FakeConnectivityMonitor> connectivity_monitor = nullptr;
  GrpcStreamTester tester;
};

TEST_F(GrpcConnectionTest, GrpcStreamsNoticeChangeInConnectivity) {
  ConnectivityObserver observer;

  std::unique_ptr<GrpcStream> stream = tester.CreateStream(&observer);
  stream->Start();
  EXPECT_EQ(observer.connectivity_change_count(), 0);

  SetNetworkStatus(NetworkStatus::Available);
  // Same status shouldn't trigger a callback.
  EXPECT_EQ(observer.connectivity_change_count(), 0);

  tester.KeepPollingGrpcQueue();
  SetNetworkStatus(NetworkStatus::Unavailable);
  EXPECT_EQ(observer.connectivity_change_count(), 1);
}

TEST_F(GrpcConnectionTest, GrpcStreamingCallsNoticeChangeInConnectivity) {
  int change_count = 0;
  std::unique_ptr<GrpcStreamingReader> streaming_call =
      tester.CreateStreamingReader();
  streaming_call->Start(
      0, [&](const std::vector<ResponsesT>) {},
      [&](const util::Status& st, bool) {
        if (IsConnectivityChange(st)) {
          ++change_count;
        }
      });

  SetNetworkStatus(NetworkStatus::Available);
  // Same status shouldn't trigger a callback.
  EXPECT_EQ(change_count, 0);

  tester.KeepPollingGrpcQueue();
  SetNetworkStatus(NetworkStatus::AvailableViaCellular);
  EXPECT_EQ(change_count, 1);
}

TEST_F(GrpcConnectionTest, GrpcUnaryCallsNoticeChangeInConnectivity) {
  int change_count = 0;
  std::unique_ptr<GrpcUnaryCall> unary_call = tester.CreateUnaryCall();
  unary_call->Start([&](const StatusOr<grpc::ByteBuffer>& result) {
    if (IsConnectivityChange(result.status())) {
      ++change_count;
    }
  });

  SetNetworkStatus(NetworkStatus::Available);
  // Same status shouldn't trigger a callback.
  EXPECT_EQ(change_count, 0);

  tester.KeepPollingGrpcQueue();
  SetNetworkStatus(NetworkStatus::AvailableViaCellular);
  EXPECT_EQ(change_count, 1);
}

TEST_F(GrpcConnectionTest, ConnectivityChangeWithSeveralActiveCalls) {
  int changes_count = 0;

  std::unique_ptr<GrpcStreamingReader> foo = tester.CreateStreamingReader();
  foo->Start(
      0, [&](const std::vector<ResponsesT>) {},
      [&](const util::Status&, bool) {
        ++changes_count;
        foo.reset();
      });

  std::unique_ptr<GrpcStreamingReader> bar = tester.CreateStreamingReader();
  bar->Start(
      0, [&](const std::vector<ResponsesT>) {},
      [&](const util::Status&, bool) {
        ++changes_count;
        bar.reset();
      });

  std::unique_ptr<GrpcStreamingReader> baz = tester.CreateStreamingReader();
  baz->Start(
      0, [&](const std::vector<ResponsesT>) {},
      [&](const util::Status&, bool) {
        ++changes_count;
        baz.reset();
      });

  tester.KeepPollingGrpcQueue();
  // Calls will be unregistering themselves with `GrpcConnection` as it notifies
  // them, make sure nothing breaks.
  EXPECT_NO_THROW(SetNetworkStatus(NetworkStatus::Unavailable));
  EXPECT_EQ(changes_count, 3);
}

TEST_F(GrpcConnectionTest, ShutdownFastFinishesActiveCalls) {
  class NoFinishObserver : public GrpcStreamObserver {
   public:
    void OnStreamStart() override {
    }
    void OnStreamRead(const grpc::ByteBuffer&) override {
    }
    void OnStreamFinish(const util::Status&) override {
      FAIL() << "Observer shouldn't have been invoked";
    }
  };

  NoFinishObserver observer;
  std::unique_ptr<GrpcStream> foo = tester.CreateStream(&observer);
  foo->Start();

  std::unique_ptr<GrpcStreamingReader> bar = tester.CreateStreamingReader();
  bar->Start(
      0, [&](const std::vector<ResponsesT>) {},
      [&](const util::Status&, bool) {
        FAIL() << "Callback shouldn't have been invoked";
      });

  std::unique_ptr<GrpcUnaryCall> baz = tester.CreateUnaryCall();
  baz->Start([](const StatusOr<grpc::ByteBuffer>&) {
    FAIL() << "Callback shouldn't have been invoked";
  });

  tester.KeepPollingGrpcQueue();
  worker_queue->EnqueueBlocking([&] { tester.grpc_connection()->Shutdown(); });

  // Destroying a call will throw if it hasn't been properly shut down.
  EXPECT_NO_THROW(foo.reset());
  EXPECT_NO_THROW(bar.reset());
  EXPECT_NO_THROW(baz.reset());
}

// Extra channel arguments

namespace {

using ChannelKind = GrpcConnection::ChannelKind;

/** A flattened copy of one `grpc_arg`. */
struct ObservedArg {
  std::string key;
  bool is_int = false;
  int int_value = 0;
  std::string string_value;
};

struct ObservedChannel {
  ChannelKind kind;
  std::vector<ObservedArg> args;

  /** Returns the arguments with the given key, in order. */
  std::vector<ObservedArg> Find(const std::string& key) const {
    std::vector<ObservedArg> result;
    for (const ObservedArg& arg : args) {
      if (arg.key == key) {
        result.push_back(arg);
      }
    }
    return result;
  }
};

ObservedChannel Observe(ChannelKind kind, const grpc::ChannelArguments& args) {
  grpc_channel_args c_args;
  args.SetChannelArgs(&c_args);

  ObservedChannel result{kind, {}};
  for (size_t i = 0; i != c_args.num_args; ++i) {
    const grpc_arg& arg = c_args.args[i];
    ObservedArg observed;
    observed.key = arg.key;
    if (arg.type == GRPC_ARG_INTEGER) {
      observed.is_int = true;
      observed.int_value = arg.value.integer;
    } else if (arg.type == GRPC_ARG_STRING) {
      observed.string_value = arg.value.string;
    }
    result.args.push_back(std::move(observed));
  }
  return result;
}

const char* const kProxyKey = "grpc.http_proxy";
const char* const kProxyValue = "http://127.0.0.1:8899";
const char* const kIntKey = "mori.test.int_arg";

GrpcExtraChannelArgs ValidExtraArgs() {
  return {GrpcExtraChannelArg::String(kProxyKey, kProxyValue),
          GrpcExtraChannelArg::Int(kIntKey, -42)};
}

/**
 * Every argument Firestore puts on a channel must be reserved, so the reserved
 * set cannot drift from what `CreateChannel` actually sets.
 */
void ExpectNonExtraArgsAreReserved(const ObservedChannel& channel) {
  for (const ObservedArg& arg : channel.args) {
    if (arg.key == kProxyKey || arg.key == kIntKey) {
      continue;
    }
    EXPECT_TRUE(GrpcConnection::IsReservedChannelArgKey(arg.key))
        << "Firestore sets channel argument '" << arg.key
        << "' that is not reserved";
  }
}

}  // namespace

class GrpcConnectionExtraChannelArgsTest : public testing::Test {
 public:
  GrpcConnectionExtraChannelArgsTest()
      : worker_queue{testutil::AsyncQueueForTesting()},
        connectivity_monitor{
            absl::make_unique<FakeConnectivityMonitor>(worker_queue)} {
    GrpcConnection::SetChannelArgsObserverForTesting(
        [this](ChannelKind kind, const grpc::ChannelArguments& args) {
          observed.push_back(Observe(kind, args));
        });
  }

  ~GrpcConnectionExtraChannelArgsTest() override {
    GrpcConnection::SetChannelArgsObserverForTesting({});
    EXPECT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs({}).ok());
  }

  /** Creates a connection to `host` and makes it create its channel. */
  std::unique_ptr<GrpcStreamTester> CreateChannelFor(const std::string& host) {
    auto tester = absl::make_unique<GrpcStreamTester>(
        worker_queue, connectivity_monitor.get(),
        DatabaseInfo{DatabaseId{"foo", "bar"}, "", host, false});
    // Creating (but not starting) a call creates the channel.
    tester->CreateUnaryCall();
    return tester;
  }

  std::shared_ptr<AsyncQueue> worker_queue;
  std::unique_ptr<FakeConnectivityMonitor> connectivity_monitor;
  std::vector<ObservedChannel> observed;
};

TEST_F(GrpcConnectionExtraChannelArgsTest, AppliedToNormalTlsChannel) {
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(ValidExtraArgs()).ok());

  auto tester = CreateChannelFor("firestore.googleapis.com");

  ASSERT_EQ(observed.size(), 1u);
  const ObservedChannel& channel = observed[0];
  EXPECT_EQ(channel.kind, ChannelKind::kSecure);

  auto proxy = channel.Find(kProxyKey);
  ASSERT_EQ(proxy.size(), 1u);
  EXPECT_FALSE(proxy[0].is_int);
  EXPECT_EQ(proxy[0].string_value, kProxyValue);

  auto int_arg = channel.Find(kIntKey);
  ASSERT_EQ(int_arg.size(), 1u);
  EXPECT_TRUE(int_arg[0].is_int);
  EXPECT_EQ(int_arg[0].int_value, -42);

  // Firestore's own arguments are still present and not duplicated.
  auto keepalive = channel.Find(GRPC_ARG_KEEPALIVE_TIME_MS);
  ASSERT_EQ(keepalive.size(), 1u);
  EXPECT_EQ(keepalive[0].int_value, 30 * 1000);
  ExpectNonExtraArgsAreReserved(channel);
}

TEST_F(GrpcConnectionExtraChannelArgsTest, AppliedToTestTlsChannel) {
  testutil::TestTempDir dir;
  util::Path cert_path = dir.Child("test-cert.pem");
  {
    std::ofstream out(cert_path.native_value());
    out << "not a real certificate";
  }
  const std::string host = "extra-args-test-tls.mori.test:443";
  GrpcConnection::UseTestCertificate(host, cert_path, "test.target.name");
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(ValidExtraArgs()).ok());

  auto tester = CreateChannelFor(host);

  ASSERT_EQ(observed.size(), 1u);
  const ObservedChannel& channel = observed[0];
  EXPECT_EQ(channel.kind, ChannelKind::kTestSecure);
  ASSERT_EQ(channel.Find(kProxyKey).size(), 1u);
  EXPECT_EQ(channel.Find(kProxyKey)[0].string_value, kProxyValue);
  ASSERT_EQ(channel.Find(kIntKey).size(), 1u);
  EXPECT_EQ(channel.Find(kIntKey)[0].int_value, -42);

  auto target = channel.Find(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG);
  ASSERT_EQ(target.size(), 1u);
  EXPECT_EQ(target[0].string_value, "test.target.name");
  ExpectNonExtraArgsAreReserved(channel);
}

TEST_F(GrpcConnectionExtraChannelArgsTest, NotAppliedToInsecureChannel) {
  const std::string host = "extra-args-insecure.mori.test:8080";
  GrpcConnection::UseInsecureChannel(host);
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(ValidExtraArgs()).ok());

  auto tester = CreateChannelFor(host);

  ASSERT_EQ(observed.size(), 1u);
  const ObservedChannel& channel = observed[0];
  EXPECT_EQ(channel.kind, ChannelKind::kInsecure);
  EXPECT_TRUE(channel.Find(kProxyKey).empty());
  EXPECT_TRUE(channel.Find(kIntKey).empty());
  EXPECT_EQ(channel.Find(GRPC_ARG_KEEPALIVE_TIME_MS).size(), 1u);
  ExpectNonExtraArgsAreReserved(channel);
}

TEST_F(GrpcConnectionExtraChannelArgsTest, NoExtraArgsByDefault) {
  auto tester = CreateChannelFor("firestore.googleapis.com");

  ASSERT_EQ(observed.size(), 1u);
  EXPECT_TRUE(observed[0].Find(kProxyKey).empty());
  EXPECT_TRUE(observed[0].Find(kIntKey).empty());
}

TEST_F(GrpcConnectionExtraChannelArgsTest, EmptyClears) {
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(ValidExtraArgs()).ok());
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs({}).ok());

  EXPECT_TRUE(GrpcConnection::GetGlobalExtraChannelArgsForTesting().empty());
  auto tester = CreateChannelFor("firestore.googleapis.com");
  ASSERT_EQ(observed.size(), 1u);
  EXPECT_TRUE(observed[0].Find(kProxyKey).empty());
}

TEST_F(GrpcConnectionExtraChannelArgsTest, OnlyNewChannelsAreAffected) {
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(
                  {GrpcExtraChannelArg::String(kProxyKey, "http://old:1")})
                  .ok());
  auto first = CreateChannelFor("firestore.googleapis.com");
  ASSERT_EQ(observed.size(), 1u);

  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(
                  {GrpcExtraChannelArg::String(kProxyKey, "http://new:2")})
                  .ok());

  // The existing connection keeps its channel: no new channel is created.
  first->CreateUnaryCall();
  ASSERT_EQ(observed.size(), 1u);
  EXPECT_EQ(observed[0].Find(kProxyKey)[0].string_value, "http://old:1");

  auto second = CreateChannelFor("firestore.googleapis.com");
  ASSERT_EQ(observed.size(), 2u);
  EXPECT_EQ(observed[1].Find(kProxyKey)[0].string_value, "http://new:2");
}

TEST_F(GrpcConnectionExtraChannelArgsTest,
       InvalidArgsAreRejectedAndPreviousArgsKept) {
  ASSERT_TRUE(GrpcConnection::SetGlobalExtraChannelArgs(ValidExtraArgs()).ok());

  struct Case {
    const char* name;
    GrpcExtraChannelArgs args;
    std::string expected_in_message;
  };
  std::vector<Case> cases = {
      {"empty key", {GrpcExtraChannelArg::String("", "v")}, "empty"},
      {"NUL in key",
       {GrpcExtraChannelArg::String(std::string("a\0b", 3), "v")},
       "NUL"},
      {"NUL in value",
       {GrpcExtraChannelArg::String("k", std::string("a\0b", 3))},
       "'k'"},
      {"duplicate key",
       {GrpcExtraChannelArg::Int("dup", 1), GrpcExtraChannelArg::Int("dup", 2)},
       "'dup'"},
      {"reserved keepalive",
       {GrpcExtraChannelArg::Int(GRPC_ARG_KEEPALIVE_TIME_MS, 1)},
       GRPC_ARG_KEEPALIVE_TIME_MS},
      {"reserved ssl target",
       {GrpcExtraChannelArg::String(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, "x")},
       GRPC_SSL_TARGET_NAME_OVERRIDE_ARG},
      {"reserved user agent",
       {GrpcExtraChannelArg::String(GRPC_ARG_PRIMARY_USER_AGENT_STRING, "x")},
       GRPC_ARG_PRIMARY_USER_AGENT_STRING},
      // A valid entry before an invalid one must not be installed either.
      {"valid then invalid",
       {GrpcExtraChannelArg::String(kProxyKey, "http://other:1"),
        GrpcExtraChannelArg::String("", "v")},
       "empty"},
  };

  for (const Case& c : cases) {
    SCOPED_TRACE(c.name);
    util::Status status = GrpcConnection::SetGlobalExtraChannelArgs(c.args);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), Error::kErrorInvalidArgument);
    EXPECT_NE(status.error_message().find(c.expected_in_message),
              std::string::npos)
        << status.error_message();
  }

  GrpcExtraChannelArgs installed =
      GrpcConnection::GetGlobalExtraChannelArgsForTesting();
  ASSERT_EQ(installed.size(), 2u);
  EXPECT_EQ(installed[0].key, kProxyKey);
  EXPECT_EQ(installed[0].string_value, kProxyValue);

  auto tester = CreateChannelFor("firestore.googleapis.com");
  ASSERT_EQ(observed.size(), 1u);
  ASSERT_EQ(observed[0].Find(kProxyKey).size(), 1u);
  EXPECT_EQ(observed[0].Find(kProxyKey)[0].string_value, kProxyValue);
}

TEST(GrpcConnectionReservedKeysTest, ReservedKeysMatchArgsFirestoreSets) {
  EXPECT_TRUE(GrpcConnection::IsReservedChannelArgKey(
      GRPC_ARG_PRIMARY_USER_AGENT_STRING));
  EXPECT_TRUE(
      GrpcConnection::IsReservedChannelArgKey(GRPC_ARG_KEEPALIVE_TIME_MS));
  EXPECT_TRUE(GrpcConnection::IsReservedChannelArgKey(
      GRPC_SSL_TARGET_NAME_OVERRIDE_ARG));
  EXPECT_FALSE(GrpcConnection::IsReservedChannelArgKey("grpc.http_proxy"));
}

}  // namespace remote
}  // namespace firestore
}  // namespace firebase
