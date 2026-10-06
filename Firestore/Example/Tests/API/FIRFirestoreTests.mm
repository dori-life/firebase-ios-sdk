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

#import <FirebaseFirestore/FIRFirestore.h>
#import <FirebaseFirestore/FIRFirestoreErrors.h>
#import <FirebaseFirestore/FIRFirestoreSettings.h>

#import <XCTest/XCTest.h>

#import "FirebaseCore/Extension/FIRAppInternal.h"

#include <climits>

#include "Firestore/core/src/remote/grpc_connection.h"
#include "Firestore/core/test/unit/testutil/app_testing.h"

using firebase::firestore::remote::GrpcConnection;
using firebase::firestore::remote::GrpcExtraChannelArg;
using firebase::firestore::remote::GrpcExtraChannelArgs;
using firebase::firestore::testutil::AppForUnitTesting;

@interface FIRFirestoreTests : XCTestCase
@end

@implementation FIRFirestoreTests

- (void)testDeleteApp {
  // Ensure the app is set appropriately.
  FIRApp *app = AppForUnitTesting();
  NSString *appName = app.name;
  FIROptions *options = app.options;

  FIRFirestore *firestore = [FIRFirestore firestoreForApp:app];
  XCTAssertEqualObjects(firestore.app, app);

  // Ensure that firestoreForApp returns the same instance.
  XCTAssertEqualObjects(firestore, [FIRFirestore firestoreForApp:app]);

  XCTestExpectation *defaultAppDeletedExpectation =
      [self expectationWithDescription:@"Deleting the default app should invalidate the default "
                                       @"Firestore instance."];
  [app deleteApp:^(BOOL success) {
    XCTAssertTrue(success);

    // Recreate the FIRApp with the same name, fetch a new Firestore instance and make sure it's
    // different than the other one.
    [FIRApp configureWithName:appName options:options];
    FIRApp *newApp = [FIRApp appNamed:appName];
    FIRFirestore *newInstance = [FIRFirestore firestoreForApp:newApp];
    XCTAssertNotEqualObjects(newInstance, firestore);

    [defaultAppDeletedExpectation fulfill];
  }];

  [self waitForExpectationsWithTimeout:2
                               handler:^(NSError *_Nullable error) {
                                 XCTAssertNil(error);
                               }];
}

- (void)testSetEmulatorSettingsSetsHost {
  // Ensure the app is set appropriately.
  FIRApp *app = AppForUnitTesting();

  FIRFirestore *firestore = [FIRFirestore firestoreForApp:app];

  [firestore useEmulatorWithHost:@"localhost" port:1000];

  NSString *host = firestore.settings.host;
  XCTAssertEqualObjects(host, @"localhost:1000");
}

#pragma mark - setGlobalGrpcChannelArguments

- (void)tearDown {
  XCTAssertTrue([FIRFirestore setGlobalGrpcChannelArguments:nil error:nil]);
  [super tearDown];
}

- (void)testSetGlobalGrpcChannelArgumentsConvertsValidDictionary {
  NSError *error = nil;
  BOOL ok = [FIRFirestore setGlobalGrpcChannelArguments:@{
    @"grpc.http_proxy" : @"http://127.0.0.1:8899",
    @"mori.int" : @(INT_MIN),
    @"mori.uint" : [NSNumber numberWithUnsignedLongLong:INT_MAX],
  }
                                                  error:&error];
  XCTAssertTrue(ok);
  XCTAssertNil(error);

  GrpcExtraChannelArgs installed = GrpcConnection::GetGlobalExtraChannelArgsForTesting();
  XCTAssertEqual(installed.size(), 3u);
  for (const GrpcExtraChannelArg &arg : installed) {
    if (arg.key == "grpc.http_proxy") {
      XCTAssertTrue(arg.type == GrpcExtraChannelArg::Type::kString);
      XCTAssertTrue(arg.string_value == "http://127.0.0.1:8899");
    } else if (arg.key == "mori.int") {
      XCTAssertTrue(arg.type == GrpcExtraChannelArg::Type::kInt);
      XCTAssertEqual(arg.int_value, INT_MIN);
    } else if (arg.key == "mori.uint") {
      XCTAssertTrue(arg.type == GrpcExtraChannelArg::Type::kInt);
      XCTAssertEqual(arg.int_value, INT_MAX);
    } else {
      XCTFail(@"Unexpected key %s", arg.key.c_str());
    }
  }
}

- (void)testSetGlobalGrpcChannelArgumentsNilAndEmptyClear {
  XCTAssertTrue([FIRFirestore setGlobalGrpcChannelArguments:@{@"a" : @"b"} error:nil]);
  XCTAssertTrue([FIRFirestore setGlobalGrpcChannelArguments:nil error:nil]);
  XCTAssertTrue(GrpcConnection::GetGlobalExtraChannelArgsForTesting().empty());

  XCTAssertTrue([FIRFirestore setGlobalGrpcChannelArguments:@{@"a" : @"b"} error:nil]);
  XCTAssertTrue([FIRFirestore setGlobalGrpcChannelArguments:@{} error:nil]);
  XCTAssertTrue(GrpcConnection::GetGlobalExtraChannelArgsForTesting().empty());
}

- (void)testSetGlobalGrpcChannelArgumentsRejectsInvalidAndKeepsPrevious {
  XCTAssertTrue([FIRFirestore setGlobalGrpcChannelArguments:@{@"grpc.http_proxy" : @"http://keep:1"}
                                                      error:nil]);

  unichar nul = 0;
  NSString *nulString = [NSString stringWithFormat:@"a%@b", [NSString stringWithCharacters:&nul
                                                                                     length:1]];
  NSDictionary *invalidCases = @{
    @"float" : @{@"k" : @(1.5)},
    @"integral double" : @{@"k" : @(2.0)},
    @"NaN" : @{@"k" : @(NAN)},
    @"BOOL" : @{@"k" : @YES},
    @"above INT_MAX" : @{@"k" : @((long long)INT_MAX + 1)},
    @"below INT_MIN" : @{@"k" : @((long long)INT_MIN - 1)},
    @"unsigned overflow" : @{@"k" : [NSNumber numberWithUnsignedLongLong:ULLONG_MAX]},
    @"decimal" : @{@"k" : [NSDecimalNumber decimalNumberWithString:@"3"]},
    @"NUL in key" : @{nulString : @"v"},
    @"NUL in value" : @{@"k" : nulString},
    @"empty key" : @{@"" : @"v"},
    @"non-string key" : @{@1 : @"v"},
    @"unsupported value type" : @{@"k" : @[ @"v" ]},
    @"reserved keepalive" : @{@"grpc.keepalive_time_ms" : @1},
    @"reserved ssl target" : @{@"grpc.ssl_target_name_override" : @"x"},
    @"reserved user agent" : @{@"grpc.primary_user_agent" : @"x"},
    @"valid and invalid" : @{@"grpc.http_proxy" : @"http://other:1", @"k" : @(1.5)},
  };

  for (NSString *name in invalidCases) {
    NSError *error = nil;
    BOOL ok = [FIRFirestore setGlobalGrpcChannelArguments:invalidCases[name] error:&error];
    XCTAssertFalse(ok, @"%@", name);
    XCTAssertNotNil(error, @"%@", name);
    XCTAssertEqualObjects(error.domain, FIRFirestoreErrorDomain, @"%@", name);
    XCTAssertEqual(error.code, FIRFirestoreErrorCodeInvalidArgument, @"%@", name);

    GrpcExtraChannelArgs installed = GrpcConnection::GetGlobalExtraChannelArgsForTesting();
    XCTAssertEqual(installed.size(), 1u, @"%@", name);
    if (installed.size() == 1) {
      XCTAssertTrue(installed[0].key == "grpc.http_proxy", @"%@", name);
      XCTAssertTrue(installed[0].string_value == "http://keep:1", @"%@", name);
    }
  }
}

- (void)testSetGlobalGrpcChannelArgumentsErrorNamesKey {
  NSError *error = nil;
  XCTAssertFalse([FIRFirestore setGlobalGrpcChannelArguments:@{@"my.key" : @(0.5)} error:&error]);
  XCTAssertTrue([error.localizedDescription containsString:@"my.key"]);

  error = nil;
  XCTAssertFalse([FIRFirestore setGlobalGrpcChannelArguments:@{@"grpc.keepalive_time_ms" : @1}
                                                       error:&error]);
  XCTAssertTrue([error.localizedDescription containsString:@"grpc.keepalive_time_ms"]);
}

@end
