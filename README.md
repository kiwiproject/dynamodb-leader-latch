### DynamoDB Leader Latch

[![Build](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/build.yml?query=branch%3Amain)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_dynamodb-leader-latch&metric=alert_status)](https://sonarcloud.io/dashboard?id=kiwiproject_dynamodb-leader-latch)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=kiwiproject_dynamodb-leader-latch&metric=coverage)](https://sonarcloud.io/dashboard?id=kiwiproject_dynamodb-leader-latch)
[![CodeQL](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/codeql.yml/badge.svg)](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/codeql.yml)
[![javadoc](https://javadoc.io/badge2/org.kiwiproject/dynamodb-leader-latch/javadoc.svg)](https://javadoc.io/doc/org.kiwiproject/dynamodb-leader-latch)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Maven Central](https://img.shields.io/maven-central/v/org.kiwiproject/dynamodb-leader-latch)](https://central.sonatype.com/artifact/org.kiwiproject/dynamodb-leader-latch/)

A small library that elects one leader among multiple instances of the same logical service,
using Amazon DynamoDB (via the AWS Labs
[amazon-dynamodb-lock-client](https://github.com/awslabs/amazon-dynamodb-lock-client)) as the backend.

It is the DynamoDB counterpart to
[dropwizard-leader-latch](https://github.com/kiwiproject/dropwizard-leader-latch), but this is the
framework-independent core: it has no Curator, ZooKeeper, Dropwizard, or Helidon dependency.
Framework integrations are intended to live in separate libraries.

## Usage

Add the dependency:

```xml
<dependency>
    <groupId>org.kiwiproject</groupId>
    <artifactId>dynamodb-leader-latch</artifactId>
    <version>[current-version]</version>
</dependency>
```

Then create and start a latch:

```java
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;  // AWS SDK v2

// Default region and credentials; on ECS this uses the task role.
// You own this client and must close it; the latch never closes it.
var dynamoDb = DynamoDbClient.create();
var config = LeaderLatchConfiguration.forTable("service-leader-locks");

var participantId = DynamoDbLeaderLatch.leaderLatchId("order-service", "1.2.3", hostname, port);
var latch = new DynamoDbLeaderLatch(dynamoDb, config, "order-service", participantId);

latch.addListener(new LeaderLatchListener() {
    public void isLeader()  { /* start leader-only work */ }
    public void notLeader() { /* stop leader-only work */ }
});

// Start before relying on leadership; add listeners before start() so none are missed.
// start() does not wait to become the leader; leadership is acquired asynchronously.
if (latch.start() instanceof StartResult.Failed failed) {
    log.error("Could not start latch", failed.cause());
}

if (latch.hasLeadership()) {
    // leader-only work
}

// or switch over the outcome
latch.whenLeader(() -> pollExternalService());

// On shutdown, close the latch first (it releases the lock), then the client it uses.
// The latch never closes the DynamoDbClient, so you must.
latch.close();
dynamoDb.close();
```

Expected failures (DynamoDB unreachable, lock held by someone else) are returned as values, not
thrown. `hasLeadership()` is true only while the latch can prove it still holds the lease; if it
cannot (for example, during a DynamoDB outage) it behaves as a follower.

### Who is the leader?

`latch.getLeader()` returns a `LeaderInfo` (`Leader`, `NoLeader`, or `LookupFailed`) read from the
lock record in DynamoDB, so any participant can tell who the leader is. The record can still name a
crashed leader until its lease expires, and DynamoDB cannot rule out a paused process briefly
believing it is still the leader. A health check should therefore combine each instance's own
`hasLeadership()` with `getLeader()` and flag more than one self-reported leader. `getLeader()`
requires a started latch.

### DynamoDB table

The table must already exist; this library never creates it. One table can be shared by many
services. Each leadership key is one item.

| Setting | Value |
|---|---|
| Partition key | `key` (String) |
| Sort key | none |

### IAM permissions

The lock client needs only item-level access to the table:

```json
{
  "Effect": "Allow",
  "Action": [
    "dynamodb:GetItem",
    "dynamodb:PutItem",
    "dynamodb:UpdateItem",
    "dynamodb:DeleteItem",
    "dynamodb:Scan"
  ],
  "Resource": "arn:aws:dynamodb:<region>:<account>:table/service-leader-locks"
}
```

`CreateTable` and `DescribeTable` are not required.

### Configuring the `DynamoDbClient`

The latch uses the `DynamoDbClient` you give it, for everything. So credentials, region, endpoint, TLS, and timeouts
are all configured by you when you build the client.

**You must close the `DynamoDbClient` yourself.** The latch never closes it, and an unclosed client keeps its
connections and threads alive. Close the latch first, then the client, because the latch uses the client until it is
closed. In an application framework, register the client's `close()` for shutdown before the latch is registered, since
most frameworks stop things in reverse order of registration (in Dropwizard, a `Managed` whose `stop()` closes the
client).

**Custom CA certificates.** If DynamoDB is reached through an endpoint whose certificate is signed by a CA that is not
in the JVM's default trust store, either point the JVM at a trust store that includes it
(`-Djavax.net.ssl.trustStore=...` and `-Djavax.net.ssl.trustStorePassword=...`), or give the HTTP client your own
trust managers:

```java
var httpClientBuilder = Apache5HttpClient.builder()
        .tlsTrustManagersProvider(() -> trustManagers);   // a TrustManager[] built from your trust store

var dynamoDb = DynamoDbClient.builder()
        .httpClientBuilder(httpClientBuilder)
        .build();
```

Pass the HTTP client *builder* to `httpClientBuilder(...)` so that the `DynamoDbClient` closes the HTTP client when you
close the `DynamoDbClient`. `Apache5HttpClient` is in `software.amazon.awssdk:apache5-client`, which this library brings in at runtime
scope, so declare it as a dependency yourself if you compile against it. A `javax.net.ssl.SSLHandshakeException` or
`PKIX path building failed` at startup means the client does not trust the endpoint's certificate.

**Timeouts.** The SDK's default timeouts and retries are patient. A heartbeat call that hangs counts against the lease
(see the table below), so set the HTTP client's connection and socket timeouts and the client's
`apiCallAttemptTimeout` and `apiCallTimeout` (through `overrideConfiguration`) well below the lease duration, and
`apiCallTimeout` no longer than the heartbeat period.

### Configuration

| Setting | Default | Notes |
|---|---|---|
| `leaseDuration` | 30s | Must be at least 3x `heartbeatPeriod` |
| `heartbeatPeriod` | 5s | How often the leader refreshes its lease |
| `acquisitionRetryInterval` | 5s | How often a follower tries to take over |

Failover after an abrupt leader failure takes roughly one `leaseDuration`; a graceful `close()`
releases the lock so another participant takes over within one `acquisitionRetryInterval`.

## Development

`mvn verify` runs the unit tests and integration tests. The integration tests use an embedded
DynamoDB Local (no Docker required).
