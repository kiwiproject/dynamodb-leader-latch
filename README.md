### DynamoDB Leader Latch

[![Build](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/build.yml?query=branch%3Amain)
[![CodeQL](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/codeql.yml/badge.svg)](https://github.com/kiwiproject/dynamodb-leader-latch/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

A small library that elects one leader among multiple instances of the same logical service,
using Amazon DynamoDB (via the AWS Labs
[amazon-dynamodb-lock-client](https://github.com/awslabs/amazon-dynamodb-lock-client)) as the backend.

It is the DynamoDB counterpart to
[dropwizard-leader-latch](https://github.com/kiwiproject/dropwizard-leader-latch), but this is the
framework-independent core: it has no Curator, ZooKeeper, Dropwizard, or Helidon dependency.
Framework integrations are intended to live in separate libraries.

> Status: under development. Not yet released.

## Usage

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

switch (latch.start()) {
    case StartResult.Failed failed -> log.error("Could not start latch", failed.cause());
    default -> { /* started; leadership is acquired asynchronously */ }
}

if (latch.hasLeadership()) {
    // leader-only work
}

// or switch over the outcome
latch.whenLeader(() -> pollExternalService());
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
