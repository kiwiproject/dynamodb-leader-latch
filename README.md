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
