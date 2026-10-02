# Contributing to TitanKV

Issues and pull requests are welcome. For a bug, include steps to reproduce, the expected and
actual behaviour, and logs; for a feature, the use case and how it fits the design in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Build and test

Requires Java 17+ and Maven 3.6+.

```bash
mvn clean package          # build target/titankv-1.0.0.jar and run the unit tests
mvn verify                 # + integration tests (real in-JVM clusters, including the chaos test)
                           #   and the 75% line-coverage gate
mvn test -Dtest=ConsistentHashTest
mvn verify -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=ChaosTest
```

Integration tests are tagged `integration` and run under Failsafe. They bind client ports from 19000
up, plus gossip (port + 1000) and metrics (port + 90).

Try a change on a real cluster:

```bash
./scripts/start-cluster.sh                 # 3 local nodes; NODES=5 for more
./scripts/titankv-cli.sh localhost:9001
./scripts/run-benchmark.sh --hosts localhost:9001,localhost:9002,localhost:9003 --duration 10
./scripts/stop-cluster.sh
```

## Expectations for a pull request

- `mvn verify` passes, and the build has no compiler warnings (it compiles with `-Xlint:all`).
- A behaviour change comes with a test. For replication, membership or storage changes, prefer an
  integration test on a `TestCluster`, and consider whether the chaos test covers the failure mode.
- A change that may affect performance includes before/after numbers from
  `./scripts/benchmark-suite.sh` (or the relevant scenario), measured on the same machine.
- A wire-format change updates the protocol section of the README. The protocol has no version
  field, so every node and client in a cluster must run the same build.

## Layout

| Package | Contents |
|---|---|
| `core` | In-memory store, write-ahead log, snapshots |
| `cluster` | Hash ring, gossip, membership, data streaming to joining nodes |
| `consistency` | Replication, read repair, hinted handoff, anti-entropy |
| `network` | NIO server, request handling, binary protocol, client connection pool |
| `util` | Hybrid logical clock, metrics, configuration |
