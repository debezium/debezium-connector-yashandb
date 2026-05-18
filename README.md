# Debezium Connector for YashanDB

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

An incubating Debezium CDC (Change Data Capture) connector for [YashanDB](https://www.yasdb.com/). This connector enables real-time data streaming from YashanDB databases to Apache Kafka.

## Overview

YashanDB is a distributed database system developed by Yashan Technologies. This connector captures row-level changes (insert, update, delete) from a YashanDB database and delivers those change events to Kafka topics.

Built on Debezium 3.6.0, the connector leverages YashanDB's YStream technology for efficient change data capture.

## Prerequisites

- **JDK 21+** (required by Debezium 3.6.0)
- **Apache Kafka 3.6.x** with Kafka Connect
- **YashanDB** with YStream enabled
- **YashanDB JDBC driver** (version 1.9.24 or compatible)
- **YStream library** (`com.sics.ystream:Ystream`)

## Building

```bash
# Build connector JAR only
mvn clean package -DskipTests

# Build connector with distribution package (recommended for deployment)
mvn clean package -Passembly -DskipTests

# Build and run all tests (unit + integration)
mvn install
```

### Testing

This module contains both unit tests and integration tests.

A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

An *integration test* is a JUnit test class named `*IT.java` or `IT*.java` that uses a YashanDB database server. The build will automatically set up the test environment before the integration tests are run.

Running `mvn install` will compile all code and run the unit and integration tests. If there are any compile problems or any of the unit tests fail, the build will stop immediately.

You should always default to using `mvn install`, especially prior to committing changes to Git. However, there are a few situations where you may want to run a different Maven command.

#### Running some tests

If you are trying to get the test methods in a single test class to pass and would rather not run *all* of the tests, you can instruct Maven to just run that one test class and to skip all of the others. For example, use the following command to run the tests in the `YashanDbConnectorTest` class:

    $ mvn test -Dtest=YashanDbConnectorTest

Of course, wildcards also work:

    $ mvn test -Dtest=YashanDb*Test

#### Building just the artifacts, without running tests

You can skip all non-essential plug-ins (tests, CheckStyle, formatter, etc.) using the "quick" build profile:

    $ mvn clean verify -Dquick

This provides the fastest way for solely producing the output artifacts, without running any of the QA related Maven plug-ins.
This comes in handy for producing connector JARs and/or archives as quickly as possible, e.g. for manual testing in Kafka Connect.

## Deployment

Extract the distribution package to your Kafka Connect plugin directory:

```bash
# Using tar.gz
tar -xzf target/debezium-connector-yashandb-3.6.0-SNAPSHOT-plugin.tar.gz \
    -C $KAFKA_CONNECT_PLUGINS_DIR/

# Or using zip
unzip target/debezium-connector-yashandb-3.6.0-SNAPSHOT-plugin.zip \
    -d $KAFKA_CONNECT_PLUGINS_DIR/
```

Then restart Kafka Connect to load the new connector.

## Configuration

### Required Properties

| Property | Description | Default |
|----------|-------------|---------|
| `name` | Unique connector instance name | - |
| `connector.class` | Connector class | `io.debezium.connector.yashandb.YashanDbConnector` |
| `database.hostname` | YashanDB server hostname | - |
| `database.port` | YashanDB server port | `1688` |
| `database.user` | Database user | - |
| `database.password` | Database password | - |
| `database.dbname` | Database name | - |
| `database.ystream.server.name` | YStream server name | - |
| `topic.prefix` | Topic name prefix | - |

### Optional Properties

#### Snapshot

| Property | Description | Default |
|----------|-------------|---------|
| `snapshot.mode` | `initial`, `initial_only`, `schema_only`, `schema_only_recovery`, `always` | `initial` |
| `snapshot.locking.mode` | `shared`, `none` | `shared` |
| `snapshot.enhance.predicate.scn` | Token for snapshot predicate enhancement | - |
| `snapshot.database.errors.max.retries` | Max retries for snapshot database errors | `0` |

#### Data Types

| Property | Description | Default |
|----------|-------------|---------|
| `interval.handling.mode` | INTERVAL column representation: `string`, `numeric` | `numeric` |
| `decimal.handling.mode` | DECIMAL column representation: `precise`, `double`, `string` | `precise` |
| `legacy.decimal.handling.strategy` | Legacy decimal handling strategy | - |

### Example Configuration

```json
{
  "name": "yashandb-connector",
  "connector.class": "io.debezium.connector.yashandb.YashanDbConnector",
  "database.hostname": "localhost",
  "database.port": "1688",
  "database.user": "debezium",
  "database.password": "dbz_password",
  "database.dbname": "mydb",
  "database.ystream.server.name": "ystream_server",
  "topic.prefix": "yashandb",
  "snapshot.mode": "initial",
  "table.include.list": "SCHEMA1.TABLE1,SCHEMA1.TABLE2",
  "schema.history.internal.kafka.topic": "yashandb-schema-history",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:9092"
}
```

## Output

### Event Key

```json
{
  "schema": { ... },
  "payload": {
    "SCHEMA_NAME": "SCHEMA1",
    "TABLE_NAME": "TABLE1",
    "PRIMARY_KEY_COLUMN": "value"
  }
}
```

### Event Value

```json
{
  "schema": { ... },
  "payload": {
    "before": { ... },
    "after": { ... },
    "source": {
      "version": "3.6.0",
      "connector": "yashandb",
      "name": "yashandb",
      "ts_ms": 1234567890,
      "db": "mydb",
      "schema": "SCHEMA1",
      "table": "TABLE1",
      "scn": "123456"
    },
    "op": "c",
    "ts_ms": 1234567890
  }
}
```

Operation codes: `c` = create, `u` = update, `d` = delete, `r` = read (snapshot).

### Topic Naming

Events are published to topics following this pattern:

```
<topic.prefix>.<schema_name>.<table_name>
```

Example: `yashandb.SCHEMA1.TABLE1`

## Monitoring

JMX metrics are available at:

```
io.debezium.connector.yashandb:type=connector-metrics,context=snapshot,server=<connector_name>
io.debezium.connector.yashandb:type=connector-metrics,context=streaming,server=<connector_name>
```

- **Snapshot Metrics**: Snapshot progress, table counts, row counts
- **Streaming Metrics**: SCN positions, transaction counts, replication lag

## Features

- **Real-time CDC**: Captures insert, update, and delete operations
- **Snapshot Support**: Initial table data snapshots before streaming
- **Schema Evolution**: Tracks schema changes in a dedicated history topic
- **Exactly-Once Delivery**: Supports exactly-once semantics
- **Partitioned Table Support**: Handles regular and partitioned tables
- **Incremental Snapshots**: Signal-based incremental snapshot support

## Project Structure

```
src/main/java/io/debezium/connector/yashandb/
├── YashanDbConnector.java              # Main connector class
├── YashanDbConnectorConfig.java        # Configuration definitions
├── YashanDbConnectorTask.java          # Task implementation
├── YashanDbConnection.java             # Database connection handling
├── YashanDbSnapshotChangeEventSource.java  # Snapshot processing
├── YashanDbOffsetContext.java          # Offset management
├── YashanDbDatabaseSchema.java         # Schema management
├── YashanDbValueConverters.java        # Data type conversion
├── ystream/                            # YStream integration
│   ├── YStreamAdapter.java
│   ├── YStreamEventHandler.java
│   └── ...
├── antlr/listener/                     # DDL parsing listeners
├── converters/                         # Data converters
├── metadata/                           # Metadata providers
└── outbox/                             # Outbox support
```

## Development

```bash
# Run unit tests
mvn test

# Run tests with coverage report
mvn verify
```

## Known Limitations

- Connector is in **incubating** status
- Only a single connector task is supported per instance
- YStream must be properly configured on the YashanDB server

## Contributing

Please submit issues and pull requests through the [Debezium GitHub repository](https://github.com/debezium/debezium).

## License

Apache License, Version 2.0. See [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) for details.

## Resources

- [Debezium Documentation](https://debezium.io/documentation/)
- [YashanDB Documentation](https://doc.yashandb.com/)
- [Debezium Community](https://debezium.io/community/)
- [GitHub Issues](https://github.com/debezium/dbz/issues)
