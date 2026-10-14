# Debezium Connector for YashanDB

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

An incubating Debezium CDC (Change Data Capture) connector for [YashanDB](https://yashandb.com/). This connector enables real-time data streaming from YashanDB databases to Apache Kafka.

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

# Build and run all tests
mvn install
```

### Testing

This module contains unit tests. A *unit test* is a JUnit test class named `*Test.java` or `Test*.java` that never requires or uses external services, though it can use the file system and can run any components within the same JVM process. They should run very quickly, be independent of each other, and clean up after itself.

Running unit tests:

```bash
# Run unit tests
mvn test

# Run a specific test class
mvn test -Dtest=YashanDbConnectorTest

# Run tests matching a pattern
mvn test -Dtest=YashanDb*Test
```

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
| `bigint.unsigned.handling.mode` | MySQL-mode `BIGINT UNSIGNED` representation: `precise` (scale-zero Decimal), `long` (signed INT64), `double` (FLOAT64), `string` (decimal text) | `precise` |
| `legacy.decimal.handling.strategy` | Legacy decimal handling strategy | - |

#### YStream MySQL mode

The connector uses native YashanDB syntax by default. To connect to a MySQL-compatible YashanDB database, set the `mysql.mode` YStream option:

```json
{
  "ystream.additional.properties.mysql.mode": "true"
}
```

This setting selects the YStream client mode, the MySQL dialect for connector-generated SQL and table identifiers, and the JDBC `compatVector=mysql` connection property. The connector does not inspect `COMPAT_VECTOR` to select a mode. Use a separate topic prefix, offsets, and schema history when changing the mode of an existing connector.

In MySQL mode, a database name is the `schema` component of the connector's table identifier, so a table is identified as `database.table`. Use `schema.include.list` and `schema.exclude.list` to filter databases. Do not use `database.include.list` or `database.exclude.list` for MySQL-mode database filtering.

In MySQL mode, integer columns declared `UNSIGNED` or `ZEROFILL` use wider Kafka Connect types so their full range is retained. `TINYINT UNSIGNED` maps to `INT16`, `SMALLINT UNSIGNED` and `MEDIUMINT UNSIGNED` to `INT32`, and `INT UNSIGNED` to `INT64`. `BIGINT UNSIGNED` maps to a scale-zero Connect `Decimal` by default, including values above Java's signed `long` maximum. Set `bigint.unsigned.handling.mode=long` for a signed `INT64` schema; values above `Long.MAX_VALUE` then cannot be represented correctly. Set it to `double` for an approximate `FLOAT64` value, or `string` for the exact unsigned decimal value as text. Signed values returned by YStream are restored according to the source integer width; already-positive JDBC snapshot values are preserved. This mapping is specific to MySQL mode and does not depend on `decimal.handling.mode`.

MySQL-mode `TIME` columns use a Kafka Connect `STRING` schema in snapshots and streaming. This preserves durations beyond 24 hours, including values such as `838:59:59` and negative values. Existing consumers of these columns need to accept the string schema.

MySQL mode parses incremental DDL with the Debezium MySQL ANTLR grammar. Supported changes include `CREATE TABLE`, `ALTER TABLE` (adding, modifying, renaming, reordering, and dropping columns, and changing primary keys), `RENAME TABLE`, `DROP TABLE`, and `TRUNCATE TABLE`. Parsed table definitions use the same `database.table` identifiers as snapshots and streaming, and are recorded in schema history. `TRUNCATE TABLE` uses the existing truncate event handling and `skipped.operations` setting.

The MySQL grammar files are copied from Debezium's `debezium-ddl-parser/src/main/antlr4/io/debezium/ddl/parser/mysql/legacy/` (`MySqlLexer.g4` and `MySqlParser.g4`), with their original license notices preserved. The connector generates them under its own package to coexist with the native YashanDB parser.

The container-backed MySQL-mode integration test is documented in [YashanDB MySQL mode integration](docs/yashandb-mysql-integration.md). Run it with `-Pmysql-integration-tests` so the YashanDB image is initialized with `--mode mysql`.

Additional YStream builder options use the same `ystream.additional.properties.` prefix, with one connector property per option. For example, `"ystream.additional.properties.enable.sequence.next.val": "true"` maps to the YStream `setEnableSequenceNextVal(true)` builder call. Dedicated connector settings take precedence when they configure the same YStream option. Replace the former comma-separated `ystream.additional.properties` setting with individually prefixed properties when upgrading.

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
