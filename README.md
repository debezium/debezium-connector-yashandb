# Debezium Connector for YashanDB

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

An incubating Debezium CDC (Change Data Capture) connector for [YashanDB](https://www.yasdb.com/). This connector enables real-time data streaming from YashanDB databases to Apache Kafka.

## Overview

YashanDB is a distributed database system developed by Yashan Technologies. This connector captures row-level changes that insert, update, and delete records in a YashanDB database, and delivers those change events to Kafka topics.

The connector is based on Debezium 3.6.0 and leverages YashanDB's YStream technology for efficient change data capture.

## Features

- **Real-time CDC**: Captures insert, update, and delete operations from YashanDB tables
- **Snapshot Support**: Performs initial snapshots of table data before streaming changes
- **Schema Evolution**: Tracks and records schema changes in a dedicated history topic
- **Exactly-Once Delivery**: Supports exactly-once semantics for reliable data delivery
- **Partitioned Table Support**: Handles both regular and partitioned tables
- **Incremental Snapshots**: Supports signal-based incremental snapshot functionality

## Prerequisites

- Java 11 or higher
- Apache Kafka 3.6.x with Kafka Connect
- YashanDB database with YStream enabled
- YashanDB JDBC driver (version 1.9.24 or compatible)

## Building

```bash
# Clone the repository
git clone https://github.com/debezium/debezium-connector-yashandb.git
cd debezium-connector-yashandb

# Build with Maven
mvn clean package -DskipTests

# The connector JAR will be in target/debezium-connector-yashandb-3.6.0-SNAPSHOT.jar
```

## Deployment

### Kafka Connect Deployment

1. Copy the connector JAR along with dependencies to your Kafka Connect plugin directory:

```bash
# Create plugin directory
mkdir -p $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yashandb

# Copy connector JAR
cp target/debezium-connector-yashandb-3.6.0-SNAPSHOT.jar $KAFKA_CONNECT_PLUGINS_DIR/debezium-connector-yashandb/
```

2. Ensure YashanDB JDBC driver and YStream library are available in the plugin directory or Kafka Connect's classpath.

3. Restart Kafka Connect to load the new connector.

## Configuration

### Required Configuration Properties

| Property | Description | Default |
|----------|-------------|---------|
| `name` | Unique name for the connector instance | - |
| `connector.class` | Connector class name | `io.debezium.connector.yashandb.YashanDBConnector` |
| `database.hostname` | YashanDB server hostname | - |
| `database.port` | YashanDB server port | `1688` |
| `database.user` | Database user name | - |
| `database.password` | Database password | - |
| `database.dbname` | Database name to capture changes from | - |
| `database.ystream.server.name` | YStream server name | - |
| `topic.prefix` | Topic name prefix for all events | - |

### Optional Configuration Properties

#### Snapshot Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `snapshot.mode` | Snapshot mode: `initial`, `initial_only`, `schema_only`, `schema_only_recovery`, `always` | `initial` |
| `snapshot.locking.mode` | Locking mode during snapshot: `shared`, `none` | `shared` |
| `snapshot.enhance.predicate.scn` | Token for snapshot predicate enhancement | - |
| `snapshot.database.errors.max.retries` | Max retries for snapshot database errors | `0` |

#### Log Mining Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `log.mining.batch.size.default` | Default batch size for reading redo/archive logs | `20000` |
| `log.mining.batch.size.min` | Minimum batch size | `1000` |
| `log.mining.batch.size.max` | Maximum batch size | `100000` |
| `log.mining.transaction.retention.ms` | Transaction retention duration in milliseconds | `0` |
| `log.mining.archive.log.hours` | Hours of archive logs to mine | `0` |

#### Data Type Handling

| Property | Description | Default |
|----------|-------------|---------|
| `interval.handling.mode` | How to represent INTERVAL columns: `string`, `numeric` | `numeric` |
| `decimal.handling.mode` | How to represent DECIMAL columns: `precise`, `double`, `string` | `precise` |
| `legacy.decimal.handling.strategy` | Legacy decimal handling strategy | - |

### Example Configuration

```json
{
  "name": "yashandb-connector",
  "connector.class": "io.debezium.connector.yashandb.YashanDBConnector",
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

## Output Format

Each change event is structured as follows:

### Key Structure

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

### Value Structure

```json
{
  "schema": { ... },
  "payload": {
    "before": { ... },  // Previous state (null for inserts)
    "after": { ... },   // Current state (null for deletes)
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
    "op": "c",  // c=create, u=update, d=delete, r=read (snapshot)
    "ts_ms": 1234567890
  }
}
```

## Topic Naming

The connector produces events to Kafka topics with the following naming pattern:

```
<topic.prefix>.<schema_name>.<table_name>
```

For example, with `topic.prefix=yashandb`, changes to `SCHEMA1.TABLE1` would be published to:

```
yashandb.SCHEMA1.TABLE1
```

## Monitoring

The connector provides JMX metrics for monitoring:

- **Snapshot Metrics**: Track snapshot progress, table counts, and row counts
- **Streaming Metrics**: Monitor SCN positions, transaction counts, and lag

Access metrics via JMX at:

```
io.debezium.connector.yashandb:type=connector-metrics,context=snapshot,server=<connector_name>
io.debezium.connector.yashandb:type=connector-metrics,context=streaming,server=<connector_name>
```

## Known Limitations

- This connector is in **incubating** status and may have limitations
- Only a single connector task is supported per connector instance
- Requires YStream to be properly configured on the YashanDB server

## Development

### Project Structure

```
src/main/java/io/debezium/connector/yashandb/
├── YashanDBConnector.java           # Main connector class
├── YashanDBConnectorConfig.java     # Configuration definitions
├── YashanDBConnectorTask.java       # Task implementation
├── YashanDBConnection.java          # Database connection handling
├── YashanDBSnapshotChangeEventSource.java  # Snapshot processing
├── YashanDBOffsetContext.java       # Offset management
├── YashanDBDatabaseSchema.java      # Schema management
├── ystream/                         # YStream integration
│   ├── YStreamAdapter.java
│   └── YStreamEventHandler.java
└── ddl/parser/                      # DDL parsing
```

### Running Tests

```bash
mvn test
```

## Contributing

Please submit issues and pull requests through the [Debezium GitHub repository](https://github.com/debezium/debezium).

## License

This project is licensed under the Apache License, Version 2.0. See [LICENSE](http://www.apache.org/licenses/LICENSE-2.0) for details.

## Resources

- [Debezium Documentation](https://debezium.io/documentation/)
- [YashanDB Documentation](https://www.yasdb.com/docs/)
- [Debezium Community](https://debezium.io/community/)
- [GitHub Issues](https://github.com/debezium/dbz/issues)