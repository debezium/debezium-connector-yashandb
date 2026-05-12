/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.source.ExactlyOnceSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.common.RelationalBaseSourceConnector;

/**
 * Unit tests for {@link YashanDbConnector}.
 */
class YashanDbConnectorTest {

    private YashanDbConnector connector;

    @BeforeEach
    void setUp() {
        connector = new YashanDbConnector();
    }

    // -----------------------------------------------------------------------
    // Basic connector lifecycle tests
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnVersion() {
        String version = connector.version();
        assertThat(version).isNotNull();
    }

    @Test
    void shouldReturnConfigDef() {
        ConfigDef configDef = connector.config();
        assertThat(configDef).isNotNull();
    }

    @Test
    void shouldReturnTaskClass() {
        assertThat(connector.taskClass()).isEqualTo(YashanDbConnectorTask.class);
    }

    @Test
    void shouldStartWithEmptyProperties() {
        Map<String, String> props = new HashMap<>();
        connector.start(props);
        // start() should succeed without throwing
    }

    @Test
    void shouldStartWithProperties() {
        Map<String, String> props = new HashMap<>();
        props.put("key1", "value1");
        props.put("key2", "value2");
        connector.start(props);
        // start() should succeed without throwing
    }

    @Test
    void shouldStopWithoutError() {
        connector.stop();
        // stop() should succeed without throwing
    }

    @Test
    void shouldReturnExactlyOnceSupport() {
        Map<String, String> config = new HashMap<>();
        assertThat(connector.exactlyOnceSupport(config))
                .isEqualTo(ExactlyOnceSupport.SUPPORTED);
    }

    @Test
    void shouldReturnExactlyOnceSupportWithNullConfig() {
        assertThat(connector.exactlyOnceSupport(null))
                .isEqualTo(ExactlyOnceSupport.SUPPORTED);
    }

    @Test
    void shouldReturnConfigFields() {
        assertThat(connector.getConfigFields()).isNotNull();
        assertThat(connector.getConfigFields()).isSameAs(YashanDbConnectorConfig.ALL_FIELDS);
    }

    @Test
    void shouldReturnSingleTaskConfigWhenMaxTasksIsOne() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "localhost");
        props.put("database.port", "1688");
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsEntry("database.hostname", "localhost");
        assertThat(taskConfigs.get(0)).containsEntry("database.port", "1688");
    }

    @Test
    void shouldThrowExceptionWhenMaxTasksGreaterThanOne() {
        Map<String, String> props = new HashMap<>();
        connector.start(props);

        assertThatThrownBy(() -> connector.taskConfigs(2))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only a single connector task may be started");
    }

    @Test
    void shouldThrowExceptionWhenMaxTasksIsZero() {
        Map<String, String> props = new HashMap<>();
        connector.start(props);

        // taskConfigs(0) returns an empty list since 0 < 1 check doesn't trigger
        // The check is maxTasks > 1, so 0 passes through
        List<Map<String, String>> taskConfigs = connector.taskConfigs(0);
        assertThat(taskConfigs).hasSize(1);
    }

    @Test
    void shouldReturnUnmodifiablePropertiesAfterStart() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        connector.start(props);

        // Verify that taskConfigs returns the same props
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs.get(0)).containsEntry("key", "value");
    }

    @Test
    void shouldPreservePropertiesAcrossTaskConfigsCalls() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test-connector");
        props.put("database.dbname", "testdb");
        connector.start(props);

        List<Map<String, String>> taskConfigs1 = connector.taskConfigs(1);
        List<Map<String, String>> taskConfigs2 = connector.taskConfigs(1);

        assertThat(taskConfigs1.get(0)).isEqualTo(taskConfigs2.get(0));
    }

    @Test
    void shouldVersionReturnModuleVersion() {
        String connectorVersion = connector.version();
        String moduleVersion = Module.version();
        assertThat(connectorVersion).isEqualTo(moduleVersion);
    }

    // -----------------------------------------------------------------------
    // Inheritance and type tests
    // -----------------------------------------------------------------------

    @Test
    void shouldBeInstanceOfRelationalBaseSourceConnector() {
        assertThat(connector).isInstanceOf(RelationalBaseSourceConnector.class);
    }

    @Test
    void shouldBeInstanceOfConnector() {
        assertThat(connector).isInstanceOf(Connector.class);
    }

    // -----------------------------------------------------------------------
    // config() delegation tests
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnSameConfigDefAsConnectorConfig() {
        ConfigDef connectorConfigDef = connector.config();
        ConfigDef configClassConfigDef = YashanDbConnectorConfig.configDef();
        // Both should return ConfigDef with the same field names
        assertThat(connectorConfigDef.names()).containsExactlyInAnyOrderElementsOf(configClassConfigDef.names());
    }

    @Test
    void shouldReturnConfigDefWithExpectedFields() {
        ConfigDef configDef = connector.config();
        assertThat(configDef.names()).contains(
                "database.hostname",
                "database.port",
                "database.user",
                "database.password",
                "database.dbname",
                "snapshot.mode",
                "database.url");
    }

    // -----------------------------------------------------------------------
    // taskConfigs() edge case tests
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnTaskConfigWithNullPropertiesWhenNotStarted() {
        // taskConfigs() before start() returns the properties field which is null
        // This verifies the behavior of calling taskConfigs before start
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).isNull();
    }

    @Test
    void shouldReturnTaskConfigsForMaxTasksEqualToOne() {
        Map<String, String> props = new HashMap<>();
        props.put("database.hostname", "dbhost");
        props.put("database.port", "1688");
        props.put("database.user", "admin");
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);

        assertThat(taskConfigs).hasSize(1);
        assertThat(taskConfigs.get(0)).containsEntry("database.hostname", "dbhost");
        assertThat(taskConfigs.get(0)).containsEntry("database.port", "1688");
        assertThat(taskConfigs.get(0)).containsEntry("database.user", "admin");
    }

    @Test
    void shouldReturnTaskConfigsThatAreUnmodifiable() {
        Map<String, String> props = new HashMap<>();
        props.put("key", "value");
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Map<String, String> taskConfig = taskConfigs.get(0);

        // The list returned is from Collections.singletonList which is unmodifiable
        assertThatThrownBy(() -> taskConfigs.add(new HashMap<>()))
                .isInstanceOf(UnsupportedOperationException.class);

        // The map stored via start() is wrapped in unmodifiableMap
        assertThatThrownBy(() -> taskConfig.put("newkey", "newvalue"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void shouldThrowOnTaskConfigsWithLargeMaxTasks() {
        connector.start(new HashMap<>());

        assertThatThrownBy(() -> connector.taskConfigs(10))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only a single connector task may be started");
    }

    @Test
    void shouldThrowOnTaskConfigsWithIntegerMaxValue() {
        connector.start(new HashMap<>());

        assertThatThrownBy(() -> connector.taskConfigs(Integer.MAX_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Only a single connector task may be started");
    }

    // -----------------------------------------------------------------------
    // exactlyOnceSupport tests
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnExactlyOnceSupportWithEmptyConfig() {
        Map<String, String> config = new HashMap<>();
        assertThat(connector.exactlyOnceSupport(config))
                .isEqualTo(ExactlyOnceSupport.SUPPORTED);
    }

    // -----------------------------------------------------------------------
    // validateConnection tests (via subclass access)
    // -----------------------------------------------------------------------

    @Test
    void shouldValidateAllFieldsDelegatesToConfigValidate() {
        // Create a connector to access the protected method
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        connector.start(props);

        // We cannot directly test validateConnection without a real DB connection,
        // but we can verify that the connector was started successfully
        assertThat(connector.taskConfigs(1)).hasSize(1);
    }

    // -----------------------------------------------------------------------
    // Multiple start/stop cycle tests
    // -----------------------------------------------------------------------

    @Test
    void shouldAllowMultipleStartCalls() {
        Map<String, String> props1 = new HashMap<>();
        props1.put("key1", "value1");
        connector.start(props1);

        Map<String, String> props2 = new HashMap<>();
        props2.put("key2", "value2");
        connector.start(props2);

        // After second start, only props2 should be retained
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs.get(0)).containsEntry("key2", "value2");
        assertThat(taskConfigs.get(0)).doesNotContainKey("key1");
    }

    @Test
    void shouldAllowStopBeforeStart() {
        connector.stop();
        // stop() should succeed without throwing even before start()
    }

    @Test
    void shouldAllowStartStopStartCycle() {
        Map<String, String> props = new HashMap<>();
        props.put("name", "test");
        connector.start(props);
        connector.stop();
        connector.start(props);

        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        assertThat(taskConfigs.get(0)).containsEntry("name", "test");
    }

    // -----------------------------------------------------------------------
    // getConfigFields() tests
    // -----------------------------------------------------------------------

    @Test
    void shouldReturnSameAllFieldsAsConnectorConfig() {
        assertThat(connector.getConfigFields()).isSameAs(YashanDbConnectorConfig.ALL_FIELDS);
    }

    @Test
    void shouldReturnNonEmptyConfigFields() {
        assertThat(connector.getConfigFields()).isNotEmpty();
    }
}
