/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

/**
 * Unit tests for {@link YashanDbStreamingChangeEventSourceMetrics}.
 */
class YashanDbStreamingChangeEventSourceMetricsTest {

    @Test
    void shouldCreateConnectorConfig() {
        final Configuration configuration = Configuration.create()
                .with("database.hostname", "localhost")
                .with("database.port", 1688)
                .with("database.user", "sys")
                .with("database.password", "123456")
                .with("database.database", "yashandb")
                .with("database.dbname", "yashandb")
                .with("connector.class", YashanDbConnector.class)
                .with("topic.prefix", "test")
                .with("schema.history.internal", "io.debezium.storage.file.history.FileSchemaHistory")
                .with("schema.history.internal.file.filename", "test.schema-history.dat")
                .build();

        YashanDbConnectorConfig connectorConfig = new YashanDbConnectorConfig(configuration);
        assertThat(connectorConfig).isNotNull();
    }

    @Test
    void shouldHaveMXBeanInterface() {
        // Verify MXBean interface is defined
        Class<?> mxbeanClass = YashanDbStreamingChangeEventSourceMetricsMXBean.class;
        assertThat(mxbeanClass).isNotNull();

        // Verify it extends the base MXBean
        assertThat(mxbeanClass.getInterfaces()).isNotEmpty();
        assertThat(mxbeanClass.getInterfaces()[0].getName()).contains("StreamingChangeEventSourceMetricsMXBean");
    }

    @Test
    void shouldHaveMetricsClass() {
        // Verify metrics class exists and has correct inheritance
        assertThat(YashanDbStreamingChangeEventSourceMetrics.class.getSuperclass().getName())
                .contains("DefaultStreamingChangeEventSourceMetrics");
    }

    @Test
    void shouldImplementMXBean() {
        // Verify YashanDbStreamingChangeEventSourceMetrics implements the MXBean interface
        assertThat(YashanDbStreamingChangeEventSourceMetricsMXBean.class)
                .isAssignableFrom(YashanDbStreamingChangeEventSourceMetrics.class);
    }
}
