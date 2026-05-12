/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link YashanDbPartition}.
 */
class YashanDbPartitionTest {

    @Test
    void shouldCreatePartitionWithServerName() {
        YashanDbPartition partition = new YashanDbPartition("server1", "db1");
        assertThat(partition).isNotNull();
    }

    @Test
    void shouldReturnSourcePartition() {
        YashanDbPartition partition = new YashanDbPartition("server1", "db1");
        Map<String, String> sourcePartition = partition.getSourcePartition();
        assertThat(sourcePartition).containsEntry("server", "server1");
    }

    @Test
    void shouldCheckEqualityByServerName() {
        YashanDbPartition p1 = new YashanDbPartition("server1", "db1");
        YashanDbPartition p2 = new YashanDbPartition("server1", "db2");
        assertThat(p1.equals(p2)).isTrue();
    }

    @Test
    void shouldCheckInequalityByServerName() {
        YashanDbPartition p1 = new YashanDbPartition("server1", "db1");
        YashanDbPartition p2 = new YashanDbPartition("server2", "db1");
        assertThat(p1.equals(p2)).isFalse();
    }

    @Test
    void shouldCheckReflexiveEquality() {
        YashanDbPartition p = new YashanDbPartition("server1", "db1");
        assertThat(p.equals(p)).isTrue();
    }

    @Test
    void shouldCheckNullInequality() {
        YashanDbPartition p = new YashanDbPartition("server1", "db1");
        assertThat(p.equals(null)).isFalse();
    }

    @Test
    void shouldCheckDifferentClassInequality() {
        YashanDbPartition p = new YashanDbPartition("server1", "db1");
        assertThat(p.equals("server1")).isFalse();
    }

    @Test
    void shouldReturnConsistentHashCode() {
        YashanDbPartition p1 = new YashanDbPartition("server1", "db1");
        YashanDbPartition p2 = new YashanDbPartition("server1", "db2");
        assertThat(p1.hashCode()).isEqualTo(p2.hashCode());
    }

    @Test
    void shouldReturnToString() {
        YashanDbPartition p = new YashanDbPartition("server1", "db1");
        String str = p.toString();
        assertThat(str).contains("YashanDbPartition");
        assertThat(str).contains("server1");
    }

    // Note: Provider test requires YashanDbConnectorConfig which has a schema conflict bug
    // The Provider simply creates a singleton partition from config, tested above via direct construction
}
