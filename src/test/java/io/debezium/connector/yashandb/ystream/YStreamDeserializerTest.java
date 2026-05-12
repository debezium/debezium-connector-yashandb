/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sics.ystream.exception.YstreamException;
import com.sics.ystream.metadata.TableMetadata;
import com.sics.ystream.result.YstreamLcrInterface;

/**
 * Unit tests for {@link YStreamDeserializer}.
 */
class YStreamDeserializerTest {

    private YStreamDeserializer deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new YStreamDeserializer();
    }

    @Test
    void shouldCreateDeserializer() {
        assertThat(deserializer).isNotNull();
    }

    @Test
    void shouldDeserializeLcrIntoRecord() throws YstreamException {
        YstreamLcrInterface lcr = mock(YstreamLcrInterface.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);

        YStreamRecord record = deserializer.deserialize(lcr, tableMetadata);

        assertThat(record).isNotNull();
        assertThat(record.getYstreamLcrInterface()).isSameAs(lcr);
        assertThat(record.getTableMetadata()).isSameAs(tableMetadata);
    }

    @Test
    void shouldDeserializeWithNullTableMetadata() throws YstreamException {
        YstreamLcrInterface lcr = mock(YstreamLcrInterface.class);

        YStreamRecord record = deserializer.deserialize(lcr, null);

        assertThat(record).isNotNull();
        assertThat(record.getYstreamLcrInterface()).isSameAs(lcr);
        assertThat(record.getTableMetadata()).isNull();
    }

    @Test
    void shouldDeserializeWithNullLcr() throws YstreamException {
        TableMetadata tableMetadata = mock(TableMetadata.class);

        YStreamRecord record = deserializer.deserialize(null, tableMetadata);

        assertThat(record).isNotNull();
        assertThat(record.getYstreamLcrInterface()).isNull();
        assertThat(record.getTableMetadata()).isSameAs(tableMetadata);
    }

    @Test
    void shouldDeserializeWithBothNull() throws YstreamException {
        YStreamRecord record = deserializer.deserialize(null, null);

        assertThat(record).isNotNull();
        assertThat(record.getYstreamLcrInterface()).isNull();
        assertThat(record.getTableMetadata()).isNull();
    }
}
