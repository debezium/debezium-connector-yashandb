/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link TimestampToStringConverter}.
 */
class TimestampToStringConverterTest {

    @Test
    void shouldConfigureWithDefaultFormat() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldConfigureWithCustomFormat() {
        Properties props = new Properties();
        props.setProperty("format", "yyyy-MM-dd HH:mm:ss");
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(props);
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldConfigureWithSelector() {
        Properties props = new Properties();
        props.setProperty("selector", "TEST_TABLE.*");
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(props);
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldReturnConfigFields() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        // Verify getConfigFields returns non-null and non-empty Field.Set
        io.debezium.config.Field.Set configFields = converter.getConfigFields();
        assertThat(configFields).isNotNull();
        // Field.Set is an Iterable, verify it's not empty by checking iterator
        assertThat(configFields.iterator().hasNext()).isTrue();
    }

    @Test
    void shouldNotRegisterConverterForNonTimestampColumn() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("VARCHAR", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNull();
    }

    @Test
    void shouldRegisterConverterForTimestampColumn() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldRegisterConverterForTimestamp0() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP(0)", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldRegisterConverterForTimestamp6() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP(6)", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldConvertTimestampToString() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123456");
        Object result = conv.convert(timestamp);
        assertThat(result).isEqualTo("2024-01-15 10:30:45.123456");
    }

    @Test
    void shouldConvertLongEpochMicrosToString() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        // 2024-01-15 10:30:45.123456 UTC in microseconds
        long epochMicros = 1705315845123456L;
        Object result = conv.convert(epochMicros);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(String.class);
    }

    @Test
    void shouldReturnStringAsIs() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Object result = conv.convert("already-a-string");
        assertThat(result).isEqualTo("already-a-string");
    }

    @Test
    void shouldHandleNullValueForOptionalColumn() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP", true);
        CustomConverter.Converter conv = getConverter(converter, column);

        Object result = conv.convert(null);
        assertThat(result).isNull();
    }

    @Test
    void shouldUseCustomFormat() {
        Properties props = new Properties();
        props.setProperty("format", "yyyy-MM-dd HH:mm:ss");
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(props);

        RelationalColumn column = mockColumn("TIMESTAMP", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123456");
        Object result = conv.convert(timestamp);
        assertThat(result).isEqualTo("2024-01-15 10:30:45");
    }

    @Test
    void shouldHandleCaseInsensitiveTypeName() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        // Test lowercase "timestamp"
        RelationalColumn column = mockColumn("timestamp", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldHandleTimestampWithSpaces() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        // Test TIMESTAMP with spaces like "TIMESTAMP ( 6 )"
        RelationalColumn column = mockColumn("TIMESTAMP ( 6 )", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldUseDefaultFormat() {
        TimestampToStringConverter converter = new TimestampToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIMESTAMP", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45.123");
        Object result = conv.convert(timestamp);
        // Default format is "yyyy-MM-dd HH:mm:ss.SSSSSS"
        assertThat(result).isEqualTo("2024-01-15 10:30:45.123000");
    }

    private RelationalColumn mockColumn(String typeName, boolean optional) {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn(typeName);
        when(column.dataCollection()).thenReturn("TEST_TABLE");
        when(column.name()).thenReturn("CREATE_TIMESTAMP");
        when(column.isOptional()).thenReturn(optional);
        when(column.hasDefaultValue()).thenReturn(false);
        return column;
    }

    private CustomConverter.Converter getConverter(TimestampToStringConverter converter, RelationalColumn column) {
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();
        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));
        return converterRef.get();
    }
}
