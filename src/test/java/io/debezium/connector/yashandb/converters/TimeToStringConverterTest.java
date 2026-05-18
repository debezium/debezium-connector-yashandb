/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link TimeToStringConverter}.
 */
class TimeToStringConverterTest {

    @Test
    void shouldConfigureWithDefaultFormat() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldConfigureWithCustomFormat() {
        Properties props = new Properties();
        props.setProperty("format", "HH:mm:ss");
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(props);
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldConfigureWithSelector() {
        Properties props = new Properties();
        props.setProperty("selector", "TEST_TABLE.*");
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(props);
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldReturnConfigFields() {
        TimeToStringConverter converter = new TimeToStringConverter();
        // Verify getConfigFields returns non-null and non-empty Field.Set
        io.debezium.config.Field.Set configFields = converter.getConfigFields();
        assertThat(configFields).isNotNull();
        // Field.Set is an Iterable, verify it's not empty by checking iterator
        assertThat(configFields.iterator().hasNext()).isTrue();
    }

    @Test
    void shouldNotRegisterConverterForNonTimeColumn() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("VARCHAR", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNull();
    }

    @Test
    void shouldRegisterConverterForTimeColumn() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIME", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldConvertTimeToString() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIME", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Time time = Time.valueOf("10:30:45");
        Object result = conv.convert(time);
        assertThat(result).isEqualTo("10:30:45.000000");
    }

    @Test
    void shouldConvertLongEpochMicrosToString() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIME", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        // 10:30:45.000000 in microseconds from epoch
        long epochMicros = 10L * 3600 * 1000000 + 30L * 60 * 1000000 + 45L * 1000000;
        Object result = conv.convert(epochMicros);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(String.class);
    }

    @Test
    void shouldConvertTimestampToString() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIME", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45");
        Object result = conv.convert(timestamp);
        assertThat(result).isEqualTo("10:30:45.000000");
    }

    @Test
    void shouldHandleNullValueForOptionalColumn() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIME", true);
        CustomConverter.Converter conv = getConverter(converter, column);

        Object result = conv.convert(null);
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnStringAsIs() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("TIME", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Object result = conv.convert("already-a-string");
        assertThat(result).isEqualTo("already-a-string");
    }

    @Test
    void shouldUseCustomFormat() {
        Properties props = new Properties();
        props.setProperty("format", "HH:mm");
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(props);

        RelationalColumn column = mockColumn("TIME", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Time time = Time.valueOf("10:30:45");
        Object result = conv.convert(time);
        assertThat(result).isEqualTo("10:30");
    }

    @Test
    void shouldHandleCaseInsensitiveTypeName() {
        TimeToStringConverter converter = new TimeToStringConverter();
        converter.configure(new Properties());

        // Test lowercase "time"
        RelationalColumn column = mockColumn("time", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    private RelationalColumn mockColumn(String typeName, boolean optional) {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn(typeName);
        when(column.dataCollection()).thenReturn("TEST_TABLE");
        when(column.name()).thenReturn("CREATE_TIME");
        when(column.isOptional()).thenReturn(optional);
        when(column.hasDefaultValue()).thenReturn(false);
        return column;
    }

    private CustomConverter.Converter getConverter(TimeToStringConverter converter, RelationalColumn column) {
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();
        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));
        return converterRef.get();
    }
}
