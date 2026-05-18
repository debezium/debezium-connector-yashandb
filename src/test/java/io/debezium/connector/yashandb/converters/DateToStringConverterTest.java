/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link DateToStringConverter}.
 */
class DateToStringConverterTest {

    @Test
    void shouldConfigureWithDefaultFormat() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());
        // No exception means configuration passed
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldConfigureWithCustomFormat() {
        Properties props = new Properties();
        props.setProperty("format", "yyyy/MM/dd");
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(props);
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldConfigureWithSelector() {
        Properties props = new Properties();
        props.setProperty("selector", "TEST_TABLE.*");
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(props);
        assertThat(converter).isNotNull();
    }

    @Test
    void shouldReturnConfigFields() {
        DateToStringConverter converter = new DateToStringConverter();
        // Verify getConfigFields returns non-null and non-empty Field.Set
        io.debezium.config.Field.Set configFields = converter.getConfigFields();
        assertThat(configFields).isNotNull();
        // Field.Set is an Iterable, verify it's not empty by checking iterator
        assertThat(configFields.iterator().hasNext()).isTrue();
    }

    @Test
    void shouldNotRegisterConverterForNonDateColumn() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("VARCHAR", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        // No converter registered for non-DATE column
        assertThat(converterRef.get()).isNull();
    }

    @Test
    void shouldRegisterConverterForDateColumn() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", false);
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();

        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        assertThat(converterRef.get()).isNotNull();
    }

    @Test
    void shouldConvertDateToString() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        // Test with java.sql.Date
        Date date = Date.valueOf("2024-01-15");
        Object result = conv.convert(date);
        assertThat(result).isEqualTo("2024-01-15");
    }

    @Test
    void shouldConvertLongEpochMicrosToString() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        // 1705276800000L = 2024-01-15 00:00:00 UTC
        long epochMicros = 1705276800000L * 1000;
        Object result = conv.convert(epochMicros);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(String.class);
    }

    @Test
    void shouldConvertTimestampToString() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Timestamp timestamp = Timestamp.valueOf("2024-01-15 10:30:45");
        Object result = conv.convert(timestamp);
        assertThat(result).isEqualTo("2024-01-15");
    }

    @Test
    void shouldHandleNullValueForOptionalColumn() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", true);
        CustomConverter.Converter conv = getConverter(converter, column);

        Object result = conv.convert(null);
        assertThat(result).isNull();
    }

    @Test
    void shouldReturnStringAsIs() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Object result = conv.convert("already-a-string");
        assertThat(result).isEqualTo("already-a-string");
    }

    @Test
    void shouldConvertUtilDateToString() {
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mockColumn("DATE", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        java.util.Date utilDate = new java.util.Date(1705276800000L);
        Object result = conv.convert(utilDate);
        assertThat(result).isNotNull();
        assertThat(result).isInstanceOf(String.class);
    }

    @Test
    void shouldUseCustomFormat() {
        Properties props = new Properties();
        props.setProperty("format", "yyyy/MM/dd");
        DateToStringConverter converter = new DateToStringConverter();
        converter.configure(props);

        RelationalColumn column = mockColumn("DATE", false);
        CustomConverter.Converter conv = getConverter(converter, column);

        Date date = Date.valueOf("2024-01-15");
        Object result = conv.convert(date);
        assertThat(result).isEqualTo("2024/01/15");
    }

    private RelationalColumn mockColumn(String typeName, boolean optional) {
        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn(typeName);
        when(column.dataCollection()).thenReturn("TEST_TABLE");
        when(column.name()).thenReturn("CREATE_DATE");
        when(column.isOptional()).thenReturn(optional);
        when(column.hasDefaultValue()).thenReturn(false);
        return column;
    }

    private CustomConverter.Converter getConverter(DateToStringConverter converter, RelationalColumn column) {
        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();
        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));
        return converterRef.get();
    }
}
