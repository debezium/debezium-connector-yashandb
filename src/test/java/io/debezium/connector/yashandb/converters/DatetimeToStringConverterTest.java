/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.converters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.debezium.spi.converter.CustomConverter;
import io.debezium.spi.converter.RelationalColumn;

/**
 * Unit tests for {@link DatetimeToStringConverter}.
 */
class DatetimeToStringConverterTest {

    @Test
    void shouldPreserveMicrosecondsAndIgnoreSystemDefaultTimeZoneForLongValue() {
        DatetimeToStringConverter converter = new DatetimeToStringConverter();
        converter.configure(new Properties());

        RelationalColumn column = mock(RelationalColumn.class);
        when(column.typeName()).thenReturn("DATETIME(6)");
        when(column.isOptional()).thenReturn(false);
        when(column.hasDefaultValue()).thenReturn(false);

        AtomicReference<CustomConverter.Converter> converterRef = new AtomicReference<>();
        converter.converterFor(column, (schema, conv) -> converterRef.set(conv));

        TimeZone originalTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"));

            Object result = converterRef.get().convert(1705314645123456L);

            assertThat(result).isEqualTo("2024-01-15 10:30:45.123456");
        }
        finally {
            TimeZone.setDefault(originalTimeZone);
        }
    }
}
