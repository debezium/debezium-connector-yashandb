/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Time;
import java.sql.Types;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.sics.ystream.result.YstreamColumn;
import com.sics.ystream.result.YstreamColumns;
import com.yashandb.protocol.accessor.Accessor;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;

class YStreamChangeRecordEmitterTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldReadMysqlTimeAsStringAndKeepNativeTimeConversion() throws Exception {
        byte[] bytes = { 1, 2, 3 };
        Accessor accessor = mock(Accessor.class);
        when(accessor.getString(bytes)).thenReturn("838:59:59");
        when(accessor.getTime(bytes, null)).thenReturn(Time.valueOf("10:30:45"));

        com.sics.ystream.metadata.Column metadataColumn = mock(com.sics.ystream.metadata.Column.class);
        when(metadataColumn.getColumnName()).thenReturn("duration");
        when(metadataColumn.getDataType()).thenReturn(Types.TIME);
        YstreamColumn value = new YstreamColumn(1, false, bytes);
        value.setColumn(metadataColumn);
        value.setAccessor(accessor);

        Column column = Column.editor().name("duration").type("time").jdbcType(Types.TIME).position(1).create();
        Table table = mock(Table.class);
        when(table.columns()).thenReturn(List.of(column));
        when(table.columnWithName("duration")).thenReturn(column);

        Object[] values = YStreamChangeRecordEmitter.getColumnValues(
                table, new YstreamColumns(List.of(value)), Collections.emptyMap(), true);
        Object[] nativeValues = YStreamChangeRecordEmitter.getColumnValues(
                table, new YstreamColumns(List.of(value)), Collections.emptyMap(), false);

        assertThat(values).containsExactly("838:59:59");
        assertThat(nativeValues).containsExactly(Time.valueOf("10:30:45"));
        verify(accessor).getString(bytes);
        verify(accessor).getTime(bytes, null);
    }
}
