/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Types;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;

/**
 * Unit tests for MySQL-mode value conversion and converter selection.
 */
class YashanDbMySqlValueConvertersTest {

    private final YashanDbConnection connection = mock(YashanDbConnection.class);

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSelectConverterFromMysqlMode() {
        YashanDbConnectorConfig nativeConfig = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "test")
                .build());

        assertThat(YashanDbConnectorTask.createValueConverters(nativeConfig, connection))
                .isExactlyInstanceOf(YashanDbValueConverters.class);
        assertThat(YashanDbConnectorTask.createValueConverters(mysqlConfig(), connection))
                .isInstanceOf(YashanDbMySqlValueConverters.class);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRepresentMysqlTimeAsStringIncludingDurationsBeyondOneDay() {
        YashanDbMySqlValueConverters converters = new YashanDbMySqlValueConverters(mysqlConfig(), connection);
        Column column = Column.editor().name("duration").type("time(6)").jdbcType(Types.TIME).optional(true).create();
        Schema schema = converters.schemaBuilder(column).optional().build();
        Field field = new Field("duration", 0, schema);

        assertThat(schema.type()).isEqualTo(Schema.Type.STRING);
        assertThat(converters.converter(column, field).convert("838:59:59")).isEqualTo("838:59:59");
        assertThat(converters.converter(column, field).convert("-838:59:59.123456")).isEqualTo("-838:59:59.123456");
        assertThat(converters.converter(column, field).convert(Time.valueOf("10:30:45"))).isEqualTo("10:30:45");
        assertThat(converters.converter(column, field).convert(null)).isNull();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldConvertUnsignedIntegerBoundaries() {
        YashanDbMySqlValueConverters converters = new YashanDbMySqlValueConverters(mysqlConfig(), connection);

        assertUnsigned(converters, "tinyint(3) unsigned", Types.TINYINT, -1, Schema.Type.INT16, (short) 255);
        assertUnsigned(converters, "smallint unsigned", Types.SMALLINT, -1, Schema.Type.INT32, 65535);
        assertUnsigned(converters, "mediumint(8) unsigned", Types.INTEGER, -1, Schema.Type.INT32, 16777215);
        assertUnsigned(converters, "int(10) unsigned", Types.INTEGER, -1, Schema.Type.INT64, 4294967295L);
        assertUnsigned(converters, "INTEGER ZEROFILL", Types.INTEGER, -1, Schema.Type.INT64, 4294967295L);
        assertUnsigned(converters, "int1 unsigned", Types.TINYINT, "255", Schema.Type.INT16, (short) 255);
        assertUnsigned(converters, "int8 unsigned", Types.BIGINT, -1L, Schema.Type.BYTES,
                new BigDecimal("18446744073709551615"));
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldPreserveUnsignedSnapshotValuesAndRejectOutOfRangeValues() {
        YashanDbMySqlValueConverters converters = new YashanDbMySqlValueConverters(mysqlConfig(), connection);

        assertUnsigned(converters, "tinyint unsigned", Types.TINYINT, 255, Schema.Type.INT16, (short) 255);
        assertUnsigned(converters, "int unsigned", Types.INTEGER, new BigDecimal("4294967295"),
                Schema.Type.INT64, 4294967295L);
        assertUnsigned(converters, "BIGINT(20) UNSIGNED ZEROFILL", Types.BIGINT,
                "18446744073709551615", Schema.Type.BYTES, new BigDecimal("18446744073709551615"));

        Column column = Column.editor().name("id").type("tinyint unsigned").jdbcType(Types.TINYINT).optional(true).create();
        Schema schema = converters.schemaBuilder(column).optional().build();
        Field field = new Field("id", 0, schema);
        assertThat(converters.converter(column, field).convert(null)).isNull();
        assertThatThrownBy(() -> converters.converter(column, field).convert(256))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldLeaveSignedIntegerConversionToNativeConverter() {
        YashanDbMySqlValueConverters converters = new YashanDbMySqlValueConverters(mysqlConfig(), connection);
        Column column = Column.editor().name("id").type("BIGINT").jdbcType(Types.BIGINT).create();
        Schema schema = converters.schemaBuilder(column).build();

        assertThat(schema.type()).isEqualTo(Schema.Type.INT64);
        assertThat(converters.converter(column, new Field("id", 0, schema)).convert(-1L)).isEqualTo(-1L);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSupportLongModeForUnsignedBigint() {
        YashanDbMySqlValueConverters converters = convertersWithUnsignedBigintMode("long");

        assertUnsigned(converters, "bigint unsigned", Types.BIGINT, 42L, Schema.Type.INT64, 42L);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSupportDoubleModeForUnsignedBigint() {
        YashanDbMySqlValueConverters converters = convertersWithUnsignedBigintMode("double");

        assertUnsigned(converters, "bigint unsigned", Types.BIGINT, -1L, Schema.Type.FLOAT64,
                18446744073709551615.0d);
        assertUnsigned(converters, "bigint unsigned", Types.BIGINT, "18446744073709551615", Schema.Type.FLOAT64,
                18446744073709551615.0d);
        assertUnsigned(converters, "int unsigned", Types.INTEGER, -1, Schema.Type.INT64, 4294967295L);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldSupportStringModeForUnsignedBigint() {
        YashanDbMySqlValueConverters converters = convertersWithUnsignedBigintMode("string");

        assertUnsigned(converters, "bigint unsigned", Types.BIGINT, -1L, Schema.Type.STRING,
                "18446744073709551615");
        assertUnsigned(converters, "bigint unsigned", Types.BIGINT, new BigDecimal("18446744073709551615"),
                Schema.Type.STRING, "18446744073709551615");
        assertUnsigned(converters, "int unsigned", Types.INTEGER, -1, Schema.Type.INT64, 4294967295L);

        Column column = Column.editor().name("id").type("bigint unsigned").jdbcType(Types.BIGINT).optional(true).create();
        Schema schema = converters.schemaBuilder(column).optional().build();
        Field field = new Field("id", 0, schema);
        assertThat(converters.converter(column, field).convert(null)).isNull();
        assertThatThrownBy(() -> converters.converter(column, field).convert("18446744073709551616"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private YashanDbMySqlValueConverters convertersWithUnsignedBigintMode(String mode) {
        Configuration configuration = Configuration.create()
                .with("topic.prefix", "test")
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("bigint.unsigned.handling.mode", mode)
                .build();
        return new YashanDbMySqlValueConverters(new YashanDbConnectorConfig(configuration), connection);
    }

    private YashanDbConnectorConfig mysqlConfig() {
        Configuration configuration = Configuration.create()
                .with("topic.prefix", "test")
                .with("ystream.additional.properties.mysql.mode", "true")
                .build();
        return new YashanDbConnectorConfig(configuration);
    }

    private void assertUnsigned(YashanDbMySqlValueConverters converters, String typeName, int jdbcType, Object raw,
                                Schema.Type expectedType, Object expectedValue) {
        Column column = Column.editor().name("id").type(typeName).jdbcType(jdbcType).optional(true).create();
        Schema schema = converters.schemaBuilder(column).optional().build();
        assertThat(schema.type()).isEqualTo(expectedType);
        if (expectedType == Schema.Type.BYTES) {
            assertThat(schema.name()).isEqualTo(Decimal.LOGICAL_NAME);
            assertThat(schema.parameters()).containsEntry(Decimal.SCALE_FIELD, "0");
        }
        assertThat(converters.converter(column, new Field("id", 0, schema)).convert(raw)).isEqualTo(expectedValue);
    }
}
