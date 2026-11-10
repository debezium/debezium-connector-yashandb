/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.data.SpecialValueDecimal;
import io.debezium.relational.Column;
import io.debezium.relational.ValueConverter;

/**
 * Value converters for the YashanDB MySQL compatibility mode.
 */
public class YashanDbMySqlValueConverters extends YashanDbValueConverters {

    private static final Pattern INTEGER_TYPE = Pattern.compile(
            "^\\s*(TINYINT|INT1|SMALLINT|INT2|MEDIUMINT|MIDDLEINT|INT3|INTEGER|INT4|INT|BIGINT|INT8)\\b",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern UNSIGNED_MODIFIER = Pattern.compile("\\b(UNSIGNED|ZEROFILL)\\b", Pattern.CASE_INSENSITIVE);

    private final YashanDbConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode;

    public YashanDbMySqlValueConverters(YashanDbConnectorConfig config, YashanDbConnection connection) {
        super(config, connection);
        this.bigIntUnsignedHandlingMode = config.getBigIntUnsignedHandlingMode();
    }

    @Override
    public SchemaBuilder schemaBuilder(Column column) {
        if (column.jdbcType() == Types.TIME) {
            return SchemaBuilder.string();
        }
        int unsignedBits = unsignedIntegerBits(column);
        switch (unsignedBits) {
            case 8:
                return SchemaBuilder.int16();
            case 16:
            case 24:
                return SchemaBuilder.int32();
            case 32:
                return SchemaBuilder.int64();
            case 64:
                switch (bigIntUnsignedHandlingMode) {
                    case LONG:
                        return SchemaBuilder.int64();
                    case DOUBLE:
                        return SchemaBuilder.float64();
                    case STRING:
                        return SchemaBuilder.string();
                    case PRECISE:
                        return SpecialValueDecimal.builder(DecimalMode.PRECISE, 20, 0);
                    default:
                        throw new IllegalStateException("Unsupported unsigned BIGINT mode: " + bigIntUnsignedHandlingMode);
                }
            default:
                return super.schemaBuilder(column);
        }
    }

    @Override
    public ValueConverter converter(Column column, Field fieldDefn) {
        if (column.jdbcType() == Types.TIME) {
            return data -> convertValue(column, fieldDefn, data, "", receiver -> {
                if (data instanceof String || data instanceof Time || data instanceof LocalTime) {
                    receiver.deliver(data.toString());
                }
            });
        }
        int unsignedBits = unsignedIntegerBits(column);
        if (unsignedBits == 0) {
            return super.converter(column, fieldDefn);
        }
        if (unsignedBits == 64 && bigIntUnsignedHandlingMode == YashanDbConnectorConfig.BigIntUnsignedHandlingMode.LONG) {
            return data -> convertBigInt(column, fieldDefn, data);
        }
        return data -> convertUnsignedInteger(column, fieldDefn, data, unsignedBits);
    }

    private int unsignedIntegerBits(Column column) {
        if (column.typeName() == null || !UNSIGNED_MODIFIER.matcher(column.typeName()).find()) {
            return 0;
        }
        Matcher type = INTEGER_TYPE.matcher(column.typeName());
        if (!type.find()) {
            return 0;
        }
        switch (type.group(1).toUpperCase(Locale.ROOT)) {
            case "TINYINT":
            case "INT1":
                return 8;
            case "SMALLINT":
            case "INT2":
                return 16;
            case "MEDIUMINT":
            case "MIDDLEINT":
            case "INT3":
                return 24;
            case "INT":
            case "INTEGER":
            case "INT4":
                return 32;
            case "BIGINT":
            case "INT8":
                return 64;
            default:
                return 0;
        }
    }

    private Object convertUnsignedInteger(Column column, Field fieldDefn, Object data, int bits) {
        Object fallback = bits == 32 ? 0L : bits == 8 ? (short) 0 : 0;
        if (bits == 64) {
            switch (bigIntUnsignedHandlingMode) {
                case DOUBLE:
                    fallback = 0.0d;
                    break;
                case STRING:
                    fallback = "0";
                    break;
                default:
                    fallback = BigDecimal.ZERO;
            }
        }
        return convertValue(column, fieldDefn, data, fallback, receiver -> {
            if (!(data instanceof Number) && !(data instanceof String)) {
                return;
            }
            BigInteger value = new BigDecimal(data.toString()).toBigIntegerExact();
            if (value.signum() < 0) {
                BigInteger signedMinimum = BigInteger.ONE.shiftLeft(bits - 1).negate();
                if (value.compareTo(signedMinimum) < 0) {
                    throw new IllegalArgumentException("Unsigned value outside " + bits + "-bit range for column " + column.name() + ": " + data);
                }
                value = value.add(BigInteger.ONE.shiftLeft(bits));
            }
            if (value.bitLength() > bits) {
                throw new IllegalArgumentException("Unsigned value outside " + bits + "-bit range for column " + column.name() + ": " + data);
            }
            switch (bits) {
                case 8:
                    receiver.deliver(value.shortValueExact());
                    break;
                case 16:
                case 24:
                    receiver.deliver(value.intValueExact());
                    break;
                case 32:
                    receiver.deliver(value.longValueExact());
                    break;
                case 64:
                    switch (bigIntUnsignedHandlingMode) {
                        case DOUBLE:
                            receiver.deliver(value.doubleValue());
                            break;
                        case STRING:
                            receiver.deliver(value.toString());
                            break;
                        case PRECISE:
                            receiver.deliver(new BigDecimal(value));
                            break;
                        default:
                            throw new IllegalStateException("Unsupported unsigned BIGINT mode: " + bigIntUnsignedHandlingMode);
                    }
                    break;
                default:
                    throw new IllegalStateException("Unsupported unsigned integer width: " + bits);
            }
        });
    }
}
