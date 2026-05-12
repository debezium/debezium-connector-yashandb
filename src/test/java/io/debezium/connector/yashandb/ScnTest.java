/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigInteger;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link Scn}.
 */
class ScnTest {

    @Test
    void shouldCreateScnFromInt() {
        Scn scn = Scn.valueOf(100);
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(100);
        assertThat(scn.isNull()).isFalse();
    }

    @Test
    void shouldCreateScnFromLong() {
        Scn scn = Scn.valueOf(123456789L);
        assertThat(scn).isNotNull();
        assertThat(scn.longValue()).isEqualTo(123456789L);
    }

    @Test
    void shouldCreateScnFromString() {
        Scn scn = Scn.valueOf("99999999999999");
        assertThat(scn).isNotNull();
        assertThat(scn.toString()).isEqualTo("99999999999999");
    }

    @Test
    void shouldCreateScnFromBigInteger() {
        Scn scn = new Scn(BigInteger.valueOf(42));
        assertThat(scn.longValue()).isEqualTo(42);
    }

    @Test
    void shouldHandleNullScn() {
        Scn scn = Scn.NULL;
        assertThat(scn.isNull()).isTrue();
        assertThat(scn.longValue()).isEqualTo(0);
        assertThat(scn.toString()).isEqualTo("null");
    }

    @Test
    void shouldHandleMaxScn() {
        Scn scn = Scn.MAX;
        assertThat(scn.isNull()).isFalse();
        assertThat(scn.toString()).isEqualTo("-2");
    }

    @Test
    void shouldAddTwoNonNullScns() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.valueOf(5);
        Scn result = a.add(b);
        assertThat(result.longValue()).isEqualTo(15);
    }

    @Test
    void shouldAddWithNullLeftOperand() {
        Scn a = Scn.NULL;
        Scn b = Scn.valueOf(5);
        Scn result = a.add(b);
        assertThat(result.longValue()).isEqualTo(5);
        assertThat(result.isNull()).isFalse();
    }

    @Test
    void shouldAddWithNullRightOperand() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.NULL;
        Scn result = a.add(b);
        assertThat(result.longValue()).isEqualTo(10);
        assertThat(result.isNull()).isFalse();
    }

    @Test
    void shouldAddTwoNullScns() {
        Scn result = Scn.NULL.add(Scn.NULL);
        assertThat(result.isNull()).isTrue();
    }

    @Test
    void shouldSubtractTwoNonNullScns() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.valueOf(3);
        Scn result = a.subtract(b);
        assertThat(result.longValue()).isEqualTo(7);
    }

    @Test
    void shouldSubtractWithNullLeftOperand() {
        Scn a = Scn.NULL;
        Scn b = Scn.valueOf(5);
        Scn result = a.subtract(b);
        assertThat(result.longValue()).isEqualTo(-5);
        assertThat(result.isNull()).isFalse();
    }

    @Test
    void shouldSubtractWithNullRightOperand() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.NULL;
        Scn result = a.subtract(b);
        assertThat(result.longValue()).isEqualTo(10);
        assertThat(result.isNull()).isFalse();
    }

    @Test
    void shouldSubtractTwoNullScns() {
        Scn result = Scn.NULL.subtract(Scn.NULL);
        assertThat(result.isNull()).isTrue();
    }

    @Test
    void shouldCompareTwoNonNullScns() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.valueOf(5);
        assertThat(a.compareTo(b)).isPositive();
        assertThat(b.compareTo(a)).isNegative();
        assertThat(a.compareTo(a)).isZero();
    }

    @Test
    void shouldCompareNullWithNonNull() {
        Scn nullScn = Scn.NULL;
        Scn valueScn = Scn.valueOf(5);
        assertThat(nullScn.compareTo(valueScn)).isEqualTo(-1);
        assertThat(valueScn.compareTo(nullScn)).isEqualTo(1);
    }

    @Test
    void shouldCompareTwoNullScns() {
        assertThat(Scn.NULL.compareTo(Scn.NULL)).isZero();
    }

    @Test
    void shouldCheckEquality() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.valueOf(10);
        Scn c = Scn.valueOf(20);
        assertThat(a.equals(b)).isTrue();
        assertThat(a.equals(c)).isFalse();
        assertThat(a.equals(null)).isFalse();
        assertThat(a.equals(a)).isTrue();
    }

    @Test
    void shouldCheckEqualityWithNullScns() {
        Scn null1 = new Scn(null);
        Scn null2 = new Scn(null);
        assertThat(null1.equals(null2)).isTrue();
    }

    @Test
    void shouldReturnConsistentHashCode() {
        Scn a = Scn.valueOf(10);
        Scn b = Scn.valueOf(10);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void shouldReturnConsistentHashCodeForNull() {
        Scn null1 = new Scn(null);
        Scn null2 = new Scn(null);
        assertThat(null1.hashCode()).isEqualTo(null2.hashCode());
    }

    @Test
    void shouldNotBeEqualToDifferentClass() {
        Scn scn = Scn.valueOf(10);
        assertThat(scn.equals("10")).isFalse();
    }

    @Test
    void shouldHandleLargeValues() {
        Scn large = Scn.valueOf("99999999999999999999");
        assertThat(large.isNull()).isFalse();
        assertThat(large.toString()).isEqualTo("99999999999999999999");
    }

    @Test
    void shouldAddLargeValues() {
        Scn a = new Scn(BigInteger.valueOf(Long.MAX_VALUE));
        Scn b = Scn.valueOf(1);
        Scn result = a.add(b);
        assertThat(result.toString()).isEqualTo("9223372036854775808");
    }

    @Test
    void shouldHandleZeroValue() {
        Scn zero = Scn.valueOf(0);
        assertThat(zero.isNull()).isFalse();
        assertThat(zero.longValue()).isEqualTo(0);
    }

    @Test
    void shouldHandleNegativeValue() {
        Scn neg = Scn.valueOf(-100);
        assertThat(neg.isNull()).isFalse();
        assertThat(neg.longValue()).isEqualTo(-100);
    }
}
