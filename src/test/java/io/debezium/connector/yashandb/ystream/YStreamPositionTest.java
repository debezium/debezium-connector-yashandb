/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

/**
 * Unit tests for {@link YStreamPosition}.
 */
class YStreamPositionTest {

    @Test
    void shouldCreatePositionFromLong() {
        YStreamPosition pos = YStreamPosition.valueOf(1000L);
        assertThat(pos).isNotNull();
        assertThat(pos.getScn().longValue()).isEqualTo(1000);
    }

    @Test
    void shouldCreatePositionFromString() {
        YStreamPosition pos = YStreamPosition.valueOf("2000");
        assertThat(pos).isNotNull();
        assertThat(pos.getScn().longValue()).isEqualTo(2000);
    }

    @Test
    void shouldReturnNullForNullString() {
        YStreamPosition pos = YStreamPosition.valueOf((String) null);
        assertThat(pos).isNull();
    }

    @Test
    void shouldGetRawPosition() {
        YStreamPosition pos = YStreamPosition.valueOf(3000L);
        Position raw = pos.getRawPosition();
        assertThat(raw).isNotNull();
        assertThat(raw.getCommitScn().getScn()).isEqualTo(3000);
    }

    @Test
    void shouldComparePositions() {
        YStreamPosition pos1 = YStreamPosition.valueOf(1000L);
        YStreamPosition pos2 = YStreamPosition.valueOf(2000L);
        assertThat(pos1.compareTo(pos2)).isNegative();
        assertThat(pos2.compareTo(pos1)).isPositive();
        assertThat(pos1.compareTo(pos1)).isZero();
    }

    @Test
    void shouldCompareWithNull() {
        YStreamPosition pos = YStreamPosition.valueOf(1000L);
        assertThat(pos.compareTo(null)).isPositive();
    }

    @Test
    void shouldCheckEquality() {
        YStreamPosition pos1 = YStreamPosition.valueOf(1000L);
        YStreamPosition pos2 = YStreamPosition.valueOf(1000L);
        YStreamPosition pos3 = YStreamPosition.valueOf(2000L);
        assertThat(pos1.equals(pos2)).isTrue();
        assertThat(pos1.equals(pos3)).isFalse();
    }

    @Test
    void shouldCheckReflexiveEquality() {
        YStreamPosition pos = YStreamPosition.valueOf(1000L);
        assertThat(pos.equals(pos)).isTrue();
    }

    @Test
    void shouldCheckNullInequality() {
        YStreamPosition pos = YStreamPosition.valueOf(1000L);
        assertThat(pos.equals(null)).isFalse();
    }

    @Test
    void shouldCheckDifferentClassInequality() {
        YStreamPosition pos = YStreamPosition.valueOf(1000L);
        assertThat(pos.equals("1000")).isFalse();
    }

    @Test
    void shouldReturnConsistentHashCode() {
        YStreamPosition pos1 = YStreamPosition.valueOf(1000L);
        YStreamPosition pos2 = YStreamPosition.valueOf(1000L);
        assertThat(pos1.hashCode()).isEqualTo(pos2.hashCode());
    }

    @Test
    void shouldReturnToString() {
        YStreamPosition pos = YStreamPosition.valueOf(1000L);
        String str = pos.toString();
        assertThat(str).isNotNull();
        assertThat(str).isNotEmpty();
    }

    @Test
    void shouldDeriveScnFromRawPosition() {
        Position rawPosition = new Position(new SystemChangeNumber(5555L), new LogPosition());
        YStreamPosition pos = new YStreamPosition(rawPosition);
        assertThat(pos.getScn().longValue()).isEqualTo(5555);
    }
}
