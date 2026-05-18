/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.yashandb.Scn;

/**
 * Unit tests for YStream wrapper classes and adapters.
 */
class YStreamRecordTest {

    @Test
    void shouldHaveTypeYStream() {
        assertThat(YStreamAdapter.TYPE).isEqualTo("ystream");
    }

    @Test
    void shouldVerifyYStreamPositionClassExists() {
        // Verify YStreamPosition can be instantiated
        com.sics.ystream.result.SystemChangeNumber scn = new com.sics.ystream.result.SystemChangeNumber(1000L);
        com.sics.ystream.result.LogPosition logPos = new com.sics.ystream.result.LogPosition((byte) 1, 100L, 1, 1);
        com.sics.ystream.result.Position position = new com.sics.ystream.result.Position(scn, logPos);

        YStreamPosition yStreamPosition = new YStreamPosition(position);
        assertThat(yStreamPosition).isNotNull();
        assertThat(yStreamPosition.getScn()).isEqualTo(Scn.valueOf(1000L));
    }

    @Test
    void shouldVerifyYStreamDeserializer() {
        YStreamDeserializer deserializer = new YStreamDeserializer();
        assertThat(deserializer).isNotNull();
    }

    @Test
    void shouldVerifyYStreamPositionWithDifferentScnValues() {
        // Test with different SCN values
        com.sics.ystream.result.SystemChangeNumber scn1 = new com.sics.ystream.result.SystemChangeNumber(0L);
        com.sics.ystream.result.LogPosition logPos1 = new com.sics.ystream.result.LogPosition((byte) 0, 0L, 0, 0);
        com.sics.ystream.result.Position position1 = new com.sics.ystream.result.Position(scn1, logPos1);

        YStreamPosition yStreamPosition1 = new YStreamPosition(position1);
        assertThat(yStreamPosition1.getScn().longValue()).isEqualTo(0L);

        // Test with larger SCN
        com.sics.ystream.result.SystemChangeNumber scn2 = new com.sics.ystream.result.SystemChangeNumber(Long.MAX_VALUE);
        com.sics.ystream.result.LogPosition logPos2 = new com.sics.ystream.result.LogPosition((byte) 127, Long.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE);
        com.sics.ystream.result.Position position2 = new com.sics.ystream.result.Position(scn2, logPos2);

        YStreamPosition yStreamPosition2 = new YStreamPosition(position2);
        assertThat(yStreamPosition2.getScn().longValue()).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void shouldVerifyYStreamPositionCompare() {
        com.sics.ystream.result.SystemChangeNumber scn1 = new com.sics.ystream.result.SystemChangeNumber(100L);
        com.sics.ystream.result.LogPosition logPos1 = new com.sics.ystream.result.LogPosition((byte) 1, 100L, 1, 1);
        com.sics.ystream.result.Position position1 = new com.sics.ystream.result.Position(scn1, logPos1);

        com.sics.ystream.result.SystemChangeNumber scn2 = new com.sics.ystream.result.SystemChangeNumber(200L);
        com.sics.ystream.result.LogPosition logPos2 = new com.sics.ystream.result.LogPosition((byte) 2, 200L, 2, 2);
        com.sics.ystream.result.Position position2 = new com.sics.ystream.result.Position(scn2, logPos2);

        YStreamPosition yStreamPosition1 = new YStreamPosition(position1);
        YStreamPosition yStreamPosition2 = new YStreamPosition(position2);

        assertThat(yStreamPosition1.compareTo(yStreamPosition2)).isNegative();
        assertThat(yStreamPosition2.compareTo(yStreamPosition1)).isPositive();
        assertThat(yStreamPosition1.compareTo(yStreamPosition1)).isZero();
    }

    @Test
    void shouldVerifyYStreamPositionEquals() {
        com.sics.ystream.result.SystemChangeNumber scn = new com.sics.ystream.result.SystemChangeNumber(100L);
        com.sics.ystream.result.LogPosition logPos = new com.sics.ystream.result.LogPosition((byte) 1, 100L, 1, 1);
        com.sics.ystream.result.Position position = new com.sics.ystream.result.Position(scn, logPos);

        YStreamPosition yStreamPosition1 = new YStreamPosition(position);
        YStreamPosition yStreamPosition2 = new YStreamPosition(position);

        assertThat(yStreamPosition1).isEqualTo(yStreamPosition2);
        assertThat(yStreamPosition1.hashCode()).isEqualTo(yStreamPosition2.hashCode());
    }
}
