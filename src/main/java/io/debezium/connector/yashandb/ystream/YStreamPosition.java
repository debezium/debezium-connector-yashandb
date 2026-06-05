/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.ystream;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sics.ystream.result.LogPosition;
import com.sics.ystream.result.Position;
import com.sics.ystream.result.SystemChangeNumber;

import io.debezium.connector.yashandb.Scn;
import io.debezium.connector.yashandb.YashanDbOffsetContext;

/**
 * A value class that wraps a raw YStream LCR {@link Position} and derives the corresponding
 * {@link Scn} (System Change Number) for ordering and comparison purposes.
 */
public class YStreamPosition implements Comparable<YStreamPosition> {

    private static final Logger LOGGER = LoggerFactory.getLogger(YStreamPosition.class);

    private final Position lcrPosition;

    /**
     * Creates a YStreamPosition from the given LCR position, deriving the SCN from it.
     *
     * @param lcrPosition the LCR position
     */
    public YStreamPosition(Position lcrPosition) {
        this.lcrPosition = lcrPosition;
        LOGGER.trace("LCR position {} converted to SCN", lcrPosition);
    }

    /**
     * Creates a YStreamPosition from the given SCN value.
     *
     * @param scn the system change number
     * @return a YStreamPosition with the given SCN
     */
    public static YStreamPosition valueOf(long scn) {
        Position position = new Position(new SystemChangeNumber(scn), new LogPosition());
        return new YStreamPosition(position);
    }

    /**
     * Creates a YStreamPosition from the given SCN string value.
     *
     * @param scn the system change number as a string
     * @return a YStreamPosition with the given SCN, or null if the input is null
     */
    public static YStreamPosition valueOf(String scn) {
        if (scn == null) {
            return null;
        }
        Position position = new Position(new SystemChangeNumber(Long.parseLong(scn)), new LogPosition());
        return new YStreamPosition(position);
    }

    /**
     * Creates a YStreamPosition from the given offset map.
     *
     * @param offset the offset map containing position data
     * @return a YStreamPosition constructed from the offset
     * @throws NullPointerException if the offset does not contain a recoverable position
     */
    public static YStreamPosition valueOf(Map<String, ?> offset) {
        return new YStreamPosition(Objects.requireNonNull(YashanDbOffsetContext.loadRecoverPosition(offset)));
    }

    /**
     * @return the raw LCR position
     */
    public Position getLcrPosition() {
        return lcrPosition;
    }

    @Override
    public int hashCode() {
        return lcrPosition != null ? lcrPosition.hashCode() : 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        YStreamPosition that = (YStreamPosition) o;

        return Objects.equals(lcrPosition, that.lcrPosition);
    }

    @Override
    public String toString() {
        return lcrPosition.toString();
    }

    @Override
    public int compareTo(YStreamPosition o) {
        if (o == null) {
            return 1;
        }

        return this.lcrPosition.compareTo(o.lcrPosition);
    }
}
