package io.debezium.connector.yashandb;

import java.util.Objects;

import io.debezium.relational.TableId;

public final class YaShanPartitionSplitRecord {
    private final TableId info;
    private final String partitionName;
    private final YaShanRowid minrowid;
    private final YaShanRowid maxrowid;

    public YaShanPartitionSplitRecord(
                                      TableId info, String partitionName, YaShanRowid minrowid, YaShanRowid maxrowid) {
        this.info = info;
        this.partitionName = partitionName;
        this.minrowid = minrowid;
        this.maxrowid = maxrowid;
    }

    public TableId info() {
        return info;
    }

    public String partitionName() {
        return partitionName;
    }

    public YaShanRowid minrowid() {
        return minrowid;
    }

    public YaShanRowid maxrowid() {
        return maxrowid;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (obj == null || obj.getClass() != this.getClass())
            return false;
        var that = (YaShanPartitionSplitRecord) obj;
        return Objects.equals(this.info, that.info) &&
                Objects.equals(this.partitionName, that.partitionName) &&
                Objects.equals(this.minrowid, that.minrowid) &&
                Objects.equals(this.maxrowid, that.maxrowid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(info, partitionName, minrowid, maxrowid);
    }

    @Override
    public String toString() {
        return "YaShanPartitionSplitRecord[" +
                "info=" + info + ", " +
                "partitionName=" + partitionName + ", " +
                "minrowid=" + minrowid + ", " +
                "maxrowid=" + maxrowid + ']';
    }
}
