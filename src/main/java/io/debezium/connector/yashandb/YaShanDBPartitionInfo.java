package io.debezium.connector.yashandb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 此类用于记录YaShan数据库本身的分区信息.
 */
public class YaShanDBPartitionInfo {
    public static final class SubPartitionInfo {
        private final String tableName;
        private final String partitionName;
        private final long size;

        public SubPartitionInfo(String tableName, String partitionName, long size) {
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.size = size;
        }

        public String tableName() {
            return tableName;
        }

        public String partitionName() {
            return partitionName;
        }

        public long size() {
            return size;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this)
                return true;
            if (obj == null || obj.getClass() != this.getClass())
                return false;
            var that = (SubPartitionInfo) obj;
            return Objects.equals(this.tableName, that.tableName) &&
                    Objects.equals(this.partitionName, that.partitionName) &&
                    this.size == that.size;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tableName, partitionName, size);
        }

        @Override
        public String toString() {
            return "SubPartitionInfo[" +
                    "tableName=" + tableName + ", " +
                    "partitionName=" + partitionName + ", " +
                    "size=" + size + ']';
        }
    }

    private final String schema;
    private final String tableName;
    private boolean isPartition;
    private List<SubPartitionInfo> subPartitionInfo;

    /** Constructor. */
    public YaShanDBPartitionInfo(final String schema, final String tableName, final boolean isPartition) {
        this.schema = schema;
        this.tableName = tableName;
        this.isPartition = isPartition;
        this.subPartitionInfo = new ArrayList<>();// Collections.emptyList();
    }

    // Getter for schema (final field, no setter)
    public String getSchema() {
        return schema;
    }

    // Getter for name (final field, no setter)
    public String getTableName() {
        return tableName;
    }

    // Getter for isPartition
    public boolean getIsPartition() {
        return isPartition;
    }

    // Setter for isPartition
    public void setIsPartition(boolean partition) {
        isPartition = partition;
    }

    // Getter for subPartitionInfo
    public List<SubPartitionInfo> getSubPartitionInfo() {
        return subPartitionInfo;
    }

    // Setter for subPartitionInfo
    public void setSubPartitionInfo(List<SubPartitionInfo> subPartitionInfo) {
        this.subPartitionInfo = subPartitionInfo;
    }

    // 可选：添加一个便捷方法来添加单个子分区信息
    public void addSubPartitionInfo(SubPartitionInfo info) {
        // 如果当前是空列表，需要先初始化为可修改的列表
        if (this.subPartitionInfo == Collections.EMPTY_LIST) {
            this.subPartitionInfo = new ArrayList<>();
        }
        this.subPartitionInfo.add(info);
    }
}
