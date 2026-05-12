package io.debezium.connector.yashandb;

import java.util.ArrayList;
import java.util.List;

import io.debezium.relational.TableId;

public class SnapshotTableSplitInfo {
    private TableId tableId;

    private final List<String> columnNames;

    private long kiloByteSize;

    /** 上层保证minRowid和maxRowid必须同时为空或非空，值为空时，此表不用拆分. */
    private Object minValue;

    private Object maxValue;

    // 此值表示被拆分多少份进行同步，会小于等于并发的线程数
    private int partCount;

    /** 空表过滤，查空表要与数据库进行一次交互，在snapshotTask中查空表会建立新链接，速度更慢. */
    private boolean isEmptyTable;

    /** 表示待迁移表的各项数据初始化完成. */
    private boolean isInitialized;

    /** 这个变量目前是MySQL在用，用于MySQL的拆表. */
    private long tableRows;

    /** 记录表的分区信息 */
    private YaShanDBPartitionInfo partitionInfo;

    public SnapshotTableSplitInfo(TableId tableId) {
        this.tableId = tableId;
        this.kiloByteSize = 0L;
        this.minValue = null;
        this.maxValue = null;
        this.partitionInfo = new YaShanDBPartitionInfo(tableId.schema(), tableId.table(), Boolean.FALSE);
        columnNames = new ArrayList<>();
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public TableId getTableId() {
        return this.tableId;
    }

    public long getKiloByteSize() {
        return kiloByteSize;
    }

    public void setKiloByteSize(long kiloByteSize) {
        this.kiloByteSize = kiloByteSize;
    }

    public Object getMinValue() {
        return minValue;
    }

    public void setMinValue(Object minValue) {
        this.minValue = minValue;
    }

    public Object getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(Object maxValue) {
        this.maxValue = maxValue;
    }

    /*
     * public Column getSplitColumn() {
     * return this.splitColumn;
     * }
     *
     * public void setSplitColumn(Column splitColumn) {
     * this.splitColumn = splitColumn;
     * }
     */
    public int getPartCount() {
        return partCount;
    }

    public void setPartCount(int partCount) {
        this.partCount = partCount;
    }

    public boolean isEmptyTable() {
        return isEmptyTable;
    }

    public void setEmptyTable(boolean emptyTable) {
        isEmptyTable = emptyTable;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    public void setInitialized(boolean initialized) {
        isInitialized = initialized;
    }

    public long getTableRows() {
        return tableRows;
    }

    public void setTableRows(long tableRows) {
        this.tableRows = tableRows;
    }

    public void increaseSize(long size) {
        kiloByteSize += size;
    }

    public void setPartitionInfo(YaShanDBPartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    public YaShanDBPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }
}
