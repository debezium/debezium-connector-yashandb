package io.debezium.connector.yashandb;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** 分片算法. */
public final class YaShanRowidSplitUtil {

    private YaShanRowidSplitUtil() {
    }

    /**
     * 计算分表rowid，升序.
     *
     * @param rowMin min rowid
     * @param rowMax max rowid
     * @param expectedSplitCount expectedSplitCount
     * @param blocksMap data blocks map
     * @return rowid list
     */
    public static List<YaShanRowid> getYashanDBSplitRowid(
                                                          final YaShanRowid rowMin,
                                                          final YaShanRowid rowMax,
                                                          long expectedSplitCount,
                                                          final Map<String, Integer> blocksMap) {
        int diffIdx = -1;
        final List<YaShanRowid> rowidRange = new ArrayList<>();
        // 如果rowMin==rowMax，则不分表
        if (rowMin.getRowid().equals(rowMax.getRowid())) {
            return rowidRange;
        }
        // 找到第一个不相同的数字（分区表大概率是partOid，普通表大概率是block）
        for (int i = 0; i < 5; i++) {
            if (rowMin.getNums()[i].compareTo(rowMax.getNums()[i]) < 0) {
                diffIdx = i;
                break;
            }
        }
        if (diffIdx != 2) {
            for (int i = diffIdx + 1; i < 5; i++) {
                rowMin.getNums()[i] = BigInteger.ZERO;
                rowMax.getNums()[i] = BigInteger.ZERO;
            }
            final BigInteger diffRange = rowMax.getNums()[diffIdx].subtract(rowMin.getNums()[diffIdx]);

            // 如果区间长度小于并发数，没法平分，需要调整并发数
            if (diffRange.compareTo(BigInteger.valueOf(expectedSplitCount)) < 0) {
                expectedSplitCount = diffRange.longValue();
            }
            for (int i = 1; i < expectedSplitCount; i++) {
                final YaShanRowid r = new YaShanRowid(rowMin.getRowid());
                r.getNums()[diffIdx] = r.getNums()[diffIdx].add(
                        diffRange
                                .multiply(BigInteger.valueOf(i))
                                .divide(BigInteger.valueOf(expectedSplitCount)));
                rowidRange.add(r);
            }
            return rowidRange;
        }
        else {
            final long spaceId = rowMin.getNums()[diffIdx - 1].longValue();

            long totalBlocks = 0L;
            for (var i = rowMin.getNums()[diffIdx]; i.compareTo(rowMax.getNums()[diffIdx]) < 0; i = i.add(BigInteger.ONE)) {
                totalBlocks += blocksMap.get(spaceId + "-" + i);
            }
            totalBlocks -= rowMin.getNums()[diffIdx + 1].longValue();
            totalBlocks += rowMax.getNums()[diffIdx + 1].longValue();
            // 如果区间长度小于并发数，没法平分，需要调整并发数
            if (expectedSplitCount > totalBlocks) {
                expectedSplitCount = totalBlocks;
            }
            for (int i = 1; i < expectedSplitCount; i++) {
                long fileId = rowMin.getNums()[diffIdx].longValue();
                final YaShanRowid r = new YaShanRowid(rowMin.getRowid());
                long tmp = r.getNums()[diffIdx + 1].longValue() + i * totalBlocks / expectedSplitCount;
                //
                while (tmp > blocksMap.get(spaceId + "-" + fileId)) {
                    tmp -= blocksMap.get(spaceId + "-" + fileId);
                    fileId++;
                }
                r.getNums()[diffIdx] = BigInteger.valueOf(fileId);
                r.getNums()[diffIdx + 1] = BigInteger.valueOf(tmp);
                r.getNums()[diffIdx + 2] = BigInteger.ZERO;
                rowidRange.add(r);
            }
            return rowidRange;
        }
    }
}