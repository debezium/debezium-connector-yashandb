package io.debezium.connector.yashandb;

import java.math.BigInteger;

public class YaShanRowid {
    private final BigInteger[] nums;

    /**
     * Constructor.
     *
     * @param rowid rowid
     */
    public YaShanRowid(final String rowid) {
        final String[] split = rowid.split(":");
        assert split.length == 5;
        final BigInteger partOid = new BigInteger(split[0]);
        final BigInteger space = new BigInteger(split[1]);
        final BigInteger file = new BigInteger(split[2]);
        final BigInteger block = new BigInteger(split[3]);
        final BigInteger row = new BigInteger(split[4]);
        nums = new BigInteger[]{ partOid, space, file, block, row };
    }

    public String getRowid() {
        return nums[0] + ":" + nums[1] + ":" + nums[2] + ":" + nums[3] + ":" + nums[4];
    }

    public BigInteger[] getNums() {
        return nums;
    }

}
