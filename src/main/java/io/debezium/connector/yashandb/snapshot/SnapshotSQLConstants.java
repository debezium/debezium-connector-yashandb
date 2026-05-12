package io.debezium.connector.yashandb.snapshot;

public final class SnapshotSQLConstants {

    public static final String QUERY_NO_PARTITION_ROW_ID_SQL = "WITH BASE AS (\n" +
            "     select '%s' AS TABLE_NAME, '%s' AS OWNER FROM DUAL\n" +
            "),\n" +
            " OBJ AS (\n" +
            "    select DATA_OBJECT_ID DATA_OID, OWNER , OBJECT_NAME  from ALL_OBJECTS WHERE \n" +
            "    OWNER =(SELECT OWNER FROM BASE)\n" +
            "    AND OBJECT_NAME=(SELECT TABLE_NAME FROM BASE)  \n" +
            "),\n" +
            "TAB AS (\n" +
            "     SELECT TS# SPACE_ID from sys.tab$ WHERE obj#= (\n" +
            "     select OBJECT_ID  from ALL_OBJECTS\n" +
            "     WHERE OBJECT_NAME =(SELECT TABLE_NAME FROM BASE)  AND OWNER =(SELECT OWNER FROM BASE) )\n" +
            "),\n" +
            "EXTENTS AS (\n" +
            "    SELECT MIN(FILE_ID) AS MIN_DATAFILE,\n" +
            "           MAX(FILE_ID) AS MAX_DATAFILE,\n" +
            "           MIN(BLOCK_ID) AS MIN_BLOCK,\n" +
            "           MAX(BLOCK_ID + BLOCKS - 1) AS MAX_BLOCK\n" +
            "    FROM DBA_EXTENTS\n" +
            "    WHERE SEGMENT_NAME  =(SELECT TABLE_NAME FROM BASE)  \n" +
            "    AND  OWNER =(SELECT OWNER FROM BASE)\n" +
            ")\n" +
            "SELECT A.OWNER, A.OBJECT_NAME, \n" +
            "       A.DATA_OID || ':' || B.SPACE_ID || ':' || C.MIN_DATAFILE || ':' || C.MIN_BLOCK || ':' || '0' AS MIN_ROWID,\n" +
            "       A.DATA_OID || ':' || B.SPACE_ID || ':' || C.MAX_DATAFILE || ':' || C.MAX_BLOCK || ':' || '4095' AS MAX_ROWID\n" +
            "FROM OBJ A,\n" +
            "     TAB B,\n" +
            "     EXTENTS C";

    public static final String QUERY_PARTITION_ROW_ID_SQL = "WITH BASE AS (\n"
            + "     SELECT OWNER,SEGMENT_NAME AS TABLE_NAME,PARTITION_NAME FROM DBA_SEGMENTS\n"
            + "     WHERE OWNER = '%s' AND SEGMENT_NAME = '%s'\n"
            + "     -- only can get from DBA_SEGMENTS use table partition name or table subpartition Name\n"
            + "     AND PARTITION_NAME IN ('%s')\n"
            + "),\n"
            + "-- select part all DATA_OBJECT_ID AS OID\n"
            + "OBJ AS (\n"
            + "    SELECT OBJECT_NAME,SUBOBJECT_NAME, DATA_OBJECT_ID AS OID FROM ALL_OBJECTS OBJ\n"
            + "    WHERE OBJ.OWNER=(SELECT DISTINCT OWNER FROM BASE)\n"
            + "    AND OBJ.OBJECT_NAME=(SELECT DISTINCT TABLE_NAME FROM BASE)\n"
            + "    AND OBJ.SUBOBJECT_NAME IN (SELECT PARTITION_NAME FROM BASE)\n"
            + "    AND OBJ.SUBOBJECT_NAME IS NOT NULL\n"
            + "),\n"
            + "OID AS (\n"
            + "    SELECT SUBOBJECT_NAME, MIN(OID) AS MIN_OID, MAX(OID) AS MAX_OID FROM OBJ\n"
            + "    GROUP BY OBJECT_NAME,SUBOBJECT_NAME\n"
            + "),\n"
            + "MIN_SPACE AS (\n"
            + "    SELECT OID.SUBOBJECT_NAME, TS# AS SPACE_ID from SYS.\"TABPART$\"\n"
            + "    JOIN OID ON DATAOBJ# = OID.MIN_OID\n"
            + "),\n"
            + "MAX_SPACE AS (\n"
            + "    SELECT OID.SUBOBJECT_NAME, TS# AS SPACE_ID from SYS.\"TABPART$\"\n"
            + "    JOIN OID ON DATAOBJ# = OID.MAX_OID\n"
            + "),\n"
            + "DATAFILE_BLOCK AS (\n"
            + "    SELECT PARTITION_NAME, \n"
            + "        MIN(FILE_ID) AS MIN_DATAFILE, \n"
            + "        MIN(BLOCK_ID) AS MIN_BLOCK, \n"
            + "        MAX(FILE_ID) AS MAX_DATAFILE, \n"
            + "        MAX(BLOCK_ID + BLOCKS - 1) AS MAX_BLOCK\n"
            + "    FROM DBA_EXTENTS B \n"
            + "    WHERE B.OWNER = (SELECT DISTINCT OWNER FROM BASE)\n"
            + "    AND B.SEGMENT_NAME = (SELECT DISTINCT TABLE_NAME FROM BASE)\n"
            + "    AND B.PARTITION_NAME IN (SELECT PARTITION_NAME FROM BASE)\n"
            + "    GROUP BY B.PARTITION_NAME\n"
            + ") \n"
            + "SELECT T.OWNER, T.TABLE_NAME,T.PARTITION_NAME,\n"
            + "       A.MIN_OID || ':' || B.SPACE_ID || ':' || D.MIN_DATAFILE || ':' || D.MIN_BLOCK || ':' || '0' AS MIN_ROWID,\n"
            + "       A.MAX_OID || ':' || C.SPACE_ID || ':' || D.MAX_DATAFILE || ':' || D.MAX_BLOCK || ':' || '4095' AS MAX_ROWID\n"
            + "FROM BASE T\n"
            + "JOIN OID A ON A.SUBOBJECT_NAME = T.PARTITION_NAME\n"
            + "JOIN MIN_SPACE B ON B.SUBOBJECT_NAME = T.PARTITION_NAME\n"
            + "JOIN MAX_SPACE C ON C.SUBOBJECT_NAME = T.PARTITION_NAME\n"
            + "JOIN DATAFILE_BLOCK D ON D.PARTITION_NAME = T.PARTITION_NAME";

    public static final String FLASH_BACK_SELECT_NO_PARTITION = "SELECT %s FROM %s AS OF SCN %s ";

    public static final String FLASH_BACK_SELECT_PARTITION = "SELECT %s FROM %s PARTITION (%s) AS OF SCN %s";
}
