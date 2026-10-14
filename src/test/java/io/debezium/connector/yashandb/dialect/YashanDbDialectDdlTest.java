/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;
import io.debezium.relational.TableId;

class YashanDbDialectDdlTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void nativeDdlUsesBoundTableAndSchema() throws SQLException {
        YashanDbDialect dialect = new YashanDbNativeDialect();
        TableId table = new TableId("", "OWNER", "ORDERS");
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThat(dialect.tableDdlUsesParameters()).isTrue();
        assertThat(dialect.tableDdlQuery(table)).isEqualTo("SELECT dbms_metadata.get_ddl('TABLE',?,?) FROM DUAL");
        dialect.bindTableDdlQuery(statement, table);
        verify(statement).setString(1, "ORDERS");
        verify(statement).setString(2, "OWNER");
        assertThat(dialect.tableDdlColumn()).isEqualTo(1);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void mysqlDdlUsesQuotedTableAndSecondResultColumn() {
        YashanDbDialect dialect = new YashanDbMySqlDialect();
        TableId table = new TableId(null, "inventory", "orders");

        assertThat(dialect.tableDdlUsesParameters()).isFalse();
        String quote = String.valueOf((char) 96);
        assertThat(dialect.tableDdlQuery(table)).isEqualTo("SHOW CREATE TABLE " + quote + "inventory" + quote + "." + quote + "orders" + quote);
        assertThat(dialect.tableDdlColumn()).isEqualTo(2);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void mysqlTableNameUsesSchema() {
        YashanDbDialect dialect = new YashanDbMySqlDialect();
        TableId table = new TableId(null, "my`database", "my`table");
        String quote = String.valueOf((char) 96);

        assertThat(dialect.qualifiedTableName(table))
                .isEqualTo(quote + "my``database" + quote + "." + quote + "my``table" + quote);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void nativeLatestTableDdlScnUsesSchemaAndTableParameters() throws SQLException {
        YashanDbDialect dialect = new YashanDbNativeDialect();
        List<TableId> tables = List.of(new TableId("", "APP", "ORDERS"), new TableId("", "OTHER", "ITEMS"));
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThat(dialect.latestTableDdlScnQuery(tables)).isEqualTo("SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(last_ddl_time)))"
                + " FROM all_objects WHERE (owner = ? AND object_name = ?) OR (owner = ? AND object_name = ?)");
        dialect.bindLatestTableDdlScnQuery(statement, tables);
        verify(statement).setString(1, "APP");
        verify(statement).setString(2, "ORDERS");
        verify(statement).setString(3, "OTHER");
        verify(statement).setString(4, "ITEMS");
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void mysqlLatestTableDdlScnUsesInformationSchemaUpdateTime() throws SQLException {
        YashanDbDialect dialect = new YashanDbMySqlDialect();
        List<TableId> tables = List.of(dialect.tableId("", "inventory", "orders"), dialect.tableId("", "other", "items"));
        PreparedStatement statement = mock(PreparedStatement.class);

        assertThat(tables).containsExactly(new TableId(null, "inventory", "orders"), new TableId(null, "other", "items"));
        assertThat(dialect.latestTableDdlScnQuery(tables)).isEqualTo("SELECT TIMESTAMP_TO_SCN(MAX(to_timestamp(update_time)))"
                + " FROM INFORMATION_SCHEMA.tables WHERE (table_schema = ? AND table_name = ?) OR (table_schema = ? AND table_name = ?)");
        dialect.bindLatestTableDdlScnQuery(statement, tables);
        verify(statement).setString(1, "inventory");
        verify(statement).setString(2, "orders");
        verify(statement).setString(3, "other");
        verify(statement).setString(4, "items");
    }

}
