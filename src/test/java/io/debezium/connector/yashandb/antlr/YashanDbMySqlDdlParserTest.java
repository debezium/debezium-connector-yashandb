/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.antlr;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import com.yashandb.jdbc.YasTypes;

import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlChanges;
import io.debezium.relational.ddl.DdlParserListener;

class YashanDbMySqlDdlParserTest {

    private final YashanDbMySqlDdlParser parser = new YashanDbMySqlDdlParser();
    private final Tables tables = new Tables();

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldCreateAndAlterColumnsInMySqlDatabaseSchema() {
        parser.setCurrentSchema("shop");
        DdlChanges create = parser.parse("CREATE TABLE `orders` (`id` BIGINT UNSIGNED NOT NULL, `amount` DECIMAL(12,2), "
                + "`created_at` TIMESTAMP, `payload` JSON, PRIMARY KEY (`id`))", tables);

        TableId id = new TableId(null, "shop", "orders");
        Table table = tables.forTable(id);
        assertThat(table).isNotNull();
        assertThat(table.primaryKeyColumnNames()).containsExactly("id");
        assertThat(table.columnWithName("id").typeName()).contains("UNSIGNED");
        assertThat(table.columnWithName("amount").length()).isEqualTo(12);
        assertThat(table.columnWithName("amount").scale()).hasValue(2);
        assertThat(table.columnWithName("created_at").jdbcType()).isEqualTo(Types.TIMESTAMP);
        assertThat(table.columnWithName("payload").jdbcType()).isEqualTo(YasTypes.JSON);
        assertThat(events(create)).hasSize(1);

        DdlChanges alter = parser.parse("ALTER TABLE `orders` ADD COLUMN `status` VARCHAR(20) AFTER `id`, "
                + "MODIFY COLUMN `amount` DECIMAL(14,3), DROP COLUMN `payload`", tables);
        table = tables.forTable(id);
        assertThat(columnNames(table)).containsExactly("id", "status", "amount", "created_at");
        assertThat(table.columnWithName("amount").length()).isEqualTo(14);
        assertThat(table.columnWithName("amount").scale()).hasValue(3);
        assertThat(events(alter)).hasSize(1);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldHandleMultipleAddsRenameAndDrop() {
        parser.setCurrentSchema("shop");
        parser.parse("CREATE TABLE t (id INT PRIMARY KEY)", tables);
        parser.parse("ALTER TABLE t ADD (first_name VARCHAR(20), last_name VARCHAR(20))", tables);
        TableId oldId = new TableId(null, "shop", "t");
        assertThat(columnNames(tables.forTable(oldId))).containsExactly("id", "first_name", "last_name");

        DdlChanges rename = parser.parse("RENAME TABLE t TO people", tables);
        TableId newId = new TableId(null, "shop", "people");
        assertThat(tables.forTable(oldId)).isNull();
        assertThat(tables.forTable(newId)).isNotNull();
        assertThat(events(rename)).singleElement().satisfies(event -> {
            assertThat(event).isInstanceOf(DdlParserListener.TableAlteredEvent.class);
            assertThat(((DdlParserListener.TableAlteredEvent) event).previousTableId()).isEqualTo(oldId);
        });

        DdlChanges drop = parser.parse("DROP TABLE people", tables);
        assertThat(tables.forTable(newId)).isNull();
        assertThat(events(drop)).hasSize(1);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldUseExplicitDatabaseAndIgnoreViewDdl() {
        parser.setCurrentSchema("default_db");
        DdlChanges changes = parser.parse("CREATE VIEW v AS SELECT 1; CREATE TABLE `other.db`.`a``b` (id TINYINT, d DATE)", tables);
        Table table = tables.forTable(new TableId(null, "other.db", "a`b"));
        assertThat(table).isNotNull();
        assertThat(table.columnWithName("id").jdbcType()).isEqualTo(Types.TINYINT);
        assertThat(table.columnWithName("d").jdbcType()).isEqualTo(Types.DATE);
        assertThat(events(changes)).hasSize(1);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldChangeColumnsAndPrimaryKeyAcrossStatements() {
        parser.setCurrentSchema("shop");
        TableId id = new TableId(null, "shop", "t");
        parser.parse("CREATE TABLE t (id INT PRIMARY KEY, old_name VARCHAR(10))", tables);
        parser.parse("ALTER TABLE t CHANGE COLUMN old_name display_name VARCHAR(30) NOT NULL FIRST", tables);
        Table table = tables.forTable(id);
        assertThat(columnNames(table)).containsExactly("display_name", "id");
        assertThat(table.columnWithName("old_name")).isNull();
        assertThat(table.columnWithName("display_name").length()).isEqualTo(30);
        assertThat(table.columnWithName("display_name").isOptional()).isFalse();

        parser.parse("ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (display_name)", tables);
        assertThat(tables.forTable(id).primaryKeyColumnNames()).containsExactly("display_name");
        DdlChanges truncate = parser.parse("TRUNCATE TABLE t", tables);
        assertThat(events(truncate)).singleElement().satisfies(event -> {
            assertThat(event.type()).isEqualTo(DdlParserListener.EventType.TRUNCATE_TABLE);
            assertThat(((DdlParserListener.TableEvent) event).tableId()).isEqualTo(id);
        });
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRetainEachColumnDefinitionInMultiAdd() {
        parser.setCurrentSchema("shop");
        parser.parse("CREATE TABLE t (id INT)", tables);
        parser.parse("ALTER TABLE t ADD (a INT NOT NULL, b VARCHAR(12) DEFAULT 'x', c DATE)", tables);
        Table table = tables.forTable(new TableId(null, "shop", "t"));
        assertThat(columnNames(table)).containsExactly("id", "a", "b", "c");
        assertThat(table.columnWithName("a").isOptional()).isFalse();
        assertThat(table.columnWithName("b").length()).isEqualTo(12);
        assertThat(table.columnWithName("b").isOptional()).isTrue();
        assertThat(table.columnWithName("b").defaultValueExpression()).hasValue("x");
        assertThat(table.columnWithName("c").jdbcType()).isEqualTo(Types.DATE);
        assertThat(table.columnWithName("c").isOptional()).isTrue();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldPreserveDefinitionOnRenameAndReplaceDefinitionOnChange() {
        parser.setCurrentSchema("shop");
        parser.parse("CREATE TABLE t (`a``b` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, label VARCHAR(10) DEFAULT 'old')", tables);
        parser.parse("ALTER TABLE t RENAME COLUMN `a``b` TO `a``c`", tables);
        TableId id = new TableId(null, "shop", "t");
        Table table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).containsExactly("a`c");
        assertThat(table.columnWithName("a`c").isAutoIncremented()).isTrue();
        assertThat(table.columnWithName("a`c").isOptional()).isFalse();

        parser.parse("ALTER TABLE t CHANGE COLUMN `a``c` id INT NOT NULL, CHANGE COLUMN label label VARCHAR(30)", tables);
        table = tables.forTable(id);
        assertThat(table.primaryKeyColumnNames()).containsExactly("id");
        assertThat(table.columnWithName("id").isAutoIncremented()).isFalse();
        assertThat(table.columnWithName("id").isGenerated()).isFalse();
        assertThat(table.columnWithName("label").defaultValueExpression()).isEmpty();
        assertThat(table.columnWithName("label").length()).isEqualTo(30);
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldKeepDatabaseWhenRenamingQualifiedTable() {
        parser.setCurrentSchema("shop");
        parser.parse("CREATE TABLE other.t (id INT)", tables);
        parser.parse("ALTER TABLE other.t RENAME TO renamed", tables);
        assertThat(tables.forTable(new TableId(null, "other", "t"))).isNull();
        assertThat(tables.forTable(new TableId(null, "other", "renamed"))).isNotNull();
        assertThat(tables.forTable(new TableId(null, "shop", "renamed"))).isNull();
    }

    private List<DdlParserListener.Event> events(DdlChanges changes) {
        List<DdlParserListener.Event> events = new ArrayList<>();
        changes.getEventsByDatabase((database, databaseEvents) -> events.addAll(databaseEvents));
        return events;
    }

    private List<String> columnNames(Table table) {
        return table.columns().stream().map(Column::name).collect(Collectors.toList());
    }
}
