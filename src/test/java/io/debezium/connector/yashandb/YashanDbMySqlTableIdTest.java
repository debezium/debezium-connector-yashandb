/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Types;
import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.doc.FixFor;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.history.JsonTableChangeSerializer;
import io.debezium.relational.history.TableChanges;

class YashanDbMySqlTableIdTest {

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldRecoverMysqlTableUsingTheSameSchemaIdAsStreaming() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "mysql_test")
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("table.include.list", "inventory.orders")
                .with("column.include.list", "inventory.orders.ID")
                .build());

        TableId streamingId = config.getModeContext().dialect().tableId(null, "inventory", "orders");
        Table table = Table.editor()
                .tableId(streamingId)
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).position(1).create())
                .setPrimaryKeyNames(List.of("ID"))
                .create();
        JsonTableChangeSerializer serializer = new JsonTableChangeSerializer();
        TableChanges recovered = serializer.deserialize(serializer.serialize(new TableChanges().create(table)), config.useCatalogBeforeSchema());
        TableId recoveredId = recovered.iterator().next().getId();
        Table legacyTable = Table.editor()
                .tableId(new TableId("inventory", null, "orders"))
                .addColumn(Column.editor().name("ID").jdbcType(Types.INTEGER).position(1).create())
                .create();
        TableChanges recoveredLegacy = serializer.deserialize(serializer.serialize(new TableChanges().create(legacyTable)), config.useCatalogBeforeSchema());

        assertThat(streamingId).isEqualTo(new TableId(null, "inventory", "orders"));
        assertThat(recoveredId).isEqualTo(streamingId);
        assertThat(recoveredLegacy.iterator().next().getId()).isEqualTo(streamingId);
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(recoveredId)).isTrue();
        assertThat(config.getColumnFilter().matches(null, recoveredId.schema(), recoveredId.table(), "ID")).isTrue();
        assertThat(config.getColumnFilter().matches(null, recoveredId.schema(), recoveredId.table(), "OTHER")).isFalse();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldApplyExplicitSchemaFilterInMysqlMode() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "mysql_test")
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("schema.include.list", "inventory")
                .build());

        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId(null, "inventory", "orders"))).isTrue();
        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId(null, "other", "orders"))).isFalse();
    }

    @FixFor("debezium/dbz#3014")
    @Test
    void shouldNotTranslateDatabaseFilterToSchemaFilterInMysqlMode() {
        YashanDbConnectorConfig config = new YashanDbConnectorConfig(Configuration.create()
                .with("topic.prefix", "mysql_test")
                .with("ystream.additional.properties.mysql.mode", "true")
                .with("database.include.list", "inventory")
                .build());

        assertThat(config.getTableFilters().dataCollectionFilter().isIncluded(new TableId(null, "inventory", "orders"))).isFalse();
    }
}
