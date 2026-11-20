/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.yashandb.dialect;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;

import io.debezium.relational.TableId;

/** Maps ordered catalog rows to the key columns selected for each requested table. */
final class KeyMetadataReader {

    private KeyMetadataReader() {
    }

    static Map<TableId, List<String>> read(ResultSet resultSet, Collection<TableId> tableIds,
                                           BiPredicate<String, String> indexFilter)
            throws SQLException {
        Map<String, TableId> tablesByName = new HashMap<>();
        Map<TableId, Map<String, KeyColumns>> keysByTable = new HashMap<>();
        Map<TableId, List<String>> selectedKeys = new HashMap<>();
        for (TableId tableId : tableIds) {
            tablesByName.put(tableId.table(), tableId);
            selectedKeys.put(tableId, List.of());
        }

        while (resultSet.next()) {
            String schemaName = resultSet.getString(1);
            String tableName = resultSet.getString(2);
            TableId tableId = tableName == null ? null : tablesByName.get(tableName);
            String indexName = resultSet.getString(3);
            if (tableId == null || !Objects.equals(tableId.schema(), schemaName) || indexName == null) {
                continue;
            }

            boolean primaryKey = "P".equalsIgnoreCase(resultSet.getString(4));
            String columnName = resultSet.getString(5);
            KeyColumns keyColumns = keysByTable.computeIfAbsent(tableId, ignored -> new LinkedHashMap<>())
                    .computeIfAbsent((primaryKey ? "P:" : "U:") + indexName, ignored -> new KeyColumns(primaryKey));
            keyColumns.columns.add(columnName);
            if (!primaryKey && !indexFilter.test(indexName, columnName)) {
                keyColumns.included = false;
            }
        }

        for (Map.Entry<TableId, Map<String, KeyColumns>> tableEntry : keysByTable.entrySet()) {
            List<String> key = null;
            for (KeyColumns candidate : tableEntry.getValue().values()) {
                if (candidate.primaryKey) {
                    key = candidate.columns;
                    break;
                }
                if (key == null && candidate.included) {
                    key = candidate.columns;
                }
            }
            if (key != null) {
                selectedKeys.put(tableEntry.getKey(), key);
            }
        }
        return selectedKeys;
    }

    private static final class KeyColumns {
        private final boolean primaryKey;
        private final List<String> columns = new ArrayList<>();
        private boolean included = true;

        private KeyColumns(boolean primaryKey) {
            this.primaryKey = primaryKey;
        }
    }
}
