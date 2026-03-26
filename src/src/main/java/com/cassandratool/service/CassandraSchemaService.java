package com.cassandratool.service;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Чтение keyspace / таблиц / порядка колонок из metadata драйвера.
 */
public final class CassandraSchemaService {

    private CassandraSchemaService() {}

    public static boolean isSystemKeyspace(String name) {
        if (name == null) {
            return true;
        }
        String n = name.toLowerCase();
        return n.equals("system") || n.startsWith("system_");
    }

    public static List<String> listKeyspaces(CqlSession session) {
        return session.getMetadata().getKeyspaces().values().stream()
                .map(k -> k.getName().asInternal())
                .filter(k -> !isSystemKeyspace(k))
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.toList());
    }

    public static List<String> listTables(CqlSession session, String keyspace) {
        if (keyspace == null || keyspace.isBlank()) {
            return List.of();
        }
        Optional<KeyspaceMetadata> ksm =
                session.getMetadata().getKeyspace(CqlIdentifier.fromInternal(keyspace.trim()));
        if (ksm.isEmpty()) {
            return List.of();
        }
        return ksm.get().getTables().values().stream()
                .filter(t -> !t.isVirtual())
                .map(t -> t.getName().asInternal())
                .sorted(String::compareToIgnoreCase)
                .collect(Collectors.toList());
    }

    public static Optional<TableMetadata> findTable(CqlSession session, String keyspace, String table) {
        if (keyspace == null || keyspace.isBlank() || table == null || table.isBlank()) {
            return Optional.empty();
        }
        return session.getMetadata()
                .getKeyspace(CqlIdentifier.fromInternal(keyspace.trim()))
                .flatMap(k -> k.getTable(CqlIdentifier.fromInternal(table.trim())));
    }

    /**
     * Порядок колонок: ключ раздела, кластеризация, остальные по имени (как удобно для INSERT по позициям из CSV).
     */
    public static List<String> getColumnNamesOrdered(TableMetadata table) {
        List<ColumnMetadata> partitionKey = table.getPartitionKey();
        List<ColumnMetadata> clustering = new ArrayList<>(table.getClusteringColumns().keySet());
        Set<CqlIdentifier> seen = new LinkedHashSet<>();
        for (ColumnMetadata c : partitionKey) {
            seen.add(c.getName());
        }
        for (ColumnMetadata c : clustering) {
            seen.add(c.getName());
        }
        List<ColumnMetadata> regular = new ArrayList<>();
        for (ColumnMetadata c : table.getColumns().values()) {
            if (!seen.contains(c.getName())) {
                regular.add(c);
            }
        }
        regular.sort(Comparator.comparing(c -> c.getName().asInternal()));
        List<String> names = new ArrayList<>();
        for (ColumnMetadata c : partitionKey) {
            names.add(c.getName().asInternal());
        }
        for (ColumnMetadata c : clustering) {
            names.add(c.getName().asInternal());
        }
        for (ColumnMetadata c : regular) {
            names.add(c.getName().asInternal());
        }
        return names;
    }

    public static Optional<List<String>> getColumnNamesOrdered(CqlSession session, String keyspace, String table) {
        return findTable(session, keyspace, table).map(CassandraSchemaService::getColumnNamesOrdered);
    }
}
