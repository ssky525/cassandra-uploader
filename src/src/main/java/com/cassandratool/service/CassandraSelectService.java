package com.cassandratool.service;

import com.datastax.oss.driver.api.core.AllNodesFailedException;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

/**
 * Простой SELECT * … LIMIT для предпросмотра данных в таблице.
 */
@Service
public class CassandraSelectService {

    public static final class TablePreview {
        private final List<String> columnNames;
        private final List<List<String>> rows;

        public TablePreview(List<String> columnNames, List<List<String>> rows) {
            this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
            this.rows = Collections.unmodifiableList(new ArrayList<>(rows));
        }

        public List<String> getColumnNames() {
            return columnNames;
        }

        public List<List<String>> getRows() {
            return rows;
        }
    }

    public TablePreview selectStarLimit(CqlSession session, String keyspace, String table, int limit) {
        if (limit < 1 || limit > 10_000) {
            throw new IllegalArgumentException("LIMIT должен быть от 1 до 10000");
        }
        String ks = keyspace.trim();
        String tb = table.trim();
        String cql = String.format(
                "SELECT * FROM %s.%s LIMIT %d",
                CqlIdentifiers.doubleQuoteIdent(ks),
                CqlIdentifiers.doubleQuoteIdent(tb),
                limit);

        ResultSet rs;
        try {
            rs = session.execute(SimpleStatement.newInstance(cql));
        } catch (AllNodesFailedException e) {
            String dcs = CassandraClusterDiagnostics.discoveredDatacenters(session);
            throw new IllegalStateException(
                    "Нет узла для запроса (часто: неверный «Local DC»). В топологии кластера DC: "
                            + (dcs.isEmpty() ? "—" : dcs)
                            + ". Укажите в приложении то же имя, что datacenter= у узла в логе, переподключитесь.",
                    e);
        }
        List<Row> rowList = new ArrayList<>();
        for (Row row : rs) {
            rowList.add(row);
        }

        List<String> columns;
        if (!rowList.isEmpty()) {
            ColumnDefinitions defs = rowList.get(0).getColumnDefinitions();
            columns = new ArrayList<>(defs.size());
            for (int i = 0; i < defs.size(); i++) {
                columns.add(defs.get(i).getName().asCql(true));
            }
        } else {
            columns = columnNamesFromMetadata(session, ks, tb);
        }

        List<List<String>> data = new ArrayList<>(rowList.size());
        for (Row row : rowList) {
            List<String> line = new ArrayList<>(columns.size());
            for (int i = 0; i < columns.size(); i++) {
                if (row.isNull(i)) {
                    line.add("NULL");
                } else {
                    Object o = row.getObject(i);
                    line.add(o != null ? String.valueOf(o) : "NULL");
                }
            }
            data.add(line);
        }

        return new TablePreview(columns, data);
    }

    private static List<String> columnNamesFromMetadata(CqlSession session, String keyspace, String table) {
        Optional<KeyspaceMetadata> ksm =
                session.getMetadata().getKeyspace(CqlIdentifier.fromInternal(keyspace));
        Optional<TableMetadata> tbl = ksm.flatMap(k -> k.getTable(CqlIdentifier.fromInternal(table)));
        if (tbl.isEmpty()) {
            return List.of();
        }
        List<ColumnMetadata> cols = new ArrayList<>(tbl.get().getColumns().values());
        cols.sort(Comparator.comparing(c -> c.getName().asInternal()));
        List<String> names = new ArrayList<>(cols.size());
        for (ColumnMetadata c : cols) {
            names.add(c.getName().asCql(true));
        }
        return names;
    }
}
