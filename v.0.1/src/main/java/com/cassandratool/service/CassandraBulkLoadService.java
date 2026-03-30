package com.cassandratool.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * Вставка строк из {@link CsvTableData} в таблицу Cassandra.
 * Если {@code explicitInsertColumns} задан — имена колонок в CQL берутся из него (как в схеме, с учётом регистра).
 * Иначе — из {@link CsvTableData#getColumnNames()} через {@link CqlIdentifiers#sanitize(String)}.
 */
public final class CassandraBulkLoadService {

    public static final class LoadResult {
        private final int inserted;
        private final int failed;

        public LoadResult(int inserted, int failed) {
            this.inserted = inserted;
            this.failed = failed;
        }

        public int inserted() {
            return inserted;
        }

        public int failed() {
            return failed;
        }
    }

    /**
     * @param explicitInsertColumns если не null и не пусто — имена колонок для INSERT (из схемы таблицы)
     * @param onProgress вызывается после каждой строки: (успешных вставок, обработано строк, всего в плане)
     */
    public LoadResult insertRows(
            CqlSession session,
            String keyspace,
            String table,
            CsvTableData data,
            List<String> explicitInsertColumns,
            int maxRows,
            Consumer<String> logLine,
            ProgressListener onProgress) {

        List<String> cqlColumns;
        if (explicitInsertColumns != null && !explicitInsertColumns.isEmpty()) {
            cqlColumns = new ArrayList<>(explicitInsertColumns);
        } else {
            List<String> rawHeaders = data.getColumnNames();
            cqlColumns = new ArrayList<>(rawHeaders.size());
            for (String h : rawHeaders) {
                cqlColumns.add(CqlIdentifiers.sanitize(h));
            }
        }

        StringBuilder colPart = new StringBuilder();
        StringBuilder valPart = new StringBuilder();
        for (int i = 0; i < cqlColumns.size(); i++) {
            if (i > 0) {
                colPart.append(", ");
                valPart.append(", ");
            }
            colPart.append(CqlIdentifiers.doubleQuoteIdent(cqlColumns.get(i)));
            valPart.append("?");
        }

        String cql = String.format(
                "INSERT INTO %s.%s (%s) VALUES (%s)",
                CqlIdentifiers.doubleQuoteIdent(keyspace),
                CqlIdentifiers.doubleQuoteIdent(table),
                colPart,
                valPart);

        logLine.accept("Подготовка запроса: INSERT INTO … (" + colPart + ") VALUES (…)");

        PreparedStatement ps;
        try {
            ps = session.prepare(cql);
        } catch (com.datastax.oss.driver.api.core.DriverTimeoutException e) {
            logLine.accept(hintNoNode());
            throw e;
        } catch (com.datastax.oss.driver.api.core.AllNodesFailedException e) {
            // включает NoNodeAvailableException
            logLine.accept(hintNoNode());
            throw e;
        }
        CodecRegistry codecRegistry = session.getContext().getCodecRegistry();
        List<List<String>> rows = data.getRows();
        int limit = Math.min(maxRows, rows.size());
        int inserted = 0;
        int failed = 0;

        for (int r = 0; r < limit; r++) {
            List<String> row = rows.get(r);
            try {
                BoundStatementBuilder bsb = ps.boundStatementBuilder();
                bsb = CsvCqlValueBinder.bindRow(bsb, ps, row, cqlColumns.size(), codecRegistry);
                session.execute(bsb.build());
                inserted++;
                logLine.accept(String.format("Строка %d из %d записана (всего успешно: %d)", r + 1, limit, inserted));
            } catch (Exception e) {
                failed++;
                logLine.accept(String.format("Строка %d: ошибка — %s", r + 1, e.getMessage()));
            }
            if (onProgress != null) {
                onProgress.onRowFinished(inserted, r + 1, limit);
            }
        }

        logLine.accept(String.format("Готово. Успешно: %d, ошибок: %d, планировалось: %d", inserted, failed, limit));
        return new LoadResult(inserted, failed);
    }

    @FunctionalInterface
    public interface ProgressListener {
        void onRowFinished(int successfulInserts, int rowsProcessed, int totalPlanned);
    }

    private static String hintNoNode() {
        return "Нет доступных узлов для запроса. Проверьте contact points и порт; "
                + "поле «Local DC» должно совпадать с datacenter= в журнале после подключения (не с файлом на диске, если узел не перезапускали). "
                + "Отключитесь, исправьте Local DC, подключитесь снова.";
    }
}
