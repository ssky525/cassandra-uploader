package com.cassandratool.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Результат разбора CSV: заголовки и строки.
 */
public final class CsvTableData {

    private final List<String> columnNames;
    private final List<List<String>> rows;

    public CsvTableData(List<String> columnNames, List<List<String>> rows) {
        this.columnNames = Collections.unmodifiableList(new ArrayList<>(columnNames));
        this.rows = Collections.unmodifiableList(new ArrayList<>(rows));
    }

    public List<String> getColumnNames() {
        return columnNames;
    }

    public List<List<String>> getRows() {
        return rows;
    }

    public int rowCount() {
        return rows.size();
    }
}
