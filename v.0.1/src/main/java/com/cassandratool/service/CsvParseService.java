package com.cassandratool.service;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class CsvParseService {

    public CsvTableData readAll(Path path, Charset charset, boolean firstRowIsHeader) throws IOException, CsvException {
        try (Reader reader = Files.newBufferedReader(path, charset);
                CSVReader csv = new CSVReader(reader)) {
            List<String[]> all = csv.readAll();
            if (all.isEmpty()) {
                return new CsvTableData(List.of(), List.of());
            }
            if (firstRowIsHeader) {
                return parseWithHeaderRow(all);
            }
            return parseWithoutHeaderRow(all);
        }
    }

    public CsvTableData readAll(Path path, Charset charset) throws IOException, CsvException {
        return readAll(path, charset, true);
    }

    public CsvTableData readAll(Path path) throws IOException, CsvException {
        return readAll(path, StandardCharsets.UTF_8, true);
    }

    private static CsvTableData parseWithHeaderRow(List<String[]> all) {
        String[] header = all.get(0);
        List<String> columns = new ArrayList<>();
        for (String h : header) {
            columns.add(h != null ? h : "");
        }
        List<List<String>> rows = new ArrayList<>();
        for (int i = 1; i < all.size(); i++) {
            String[] line = all.get(i);
            List<String> row = new ArrayList<>(columns.size());
            for (int c = 0; c < columns.size(); c++) {
                row.add(c < line.length && line[c] != null ? line[c] : "");
            }
            rows.add(row);
        }
        return new CsvTableData(columns, rows);
    }

    /**
     * Все строки файла — данные; колонки именуются {@code col_1}, {@code col_2}, …
     */
    private static CsvTableData parseWithoutHeaderRow(List<String[]> all) {
        int maxCols = 0;
        for (String[] line : all) {
            maxCols = Math.max(maxCols, line != null ? line.length : 0);
        }
        List<String> columns = new ArrayList<>(maxCols);
        for (int i = 0; i < maxCols; i++) {
            columns.add("col_" + (i + 1));
        }
        List<List<String>> rows = new ArrayList<>(all.size());
        for (String[] line : all) {
            List<String> row = new ArrayList<>(maxCols);
            for (int c = 0; c < maxCols; c++) {
                row.add(line != null && c < line.length && line[c] != null ? line[c] : "");
            }
            rows.add(row);
        }
        return new CsvTableData(columns, rows);
    }
}
