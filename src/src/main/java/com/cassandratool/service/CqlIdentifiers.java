package com.cassandratool.service;

/**
 * Приведение имён колонок из CSV к допустимым идентификаторам CQL.
 */
public final class CqlIdentifiers {

    private CqlIdentifiers() {}

    public static String sanitize(String raw) {
        if (raw == null || raw.isBlank()) {
            return "col_empty";
        }
        String s = raw.trim().replaceAll("[^a-zA-Z0-9_]", "_");
        if (s.isEmpty()) {
            s = "col";
        }
        if (Character.isDigit(s.charAt(0))) {
            s = "c_" + s;
        }
        return s.toLowerCase();
    }

    /** Идентификатор в двойных кавычках для CQL (keyspace, таблица, колонка). */
    public static String doubleQuoteIdent(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Пустой идентификатор CQL");
        }
        String n = name.trim();
        return "\"" + n.replace("\"", "\"\"") + "\"";
    }
}
