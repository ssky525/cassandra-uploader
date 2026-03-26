package com.cassandratool.service;

import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

import java.util.List;
import java.util.Locale;

/**
 * Подстановка значений из CSV в {@link BoundStatementBuilder} с учётом типов CQL из prepared statement.
 */
public final class CsvCqlValueBinder {

    private CsvCqlValueBinder() {}

    public static BoundStatementBuilder bindRow(
            BoundStatementBuilder bsb,
            PreparedStatement ps,
            List<String> row,
            int columnCount,
            CodecRegistry registry) {

        for (int c = 0; c < columnCount; c++) {
            DataType dt = ps.getVariableDefinitions().get(c).getType();
            String raw = c < row.size() ? row.get(c) : null;
            bsb = bindOne(bsb, c, raw, dt, registry);
        }
        return bsb;
    }

    private static BoundStatementBuilder bindOne(
            BoundStatementBuilder bsb,
            int index,
            String raw,
            DataType dt,
            CodecRegistry registry) {

        String s = raw != null ? raw.trim() : "";

        if (dt.equals(DataTypes.BOOLEAN)) {
            if (s.isEmpty()) {
                return bsb.setToNull(index);
            }
            return bsb.setBoolean(index, parseBooleanLoose(s));
        }

        if (dt.equals(DataTypes.COUNTER)) {
            throw new IllegalArgumentException(
                    "Колонки типа counter нельзя задавать обычным INSERT из CSV");
        }

        @SuppressWarnings("unchecked")
        TypeCodec<Object> codec = (TypeCodec<Object>) registry.codecFor(dt);

        /*
         * text / varchar / ascii: codec.parse() ждёт CQL-литерал в одинарных кавычках ('...'),
         * а в CSV приходит обычная строка — всегда setString.
         * varchar в metadata может не совпадать с DataTypes.TEXT по equals, поэтому ориентируемся на Java-тип codec.
         */
        if (String.class.equals(codec.getJavaType().getRawType())) {
            return bsb.setString(index, s);
        }

        if (s.isEmpty()) {
            return bsb.setToNull(index);
        }

        try {
            Object value = codec.parse(normalizeForCodec(s, dt));
            return bsb.set(index, value, codec);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Значение \"" + s + "\" для типа " + dt.asCql(true, true) + ": " + e.getMessage(), e);
        }
    }

    private static String normalizeForCodec(String s, DataType dt) {
        if (dt.equals(DataTypes.UUID) || dt.equals(DataTypes.TIMEUUID)) {
            String t = s.trim();
            if (t.startsWith("\"") && t.endsWith("\"") && t.length() >= 2) {
                return t.substring(1, t.length() - 1);
            }
        }
        return s;
    }

    private static boolean parseBooleanLoose(String s) {
        String x = s.trim().toLowerCase(Locale.ROOT);
        if (x.equals("true") || x.equals("1") || x.equals("yes") || x.equals("t") || x.equals("y")) {
            return true;
        }
        if (x.equals("false") || x.equals("0") || x.equals("no") || x.equals("f") || x.equals("n")) {
            return false;
        }
        throw new IllegalArgumentException("Ожидалось true/false, 1/0, yes/no, получено: " + s);
    }
}
