package com.cassandratool.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;

import java.util.Objects;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Вывод в лог сведений о кластере после подключения (datacenter узлов и т.д.).
 */
public final class CassandraClusterDiagnostics {

    private CassandraClusterDiagnostics() {}

    public static void logTopology(CqlSession session, Consumer<String> logLine) {
        Metadata meta = session.getMetadata();
        logLine.accept("Кластер: " + meta.getClusterName());
        logLine.accept("Поле «Local DC» должно совпадать с колонкой datacenter= ниже (как видит драйвер). "
                + "Если в cassandra-rackdc.properties другое dc= — ориентируйтесь на этот лог после перезапуска узла.");
        logLine.accept("Узлы:");
        for (Node node : meta.getNodes().values()) {
            logLine.accept(String.format(
                    "  • %s | datacenter=%s | rack=%s | состояние=%s",
                    node.getEndPoint(),
                    node.getDatacenter(),
                    node.getRack(),
                    node.getState()));
        }
    }

    /**
     * Если введённый Local DC не совпадает ни с одним узлом — типичная причина «No node was available» при SELECT.
     */
    public static void warnIfLocalDcMismatch(CqlSession session, String configuredLocalDc, Consumer<String> logLine) {
        if (configuredLocalDc == null || configuredLocalDc.isBlank()) {
            return;
        }
        String want = configuredLocalDc.trim();
        TreeSet<String> present = session.getMetadata().getNodes().values().stream()
                .map(Node::getDatacenter)
                .filter(Objects::nonNull)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toCollection(TreeSet::new));
        if (present.isEmpty()) {
            return;
        }
        if (!present.contains(want)) {
            logLine.accept(">>> ВНИМАНИЕ: «Local DC» = \"" + want + "\", а узлы кластера в DC: " + String.join(", ", present));
            logLine.accept(">>> Впишите в поле Local DC одно из этих имён (например datacenter1), нажмите «Отключиться» и снова «Подключиться».");
        }
    }

    public static String discoveredDatacenters(CqlSession session) {
        return session.getMetadata().getNodes().values().stream()
                .map(Node::getDatacenter)
                .filter(Objects::nonNull)
                .filter(s -> !s.isEmpty())
                .distinct()
                .sorted()
                .collect(Collectors.joining(", "));
    }
}
