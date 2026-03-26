package com.cassandratool.service;

import com.cassandratool.model.ConnectionProfile;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public final class CassandraSessionFactory {

    private CassandraSessionFactory() {}

    public static CqlSession connect(ConnectionProfile profile) {
        CqlSessionBuilder b = CqlSession.builder();
        String points = profile.getContactPoints();
        if (points == null || points.isBlank()) {
            throw new IllegalArgumentException("Укажите хотя бы один contact point");
        }
        String dc = profile.getLocalDatacenter();
        if (dc == null || dc.isBlank()) {
            throw new IllegalArgumentException("Укажите local datacenter");
        }
        int port = profile.getPort() > 0 ? profile.getPort() : 9042;

        for (String part : points.split("[,;\\s]+")) {
            String host = part.trim();
            if (!host.isEmpty()) {
                b.addContactPoint(new InetSocketAddress(host, port));
            }
        }

        b.withLocalDatacenter(dc.trim());

        String user = profile.getUsername();
        String pass = profile.getPassword();
        if (user != null && !user.isBlank()) {
            b.withAuthCredentials(user.trim(), pass != null ? pass : "");
        }

        return b.build();
    }

    public static List<String> validateProfile(ConnectionProfile p) {
        List<String> errors = new ArrayList<>();
        if (p.getContactPoints() == null || p.getContactPoints().isBlank()) {
            errors.add("Contact points");
        }
        if (p.getLocalDatacenter() == null || p.getLocalDatacenter().isBlank()) {
            errors.add("Local datacenter");
        }
        return errors;
    }
}
