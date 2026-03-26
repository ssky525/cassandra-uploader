package com.cassandratool.model;

import java.util.Objects;
import java.util.UUID;

/**
 * Сохранённый профиль подключения к Cassandra.
 */
public final class ConnectionProfile {

    private String id;
    private String displayName;
    private String contactPoints;
    private int port;
    private String localDatacenter;
    private String username;
    private String password;

    public ConnectionProfile() {
        this.id = UUID.randomUUID().toString();
    }

    public ConnectionProfile(
            String id,
            String displayName,
            String contactPoints,
            int port,
            String localDatacenter,
            String username,
            String password) {
        this.id = id != null ? id : UUID.randomUUID().toString();
        this.displayName = displayName;
        this.contactPoints = contactPoints;
        this.port = port;
        this.localDatacenter = localDatacenter;
        this.username = username;
        this.password = password;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getContactPoints() {
        return contactPoints;
    }

    public void setContactPoints(String contactPoints) {
        this.contactPoints = contactPoints;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getLocalDatacenter() {
        return localDatacenter;
    }

    public void setLocalDatacenter(String localDatacenter) {
        this.localDatacenter = localDatacenter;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String toString() {
        return displayName != null && !displayName.isBlank()
                ? displayName
                : (contactPoints + ":" + port);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ConnectionProfile that = (ConnectionProfile) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}
