package com.cassandratool.service;

import com.cassandratool.model.ConnectionProfile;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Хранение списка подключений в JSON в домашней директории пользователя.
 */
public final class ConnectionStore {

    private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
    private static final Type LIST_TYPE = new TypeToken<List<ConnectionProfile>>() {}.getType();

    private final Path file;

    public ConnectionStore() {
        String home = System.getProperty("user.home");
        Path dir = Path.of(home, ".cassandra-csv-loader");
        this.file = dir.resolve("connections.json");
    }

    public Path getStoragePath() {
        return file;
    }

    public List<ConnectionProfile> loadAll() throws IOException {
        if (!Files.isRegularFile(file)) {
            return new ArrayList<>();
        }
        String json = Files.readString(file, StandardCharsets.UTF_8);
        if (json.isBlank()) {
            return new ArrayList<>();
        }
        List<ConnectionProfile> list = GSON.fromJson(json, LIST_TYPE);
        return list != null ? new ArrayList<>(list) : new ArrayList<>();
    }

    public void saveAll(List<ConnectionProfile> profiles) throws IOException {
        Files.createDirectories(file.getParent());
        Files.writeString(file, GSON.toJson(profiles), StandardCharsets.UTF_8);
    }
}
