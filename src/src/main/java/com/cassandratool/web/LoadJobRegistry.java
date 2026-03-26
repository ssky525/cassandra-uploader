package com.cassandratool.web;

import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class LoadJobRegistry {

    private final ConcurrentHashMap<String, LoadJobState> jobs = new ConcurrentHashMap<>();

    public void put(String id, LoadJobState state) {
        jobs.put(id, state);
    }

    public LoadJobState get(String id) {
        return jobs.get(id);
    }

    public void remove(String id) {
        jobs.remove(id);
    }
}
