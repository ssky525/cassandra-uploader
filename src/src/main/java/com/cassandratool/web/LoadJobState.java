package com.cassandratool.web;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class LoadJobState {

    private final List<String> logLines = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean done;
    private volatile int inserted;
    private volatile int failed;
    private volatile String error;

    public void appendLog(String line) {
        logLines.add(line);
    }

    public List<String> getLogLines() {
        synchronized (logLines) {
            return new ArrayList<>(logLines);
        }
    }

    public boolean isDone() {
        return done;
    }

    public void finish(int inserted, int failed) {
        this.inserted = inserted;
        this.failed = failed;
        this.done = true;
    }

    public void fail(String message) {
        this.error = message;
        this.done = true;
    }

    public int getInserted() {
        return inserted;
    }

    public int getFailed() {
        return failed;
    }

    public String getError() {
        return error;
    }
}
