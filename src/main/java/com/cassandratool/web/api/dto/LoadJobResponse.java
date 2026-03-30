package com.cassandratool.web.api.dto;

import java.util.List;

public class LoadJobResponse {

    private List<String> log;
    private boolean done;
    private int inserted;
    private int failed;
    private String error;

    public LoadJobResponse() {}

    public LoadJobResponse(List<String> log, boolean done, int inserted, int failed, String error) {
        this.log = log;
        this.done = done;
        this.inserted = inserted;
        this.failed = failed;
        this.error = error;
    }

    public List<String> getLog() {
        return log;
    }

    public void setLog(List<String> log) {
        this.log = log;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }

    public int getInserted() {
        return inserted;
    }

    public void setInserted(int inserted) {
        this.inserted = inserted;
    }

    public int getFailed() {
        return failed;
    }

    public void setFailed(int failed) {
        this.failed = failed;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }
}
