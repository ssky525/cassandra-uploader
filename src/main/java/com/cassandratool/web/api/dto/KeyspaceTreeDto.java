package com.cassandratool.web.api.dto;

import java.util.List;

public class KeyspaceTreeDto {

    private String name;
    private List<String> tables;

    public KeyspaceTreeDto() {}

    public KeyspaceTreeDto(String name, List<String> tables) {
        this.name = name;
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getTables() {
        return tables;
    }

    public void setTables(List<String> tables) {
        this.tables = tables;
    }
}
