package com.cassandratool.web.api.dto;

public class MetadataDescriptionDto {

    private String description;

    public MetadataDescriptionDto() {}

    public MetadataDescriptionDto(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
