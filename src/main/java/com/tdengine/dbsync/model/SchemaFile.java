package com.tdengine.dbsync.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

public class SchemaFile {

    private String database;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime exportTime;

    private Map<String, SuperTableMeta> superTables = new LinkedHashMap<>();

    public SchemaFile() {
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public LocalDateTime getExportTime() {
        return exportTime;
    }

    public void setExportTime(LocalDateTime exportTime) {
        this.exportTime = exportTime;
    }

    public Map<String, SuperTableMeta> getSuperTables() {
        return superTables;
    }

    public void setSuperTables(Map<String, SuperTableMeta> superTables) {
        this.superTables = superTables;
    }
}
