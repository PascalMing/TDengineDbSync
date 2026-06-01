package com.tdengine.dbsync.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ChildTableManifest {

    private String database;

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime exportTime;

    private Map<String, List<ChildTableMeta>> superTables = new LinkedHashMap<>();

    public ChildTableManifest() {
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

    public Map<String, List<ChildTableMeta>> getSuperTables() {
        return superTables;
    }

    public void setSuperTables(Map<String, List<ChildTableMeta>> superTables) {
        this.superTables = superTables;
    }
}
