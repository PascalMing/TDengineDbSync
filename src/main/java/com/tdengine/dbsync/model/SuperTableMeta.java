package com.tdengine.dbsync.model;

import java.util.ArrayList;
import java.util.List;

public class SuperTableMeta {

    private String name;
    private String createStmt;
    private List<DataColumn> columns = new ArrayList<>();
    private List<DataColumn> tags = new ArrayList<>();

    public SuperTableMeta() {
    }

    public SuperTableMeta(String name, String createStmt) {
        this.name = name;
        this.createStmt = createStmt;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCreateStmt() {
        return createStmt;
    }

    public void setCreateStmt(String createStmt) {
        this.createStmt = createStmt;
    }

    public List<DataColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<DataColumn> columns) {
        this.columns = columns;
    }

    public List<DataColumn> getTags() {
        return tags;
    }

    public void setTags(List<DataColumn> tags) {
        this.tags = tags;
    }

    @Override
    public String toString() {
        return "SuperTableMeta{name='" + name + "', createStmt='" + createStmt + "', columns=" + columns + ", tags=" + tags + '}';
    }
}
