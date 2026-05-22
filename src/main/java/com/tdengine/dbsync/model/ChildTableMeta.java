package com.tdengine.dbsync.model;

import java.util.LinkedHashMap;

public class ChildTableMeta {

    private String tbname;
    private LinkedHashMap<String, String> tagValues = new LinkedHashMap<>();

    public ChildTableMeta() {
    }

    public String getTbname() {
        return tbname;
    }

    public void setTbname(String tbname) {
        this.tbname = tbname;
    }

    public LinkedHashMap<String, String> getTagValues() {
        return tagValues;
    }

    public void setTagValues(LinkedHashMap<String, String> tagValues) {
        this.tagValues = tagValues;
    }

    @Override
    public String toString() {
        return "ChildTableMeta{tbname='" + tbname + "', tagValues=" + tagValues + '}';
    }
}
