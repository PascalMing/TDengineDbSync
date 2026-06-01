package com.tdengine.dbsync.model;

public class DataColumn {

    private String name;
    private String type;
    private int length;

    public DataColumn() {
    }

    public DataColumn(String name, String type, int length) {
        this.name = name;
        this.type = type;
        this.length = length;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    @Override
    public String toString() {
        return "DataColumn{name='" + name + "', type='" + type + "', length=" + length + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataColumn that = (DataColumn) o;
        return length == that.length
                && name.equalsIgnoreCase(that.name)
                && type.equalsIgnoreCase(that.type);
    }

    @Override
    public int hashCode() {
        int result = name.toLowerCase().hashCode();
        result = 31 * result + type.toLowerCase().hashCode();
        result = 31 * result + length;
        return result;
    }
}
