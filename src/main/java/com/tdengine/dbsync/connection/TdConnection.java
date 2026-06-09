package com.tdengine.dbsync.connection;

import com.tdengine.dbsync.model.DataColumn;
import com.tdengine.dbsync.model.SuperTableMeta;

import java.sql.ResultSet;
import java.util.List;

/**
 * TDengine connection abstraction supporting JDBC and REST API modes.
 */
public interface TdConnection extends AutoCloseable {

    /**
     * Test the connection.
     */
    void testConnection();

    /**
     * Get the TDengine server version.
     */
    String getServerVersion();

    /**
     * Get all super table names in the specified database.
     */
    List<String> getSuperTableNames(String database);

    /**
     * Get super table metadata (DDL, columns, tags).
     */
    SuperTableMeta getSuperTableMeta(String database, String stableName);

    /**
     * Execute a query and process results with the given handler.
     */
    void query(String sql, ResultSetHandler handler);

    /**
     * Execute a SQL statement (CREATE, INSERT, USE, etc.).
     */
    void execute(String sql);

    /**
     * Execute an INSERT/UPDATE/DELETE and return the number of affected rows.
     * TDengine may silently truncate large INSERTs; the caller MUST verify
     * the returned count matches expectations.
     */
    int executeUpdate(String sql);

    /**
     * Execute a query and return the result set directly.
     * Caller is responsible for closing resources.
     */
    ResultSet queryDirect(String sql);

    /**
     * Parse column definitions from DESCRIBE result set.
     */
    static void parseColumnsFromDescribe(ResultSet rs, SuperTableMeta meta) throws Exception {
        while (rs.next()) {
            String colName = rs.getString(1);
            String colType = rs.getString(2);
            String note = rs.getString(4);

            DataColumn column = parseColumnType(colName, colType);

            if ("TAG".equalsIgnoreCase(note)) {
                meta.getTags().add(column);
            } else {
                meta.getColumns().add(column);
            }
        }
    }

    /**
     * Parse column type string like "INT", "NCHAR(50)", "DOUBLE" into DataColumn.
     */
    static DataColumn parseColumnType(String name, String typeStr) {
        int length = 0;
        String type = typeStr;
        int parenIdx = typeStr.indexOf('(');
        if (parenIdx > 0) {
            type = typeStr.substring(0, parenIdx).toUpperCase();
            int closeIdx = typeStr.indexOf(')', parenIdx);
            if (closeIdx > parenIdx) {
                try {
                    length = Integer.parseInt(typeStr.substring(parenIdx + 1, closeIdx).trim());
                } catch (NumberFormatException ignored) {
                }
            }
        } else {
            type = typeStr.toUpperCase();
        }
        return new DataColumn(name, type, length);
    }

    @FunctionalInterface
    interface ResultSetHandler {
        void handle(ResultSet rs) throws Exception;
    }
}
