package com.tdengine.dbsync.connection;

import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.model.SuperTableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;
import java.util.List;

/**
 * REST API connection using taos-jdbcdriver's REST connector.
 * The REST API mode uses the JDBC-REST driver from taosdata which
 * communicates via HTTP with the TDengine REST service on port 6041.
 */
public class RestApiConnection implements TdConnection {

    private static final Logger log = LoggerFactory.getLogger(RestApiConnection.class);

    private final SyncProperties.RestfulConfig config;
    private Connection connection;

    public RestApiConnection(SyncProperties.RestfulConfig config) {
        this.config = config;
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            String url = config.getUrl();
            // Convert http URL to JDBC-REST URL format
            if (url.startsWith("http://")) {
                url = url.replace("http://", "jdbc:TAOS-RS://");
            } else if (url.startsWith("https://")) {
                url = url.replace("https://", "jdbc:TAOS-RS://");
            }
            // Remove trailing path if any
            if (url.endsWith("/")) {
                url = url.substring(0, url.length() - 1);
            }
            log.debug("Establishing REST API connection: {}", url);
            Properties props = new Properties();
            props.setProperty("user", config.getUsername());
            props.setProperty("password", config.getPassword());
            props.setProperty("httpConnectTimeout", String.valueOf(config.getConnectTimeout()));
            props.setProperty("httpSocketTimeout", String.valueOf(config.getSocketTimeout()));
            props.setProperty("messageWaitTimeout", String.valueOf(config.getRequestTimeout()));
            connection = DriverManager.getConnection(url, props);
        }
        return connection;
    }

    @Override
    public void testConnection() {
        try {
            Connection conn = getConnection();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT SERVER_VERSION()")) {
                if (rs.next()) {
                    log.debug("TDengine server version (REST): {}", rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("REST API connection test failed: " + e.getMessage(), e);
        }
    }

    @Override
    public String getServerVersion() {
        try {
            Connection conn = getConnection();
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT SERVER_VERSION()")) {
                if (rs.next()) {
                    return rs.getString(1);
                }
            }
        } catch (SQLException e) {
            log.debug("Failed to get server version: {}", e.getMessage());
        }
        return "unknown";
    }

    @Override
    public List<String> getSuperTableNames(String database) {
        List<String> names = new ArrayList<>();
        try {
            execute("USE " + database);
            query("SHOW STABLES", rs -> {
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name != null && !name.isBlank()) {
                        names.add(name);
                    }
                }
            });
        } catch (Exception e) {
            // Last resort: try information_schema (column name varies by version)
            try {
                query("SELECT table_name FROM information_schema.ins_stables WHERE db_name = '" + database + "'", rs -> {
                    while (rs.next()) {
                        String name = rs.getString(1);
                        if (name != null && !name.isBlank()) {
                            names.add(name);
                        }
                    }
                });
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get super table names: " + e.getMessage(), e);
            }
        }
        log.info("getSuperTableNames({}): found {} super table(s)", database, names.size());
        return names;
    }

    @Override
    public SuperTableMeta getSuperTableMeta(String database, String stableName) {
        try {
            execute("USE " + database);

            // Get CREATE STABLE statement
            String createStmt = null;
            try (Statement stmt = getConnection().createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW CREATE STABLE " + stableName)) {
                if (rs.next()) {
                    createStmt = rs.getString(2);
                }
            }

            SuperTableMeta meta = new SuperTableMeta(stableName, createStmt);

            // Get column and tag definitions via DESCRIBE
            try (Statement stmt = getConnection().createStatement();
                 ResultSet rs = stmt.executeQuery("DESCRIBE " + stableName)) {
                TdConnection.parseColumnsFromDescribe(rs, meta);
            }

            return meta;
        } catch (Exception e) {
            throw new RuntimeException("Failed to get super table meta for " + stableName + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void query(String sql, ResultSetHandler handler) {
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            handler.handle(rs);
        } catch (Exception e) {
            throw new RuntimeException("Query failed: " + sql + " - " + e.getMessage(), e);
        }
    }

    @Override
    public void execute(String sql) {
        try (Statement stmt = getConnection().createStatement()) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Execute failed: " + sql + " - " + e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate(String sql) {
        try (Statement stmt = getConnection().createStatement()) {
            return stmt.executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Execute update failed: " + sql + " - " + e.getMessage(), e);
        }
    }

    @Override
    public ResultSet queryDirect(String sql) {
        try {
            Statement stmt = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // Use a smaller fetch size to enable streaming mode, preventing OOM
            // when querying large result sets via the REST API.
            stmt.setFetchSize(2000);
            return stmt.executeQuery(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Direct query failed: " + sql + " - " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                connection.close();
                log.debug("REST API connection closed");
            } catch (SQLException e) {
                log.warn("Failed to close REST API connection: {}", e.getMessage());
            }
        }
    }
}
