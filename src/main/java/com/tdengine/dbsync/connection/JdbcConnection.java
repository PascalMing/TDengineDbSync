package com.tdengine.dbsync.connection;

import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.model.SuperTableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcConnection implements TdConnection {

    private static final Logger log = LoggerFactory.getLogger(JdbcConnection.class);

    private final SyncProperties.JdbcConfig config;
    private Connection connection;

    public JdbcConnection(SyncProperties.JdbcConfig config) {
        this.config = config;
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            log.info("Establishing JDBC connection: {}", config.getUrl());
            connection = DriverManager.getConnection(
                    config.getUrl(),
                    config.getUsername(),
                    config.getPassword()
            );
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
                    log.info("TDengine server version: {}", rs.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("JDBC connection test failed: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> getSuperTableNames(String database) {
        List<String> names = new ArrayList<>();
        try {
            execute("USE " + database);
            query("SHOW STABLES", rs -> {
                while (rs.next()) {
                    names.add(rs.getString(1));
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to get super table names: " + e.getMessage(), e);
        }
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
    public ResultSet queryDirect(String sql) {
        try {
            Statement stmt = getConnection().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stmt.setFetchSize(10000);
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
                log.info("JDBC connection closed");
            } catch (SQLException e) {
                log.warn("Failed to close JDBC connection: {}", e.getMessage());
            }
        }
    }
}
