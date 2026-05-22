package com.tdengine.dbsync.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConfigurationProperties(prefix = "tdengine")
public class SyncProperties {

    public enum ConnectionMode {
        JDBC, RESTFUL
    }

    public enum RunMode {
        EXPORT, IMPORT
    }

    public enum Compression {
        GZIP
    }

    public enum DataFormat {
        CSV, JSON
    }

    private ConnectionMode connectionMode = ConnectionMode.JDBC;
    private JdbcConfig jdbc = new JdbcConfig();
    private RestfulConfig restful = new RestfulConfig();
    private RunMode mode = RunMode.EXPORT;
    private String dataDir = "./data";
    private String database;
    /** Target database for import. When null/empty, defaults to source database. */
    private String targetDatabase;
    private List<String> superTables = new ArrayList<>();
    /** Common non-time export conditions applied to all super tables */
    private String exportConditions = "";
    /** Per-super-table additional export conditions, keyed by stable name */
    private Map<String, String> stableConditions = new HashMap<>();
    /** Export start time (inclusive). Format: "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Blank=auto-detect. */
    private String startTime;
    /** Export end time (exclusive). Format: "yyyy-MM-dd" or "yyyy-MM-dd HH:mm:ss". Blank=auto-detect. */
    private String endTime;
    private int blockSize = 100000;
    private Compression compression = Compression.GZIP;
    /** Data file format: CSV (fast) or JSON (compatible) */
    private DataFormat format = DataFormat.CSV;
    /** Parallel threads for export/import, default 30 */
    private int parallel = 30;
    /** Batch insert size for import, default 5000 */
    private int batchSize = 5000;
    /** Pipeline queue size for import (number of batches buffered) */
    private int pipelineQueueSize = 10;
    /** Connection pool size (0 disables pooling, default 50) */
    private int connectionPoolSize = 50;

    public ConnectionMode getConnectionMode() {
        return connectionMode;
    }

    public void setConnectionMode(ConnectionMode connectionMode) {
        this.connectionMode = connectionMode;
    }

    public JdbcConfig getJdbc() {
        return jdbc;
    }

    public void setJdbc(JdbcConfig jdbc) {
        this.jdbc = jdbc;
    }

    public RestfulConfig getRestful() {
        return restful;
    }

    public void setRestful(RestfulConfig restful) {
        this.restful = restful;
    }

    public RunMode getMode() {
        return mode;
    }

    public void setMode(RunMode mode) {
        this.mode = mode;
    }

    public String getDataDir() {
        return dataDir;
    }

    public void setDataDir(String dataDir) {
        this.dataDir = dataDir;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Get the target database for import.
     * Falls back to source database ({@link #getDatabase()}) when not configured.
     */
    public String getTargetDatabase() {
        return targetDatabase != null && !targetDatabase.isBlank() ? targetDatabase : database;
    }

    public void setTargetDatabase(String targetDatabase) {
        this.targetDatabase = targetDatabase;
    }

    public List<String> getSuperTables() {
        return superTables;
    }

    public void setSuperTables(List<String> superTables) {
        this.superTables = superTables;
    }

    public String getExportConditions() {
        return exportConditions;
    }

    public void setExportConditions(String exportConditions) {
        this.exportConditions = exportConditions;
    }

    public Map<String, String> getStableConditions() {
        return stableConditions;
    }

    public void setStableConditions(Map<String, String> stableConditions) {
        this.stableConditions = stableConditions;
    }

    /**
     * Get the combined WHERE conditions for a specific super table.
     * Merges common conditions + per-table conditions with AND.
     */
    public String getCombinedConditions(String stableName) {
        String common = (exportConditions != null && !exportConditions.isBlank()) ? exportConditions.trim() : "";
        String specific = stableConditions != null ? stableConditions.getOrDefault(stableName, "").trim() : "";

        if (!common.isEmpty() && !specific.isEmpty()) {
            return common + " AND " + specific;
        } else if (!common.isEmpty()) {
            return common;
        } else if (!specific.isEmpty()) {
            return specific;
        }
        return "";
    }

    public boolean hasTimeRange() {
        return (startTime != null && !startTime.isBlank()) || (endTime != null && !endTime.isBlank());
    }

    /**
     * Parse startTime to LocalDateTime. Date-only format ("yyyy-MM-dd") is interpreted as midnight.
     */
    public LocalDateTime parseStartTime() {
        if (startTime == null || startTime.isBlank()) return null;
        String trimmed = startTime.trim();
        if (trimmed.length() <= 10) {
            return LocalDate.parse(trimmed, DateTimeFormatter.ISO_LOCAL_DATE).atStartOfDay();
        }
        return LocalDateTime.parse(trimmed, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * Parse endTime to LocalDateTime. Date-only format ("yyyy-MM-dd") is interpreted as next day midnight.
     */
    public LocalDateTime parseEndTime() {
        if (endTime == null || endTime.isBlank()) return null;
        String trimmed = endTime.trim();
        if (trimmed.length() <= 10) {
            return LocalDate.parse(trimmed, DateTimeFormatter.ISO_LOCAL_DATE).plusDays(1).atStartOfDay();
        }
        return LocalDateTime.parse(trimmed, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public Compression getCompression() {
        return compression;
    }

    public void setCompression(Compression compression) {
        this.compression = compression;
    }

    public DataFormat getFormat() {
        return format;
    }

    public void setFormat(DataFormat format) {
        this.format = format;
    }

    public int getParallel() {
        return parallel;
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getPipelineQueueSize() {
        return pipelineQueueSize;
    }

    public void setPipelineQueueSize(int pipelineQueueSize) {
        this.pipelineQueueSize = pipelineQueueSize;
    }

    public int getConnectionPoolSize() {
        return connectionPoolSize;
    }

    public void setConnectionPoolSize(int connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
    }

    public static class JdbcConfig {
        private String driverClassName = "com.taosdata.jdbc.TSDBDriver";
        private String url = "jdbc:TAOS://localhost:6030";
        private String username = "root";
        private String password = "taosdata";

        public String getDriverClassName() {
            return driverClassName;
        }

        public void setDriverClassName(String driverClassName) {
            this.driverClassName = driverClassName;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    public static class RestfulConfig {
        private String url = "http://localhost:6041";
        private String username = "root";
        private String password = "taosdata";

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }
}
