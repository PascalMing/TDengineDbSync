package com.tdengine.dbsync.service;

import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.connection.TdConnectionFactory;
import com.tdengine.dbsync.exporter.DataExporter;
import com.tdengine.dbsync.exporter.TdDataExporter;
import com.tdengine.dbsync.importer.DataImporter;
import com.tdengine.dbsync.importer.TdDataImporter;
import com.tdengine.dbsync.model.SuperTableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

@Service
public class SyncService {

    private static final Logger log = LoggerFactory.getLogger(SyncService.class);

    private final SyncProperties properties;
    private final TdConnectionFactory connectionFactory;
    private final CheckpointManager checkpointManager;

    public SyncService(SyncProperties properties, TdConnectionFactory connectionFactory,
                       CheckpointManager checkpointManager) {
        this.properties = properties;
        this.connectionFactory = connectionFactory;
        this.checkpointManager = checkpointManager;
    }

    public void execute() {
        log.info("TDengine DbSync starting...");
        log.info("  Source: https://github.com/PascalMing");
        log.info("  Connection mode: {}", properties.getConnectionMode());
        log.info("  Run mode: {}", properties.getMode());
        log.info("  Database: {}", properties.getDatabase());
        String targetDb = properties.getTargetDatabase();
        if (!targetDb.equals(properties.getDatabase())) {
            log.info("  Target database: {}", targetDb);
        }
        if (properties.getConnectionMode() == SyncProperties.ConnectionMode.RESTFUL) {
            log.info("  REST connect timeout: {} ms", properties.getRestful().getConnectTimeout());
            log.info("  REST socket timeout: {} ms", properties.getRestful().getSocketTimeout());
            log.info("  REST request timeout: {} ms", properties.getRestful().getRequestTimeout());
        }
        log.info("  Data directory: {}", properties.getDataDir());
        log.info("  Format: {}", properties.getFormat());
        log.info("  File size: {} MB", properties.getFileSizeMb());
        log.info("  Batch size: {}", properties.getBatchSize());
        log.info("  Page size: {}", properties.getPageSize());
        log.info("  Parallel: {}", properties.getParallel());
        log.info("  Compression: {}", properties.getCompression());
        if (properties.hasTimeRange()) {
            log.info("  Start time: {}", properties.getStartTime());
            log.info("  End time: {}", properties.getEndTime());
        } else {
            log.info("  Time range: auto-detect");
        }
        if (properties.getExportConditions() != null && !properties.getExportConditions().isBlank()) {
            log.info("  Common export conditions: {}", properties.getExportConditions());
        }
        if (properties.getStableConditions() != null && !properties.getStableConditions().isEmpty()) {
            log.info("  Per-table conditions: {}", properties.getStableConditions());
        }

        // Initialize checkpoint manager and register shutdown hook
        checkpointManager.init();
        checkpointManager.registerShutdownHook();

        try {
            validateStableConditions(properties.getDatabase());
            switch (properties.getMode()) {
                case EXPORT -> {
                    DataExporter exporter = new TdDataExporter(properties, connectionFactory, checkpointManager);
                    exporter.exportData();
                }
                case IMPORT -> {
                    DataImporter importer = new TdDataImporter(properties, connectionFactory, checkpointManager);
                    importer.importData();
                }
            }
        } catch (Exception e) {
            log.error("Sync operation failed: {}", e.getMessage(), e);
            checkpointManager.save();
            throw new RuntimeException("Sync operation failed", e);
        }
    }

    private void validateStableConditions(String database) {
        Map<String, String> stableConditions = properties.getStableConditions();
        String exportConditions = properties.getExportConditions();
        if ((stableConditions == null || stableConditions.isEmpty()) && (exportConditions == null || exportConditions.isBlank())) {
            return;
        }

        Set<String> stableNames = new LinkedHashSet<>();
        if (stableConditions != null) {
            stableNames.addAll(stableConditions.keySet());
        }

        try (var conn = connectionFactory.create()) {
            for (String stableName : stableNames) {
                SuperTableMeta meta = conn.getSuperTableMeta(database, stableName);
                Set<String> validColumns = new LinkedHashSet<>();
                meta.getColumns().forEach(c -> validColumns.add(c.getName().toLowerCase()));
                meta.getTags().forEach(t -> validColumns.add(t.getName().toLowerCase()));

                String condition = stableConditions != null ? stableConditions.getOrDefault(stableName, null) : null;
                if (condition == null || condition.isBlank()) {
                    continue;
                }

                Set<String> referenced = extractConditionFields(condition);
                for (String field : referenced) {
                    if (!validColumns.contains(field.toLowerCase())) {
                        throw new IllegalArgumentException("Invalid stable condition field '" + field + "' for "
                                + stableName + ". Valid columns/tags: " + validColumns);
                    }
                }
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to validate stable-conditions: " + e.getMessage(), e);
        }
    }

    private Set<String> extractConditionFields(String condition) {
        Set<String> fields = new LinkedHashSet<>();
        if (condition == null || condition.isBlank()) {
            return fields;
        }

        String normalized = condition.replace("!=", " ")
                .replace(">=", " ")
                .replace("<=", " ")
                .replace("=", " ")
                .replace(">", " ")
                .replace("<", " ")
                .replace("(", " ")
                .replace(")", " ")
                .replace(",", " ");

        for (String token : normalized.split("\\s+")) {
            if (token.isBlank()) {
                continue;
            }
            if (token.startsWith("'") || token.startsWith("\"")) {
                continue;
            }
            if (token.matches("(?i)and|or|not|in|like|is|null|between|exists|true|false")) {
                continue;
            }
            if (token.matches("[0-9.]+")) {
                continue;
            }
            fields.add(token.replace("`", ""));
        }
        return fields;
    }
}
