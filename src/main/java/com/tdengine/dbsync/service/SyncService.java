package com.tdengine.dbsync.service;

import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.connection.TdConnectionFactory;
import com.tdengine.dbsync.exporter.DataExporter;
import com.tdengine.dbsync.exporter.TdDataExporter;
import com.tdengine.dbsync.importer.DataImporter;
import com.tdengine.dbsync.importer.TdDataImporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
        log.info("  Data directory: {}", properties.getDataDir());
        log.info("  Format: {}", properties.getFormat());
        log.info("  Block size: {}", properties.getBlockSize());
        log.info("  Batch size: {}", properties.getBatchSize());
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
}
