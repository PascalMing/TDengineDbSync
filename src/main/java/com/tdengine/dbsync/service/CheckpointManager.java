package com.tdengine.dbsync.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.model.ProgressCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages progress checkpoint persistence for resume support.
 * Writes checkpoint to disk on shutdown hook and periodically.
 */
@Component
public class CheckpointManager {

    private static final Logger log = LoggerFactory.getLogger(CheckpointManager.class);

    private final SyncProperties properties;
    private final ObjectMapper objectMapper;
    private volatile ProgressCheckpoint checkpoint;
    private final AtomicBoolean dirty = new AtomicBoolean(false);
    private volatile Path checkpointPath;

    public CheckpointManager(SyncProperties properties) {
        this.properties = properties;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    /**
     * Initialize checkpoint: load existing or create new.
     */
    public synchronized void init() {
        Path dataDir = Path.of(properties.getDataDir(), properties.getDatabase());
        checkpointPath = dataDir.resolve(properties.getDatabase() + "_progress.json");

        if (Files.exists(checkpointPath)) {
            try {
                checkpoint = objectMapper.readValue(checkpointPath.toFile(), ProgressCheckpoint.class);
                log.info("Loaded existing checkpoint: mode={}, updated={}", checkpoint.getMode(), checkpoint.getLastUpdateTime());
                logCheckpointSummary();
            } catch (Exception e) {
                log.warn("Failed to load checkpoint, starting fresh: {}", e.getMessage());
                checkpoint = createNewCheckpoint();
            }
        } else {
            checkpoint = createNewCheckpoint();
        }
    }

    private ProgressCheckpoint createNewCheckpoint() {
        ProgressCheckpoint cp = new ProgressCheckpoint();
        cp.setDatabase(properties.getDatabase());
        cp.setMode(properties.getMode().name());
        cp.setLastUpdateTime(LocalDateTime.now());
        return cp;
    }

    private void logCheckpointSummary() {
        if (checkpoint == null) return;
        for (var entry : checkpoint.getStables().entrySet()) {
            var sp = entry.getValue();
            log.info("  Checkpoint [{}]: schemaDone={}, completedFiles={}, currentFile={}, totalRecords={}",
                    entry.getKey(), sp.isSchemaDone(), sp.getCompletedFiles().size(),
                    sp.getCurrentFile(), sp.getTotalRecords());
        }
    }

    /**
     * Get the current checkpoint (thread-safe read).
     */
    public ProgressCheckpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Mark checkpoint as needing persistence.
     */
    public void markDirty() {
        dirty.set(true);
    }

    /**
     * Save checkpoint to disk if dirty.
     */
    public synchronized void saveIfDirty() {
        if (!dirty.get() || checkpointPath == null) return;
        save();
    }

    /**
     * Force save checkpoint to disk.
     */
    public synchronized void save() {
        if (checkpoint == null || checkpointPath == null) return;
        try {
            checkpoint.setLastUpdateTime(LocalDateTime.now());
            Files.createDirectories(checkpointPath.getParent());
            objectMapper.writeValue(checkpointPath.toFile(), checkpoint);
            dirty.set(false);
            log.debug("Checkpoint saved to {}", checkpointPath);
        } catch (Exception e) {
            log.error("Failed to save checkpoint: {}", e.getMessage());
        }
    }

    /**
     * Delete checkpoint file after successful completion.
     */
    public synchronized void delete() {
        if (checkpointPath != null && Files.exists(checkpointPath)) {
            try {
                Files.delete(checkpointPath);
                log.info("Checkpoint file deleted: {}", checkpointPath);
            } catch (Exception e) {
                log.warn("Failed to delete checkpoint: {}", e.getMessage());
            }
        }
        checkpoint = null;
        dirty.set(false);
    }

    /**
     * Register a shutdown hook for graceful exit.
     */
    public void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (checkpoint != null && dirty.get()) {
                log.info("Shutdown hook: saving checkpoint...");
                save();
                log.info("Checkpoint saved on shutdown. Resume with same configuration to continue.");
            }
        }, "checkpoint-shutdown-hook"));
    }
}
