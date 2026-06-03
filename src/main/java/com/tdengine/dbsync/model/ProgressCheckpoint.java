package com.tdengine.dbsync.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Progress checkpoint file for resume support.
 * Stored as {database}_progress.json in the data directory.
 */
public class ProgressCheckpoint {

    private int version = 2;

    private String database;

    private String mode; // "EXPORT" or "IMPORT"

    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss")
    private LocalDateTime lastUpdateTime;

    /** Per-super-table progress: stableName -> StableProgress */
    private Map<String, StableProgress> stables = new ConcurrentHashMap<>();

    /** Time range used for this export run (for resume validation) */
    private String timeRangeStart;
    private String timeRangeEnd;

    public ProgressCheckpoint() {
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public LocalDateTime getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(LocalDateTime lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public Map<String, StableProgress> getStables() {
        return stables;
    }

    public void setStables(Map<String, StableProgress> stables) {
        this.stables = stables == null ? new ConcurrentHashMap<>() : new ConcurrentHashMap<>(stables);
    }

    public StableProgress getOrCreateStable(String stableName) {
        return stables.computeIfAbsent(stableName, k -> new StableProgress());
    }

    public String getTimeRangeStart() {
        return timeRangeStart;
    }

    public void setTimeRangeStart(String timeRangeStart) {
        this.timeRangeStart = timeRangeStart;
    }

    public String getTimeRangeEnd() {
        return timeRangeEnd;
    }

    public void setTimeRangeEnd(String timeRangeEnd) {
        this.timeRangeEnd = timeRangeEnd;
    }

    /**
     * Per-super-table progress tracking.
     * Thread-safe: used by parallel partition exporters concurrently.
     */
    public static class StableProgress {
        private boolean schemaDone;
        /** Completed block file names -> record count */
        private final Map<String, Long> completedFiles = new ConcurrentHashMap<>();
        private volatile String currentFile;
        private volatile long currentFileOffset;
        private long totalRecords;
        /**
         * Last exported timestamp (ISO format string).
         * Used for timestamp-based resume: WHERE ts > lastExportTs
         * instead of re-querying and skipping rows.
         */
        private volatile String lastExportTs;
        /** Completed day strings (e.g. "2024-01-15") for day-level resume */
        private final Set<String> completedDays = ConcurrentHashMap.newKeySet();
        /** Currently processing day string (e.g. "2024-01-15") */
        private volatile String currentDay;
        /**
         * Whether all child table tags have been verified (reset=0 on last run).
         * When true, the reconciliation phase skips tag verification entirely,
         * avoiding 280k+ SELECT + ALTER TABLE calls on re-import.
         */
        private boolean tagsVerified;

        public StableProgress() {
        }

        public boolean isSchemaDone() {
            return schemaDone;
        }

        public synchronized void setSchemaDone(boolean schemaDone) {
            this.schemaDone = schemaDone;
        }

        public Map<String, Long> getCompletedFiles() {
            return completedFiles;
        }

        public void setCompletedFiles(Map<String, Long> completedFiles) {
            this.completedFiles.clear();
            if (completedFiles != null) {
                this.completedFiles.putAll(completedFiles);
            }
        }

        public String getCurrentFile() {
            return currentFile;
        }

        public synchronized void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }

        public long getCurrentFileOffset() {
            return currentFileOffset;
        }

        public synchronized void setCurrentFileOffset(long currentFileOffset) {
            this.currentFileOffset = currentFileOffset;
        }

        public long getTotalRecords() {
            return totalRecords;
        }

        public synchronized void setTotalRecords(long totalRecords) {
            this.totalRecords = totalRecords;
        }

        /**
         * Atomically add delta to totalRecords. Thread-safe for concurrent partition calls.
         */
        public synchronized long addTotalRecords(long delta) {
            this.totalRecords += delta;
            return this.totalRecords;
        }

        public String getLastExportTs() {
            return lastExportTs;
        }

        public synchronized void setLastExportTs(String lastExportTs) {
            this.lastExportTs = lastExportTs;
        }

        public synchronized void markFileCompleted(String fileName, long recordCount) {
            completedFiles.put(fileName, recordCount);
            currentFile = null;
            currentFileOffset = 0;
        }

        public boolean isFileCompleted(String fileName) {
            return completedFiles.containsKey(fileName);
        }

        public Set<String> getCompletedDays() {
            return completedDays;
        }

        public void setCompletedDays(Set<String> completedDays) {
            this.completedDays.clear();
            if (completedDays != null) {
                this.completedDays.addAll(completedDays);
            }
        }

        public String getCurrentDay() {
            return currentDay;
        }

        public synchronized void setCurrentDay(String currentDay) {
            this.currentDay = currentDay;
        }

        public boolean isDayCompleted(String dateStr) {
            return completedDays != null && completedDays.contains(dateStr);
        }

        public synchronized void markDayCompleted(String dateStr) {
            completedDays.add(dateStr);
            if (dateStr.equals(currentDay)) {
                currentDay = null;
                lastExportTs = null;
            }
        }

        public boolean isTagsVerified() {
            return tagsVerified;
        }

        public synchronized void setTagsVerified(boolean tagsVerified) {
            this.tagsVerified = tagsVerified;
        }
    }
}
