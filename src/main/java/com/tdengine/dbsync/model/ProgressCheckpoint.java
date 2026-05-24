package com.tdengine.dbsync.model;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
     */
    public static class StableProgress {
        private boolean schemaDone;
        /** Completed block file names -> record count */
        private Map<String, Long> completedFiles = new LinkedHashMap<>();
        private String currentFile;
        private long currentFileOffset;
        private long totalRecords;
        /**
         * Last exported timestamp (ISO format string).
         * Used for timestamp-based resume: WHERE ts > lastExportTs
         * instead of re-querying and skipping rows.
         */
        private String lastExportTs;
        /** Completed day strings (e.g. "2024-01-15") for day-level resume */
        private Set<String> completedDays = new LinkedHashSet<>();
        /** Currently processing day string (e.g. "2024-01-15") */
        private String currentDay;

        public StableProgress() {
        }

        public boolean isSchemaDone() {
            return schemaDone;
        }

        public void setSchemaDone(boolean schemaDone) {
            this.schemaDone = schemaDone;
        }

        public Map<String, Long> getCompletedFiles() {
            return completedFiles;
        }

        public void setCompletedFiles(Map<String, Long> completedFiles) {
            this.completedFiles = completedFiles;
        }

        public String getCurrentFile() {
            return currentFile;
        }

        public void setCurrentFile(String currentFile) {
            this.currentFile = currentFile;
        }

        public long getCurrentFileOffset() {
            return currentFileOffset;
        }

        public void setCurrentFileOffset(long currentFileOffset) {
            this.currentFileOffset = currentFileOffset;
        }

        public long getTotalRecords() {
            return totalRecords;
        }

        public void setTotalRecords(long totalRecords) {
            this.totalRecords = totalRecords;
        }

        public String getLastExportTs() {
            return lastExportTs;
        }

        public void setLastExportTs(String lastExportTs) {
            this.lastExportTs = lastExportTs;
        }

        public void markFileCompleted(String fileName, long recordCount) {
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
            this.completedDays = completedDays;
        }

        public String getCurrentDay() {
            return currentDay;
        }

        public void setCurrentDay(String currentDay) {
            this.currentDay = currentDay;
        }

        public boolean isDayCompleted(String dateStr) {
            return completedDays != null && completedDays.contains(dateStr);
        }

        public void markDayCompleted(String dateStr) {
            if (completedDays == null) {
                completedDays = new LinkedHashSet<>();
            }
            completedDays.add(dateStr);
            if (dateStr.equals(currentDay)) {
                currentDay = null;
                lastExportTs = null;
            }
        }
    }
}
