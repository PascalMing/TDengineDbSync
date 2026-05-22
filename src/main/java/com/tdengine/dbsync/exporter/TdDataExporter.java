package com.tdengine.dbsync.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.connection.TdConnection;
import com.tdengine.dbsync.connection.TdConnectionFactory;
import com.tdengine.dbsync.model.ChildTableManifest;
import com.tdengine.dbsync.model.ChildTableMeta;
import com.tdengine.dbsync.model.DataColumn;
import com.tdengine.dbsync.model.ProgressCheckpoint;
import com.tdengine.dbsync.model.SchemaFile;
import com.tdengine.dbsync.model.SuperTableMeta;
import com.tdengine.dbsync.service.CheckpointManager;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class TdDataExporter implements DataExporter {

    private static final Logger log = LoggerFactory.getLogger(TdDataExporter.class);
    private static final DateTimeFormatter DATE_DIR_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter TS_FILE_FMT = DateTimeFormatter.ofPattern("HHmmssSSS")
            .withZone(ZoneId.systemDefault());
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000;
    private static final long CHECKPOINT_SAVE_INTERVAL_MS = 30_000;
    private static final char FIELD_SEP = '\t';
    private static final String NULL_MARKER = "\\N";

    private final SyncProperties properties;
    private final TdConnectionFactory connectionFactory;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper objectMapper;

    public TdDataExporter(SyncProperties properties, TdConnectionFactory connectionFactory,
                          CheckpointManager checkpointManager) {
        this.properties = properties;
        this.connectionFactory = connectionFactory;
        this.checkpointManager = checkpointManager;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @Override
    public void exportData() throws Exception {
        String database = properties.getDatabase();
        log.info("========== Start exporting database: {} ==========", database);
        log.info("  Format: {}, Parallel: {}, Block size: {}",
                properties.getFormat(), properties.getParallel(), properties.getBlockSize());

        ProgressCheckpoint checkpoint = checkpointManager.getCheckpoint();
        if (checkpoint != null && !checkpoint.getStables().isEmpty()) {
            log.info("Resuming from previous checkpoint...");
        }

        Path dataDir = Path.of(properties.getDataDir(), database);
        Files.createDirectories(dataDir);

        try (TdConnection metaConn = connectionFactory.create()) {
            metaConn.testConnection();

            List<String> stableNames = resolveSuperTables(database, metaConn);
            log.info("Found {} super table(s) to export: {}", stableNames.size(), stableNames);

            SchemaFile schemaFile = exportSchema(database, stableNames, dataDir, metaConn, checkpoint);

            // Export child table manifest (non-blocking: failure won't stop data export)
            try {
                exportChildTableManifest(database, stableNames, dataDir, metaConn, schemaFile);
            } catch (Exception e) {
                log.warn("Failed to export child table manifest, data export will continue: {}", e.getMessage());
            }

            // Resolve time range
            LocalDateTime[] timeRange = resolveTimeRange(database, stableNames, metaConn, schemaFile);
            LocalDateTime rangeStart = timeRange[0];
            LocalDateTime rangeEnd = timeRange[1];
            log.info("Export time range: {} ~ {}", rangeStart, rangeEnd);

            // Validate checkpoint time range
            if (checkpoint != null) {
                String cpStart = checkpoint.getTimeRangeStart();
                String cpEnd = checkpoint.getTimeRangeEnd();
                String curStart = rangeStart.toString();
                String curEnd = rangeEnd.toString();
                if (cpStart != null && cpEnd != null && (!cpStart.equals(curStart) || !cpEnd.equals(curEnd))) {
                    log.warn("Time range differs from checkpoint (was: {} ~ {}, now: {} ~ {}). Starting fresh.",
                            cpStart, cpEnd, curStart, curEnd);
                    checkpointManager.delete();
                    checkpoint = checkpointManager.getCheckpoint();
                }
            }

            // Save time range to checkpoint
            if (checkpoint != null) {
                checkpoint.setTimeRangeStart(rangeStart.toString());
                checkpoint.setTimeRangeEnd(rangeEnd.toString());
                checkpointManager.markDirty();
            }

            // Generate day list
            List<LocalDate> dayList = generateDayList(rangeStart, rangeEnd);
            log.info("Export spans {} day(s): {} ~ {}",
                    dayList.size(),
                    dayList.isEmpty() ? "N/A" : dayList.getFirst(),
                    dayList.isEmpty() ? "N/A" : dayList.getLast());

            // Build partitions (stable + optional child-table-group)
            int parallelism = properties.getParallel();
            List<Partition> partitions = buildPartitions(database, stableNames, metaConn, parallelism);
            log.info("Created {} partition(s) across {} parallel thread(s)", partitions.size(), parallelism);

            // Iterate by day (outer loop), partitions run in parallel within each day
            for (LocalDate dayVal : dayList) {
                final LocalDate day = dayVal;
                // Check if all stables have this day completed
                boolean allDone = true;
                String dayStr = day.toString();
                for (String stableName : stableNames) {
                    ProgressCheckpoint.StableProgress sp = checkpoint != null
                            ? checkpoint.getOrCreateStable(stableName) : null;
                    if (sp == null || !sp.isDayCompleted(dayStr)) {
                        allDone = false;
                        break;
                    }
                }
                if (allDone) {
                    log.debug("  Day {} already completed for all stables, skipping", dayStr);
                    continue;
                }

                log.info("=== Exporting day: {} ===", dayStr);

                // Set currentDay for each stable
                for (String stableName : stableNames) {
                    ProgressCheckpoint.StableProgress sp = checkpoint != null
                            ? checkpoint.getOrCreateStable(stableName) : null;
                    if (sp != null && !sp.isDayCompleted(dayStr)) {
                        sp.setCurrentDay(dayStr);
                    }
                }
                if (checkpoint != null) checkpointManager.markDirty();

                // Submit all partitions for this day in parallel
                ExecutorService executor = Executors.newFixedThreadPool(parallelism,
                        r -> {
                            Thread t = new Thread(r, "export-day-worker");
                            t.setDaemon(true);
                            return t;
                        });

                List<Future<Void>> futures = new ArrayList<>();
                int submittedCount = 0;
                int skippedCount = 0;
                for (Partition partVal : partitions) {
                    final Partition partition = partVal;
                    ProgressCheckpoint.StableProgress sp = checkpoint != null
                            ? checkpoint.getOrCreateStable(partition.stableName) : null;
                    if (sp != null && sp.isDayCompleted(dayStr)) {
                        skippedCount++;
                        log.debug("  [{}][{}] Day already completed, skipping partition",
                                partition.stableName, dayStr);
                        continue;
                    }
                    submittedCount++;
                    log.debug("  [{}][{}] Submitting partition for export", partition.stableName, dayStr);

                    futures.add(executor.submit(() -> {
                        try (TdConnection dataConn = connectionFactory.create()) {
                            log.debug("  [{}][{}] Partition execution started (connection created)",
                                    partition.stableName, day);
                            if (partition.childTable != null) {
                                exportSingleChildTableData(database, partition, day, rangeStart, rangeEnd,
                                        schemaFile.getSuperTables().get(partition.stableName),
                                        dataDir, dataConn);
                            } else {
                                exportStableDayData(database, partition.stableName, day, rangeStart, rangeEnd,
                                        schemaFile.getSuperTables().get(partition.stableName),
                                        dataDir, dataConn);
                            }
                            log.debug("  [{}][{}] Partition execution completed successfully",
                                    partition.stableName, day);
                        } catch (Exception e) {
                            log.error("  [{}][{}] Partition execution FAILED: {}",
                                    partition.stableName, day, e.getMessage(), e);
                            throw e;
                        }
                        return null;
                    }));
                }
                log.info("  Day {}: submitted {} partition(s), skipped {} partition(s)",
                        dayStr, submittedCount, skippedCount);

                List<Exception> errors = new ArrayList<>();
                for (Future<Void> future : futures) {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        errors.add(e.getCause() != null ? (Exception) e.getCause() : e);
                    }
                }

                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);

                if (!errors.isEmpty()) {
                    for (Exception ex : errors) {
                        log.error("Export task failed for day {}: {}", dayStr, ex.getMessage());
                    }
                    throw new RuntimeException("Some export tasks failed for day " + dayStr);
                }

                // Mark day completed for all stables
                for (String stableName : stableNames) {
                    ProgressCheckpoint.StableProgress sp = checkpoint != null
                            ? checkpoint.getOrCreateStable(stableName) : null;
                    if (sp != null && !sp.isDayCompleted(dayStr)) {
                        sp.markDayCompleted(dayStr);
                    }
                }
                if (checkpoint != null) checkpointManager.saveIfDirty();

                log.info("=== Day {} completed ===", dayStr);
            }
        }

        checkpointManager.delete();
        log.info("========== Export completed for database: {} ==========", database);
    }

    // ---- Partition definition (stable + optional single child table, no date) ----

    static class Partition {
        final String stableName;
        /** If non-null, this partition exports a single child table directly */
        final String childTable;
        /** Partition index for file naming */
        final int partitionIndex;

        Partition(String stableName, String childTable, int partitionIndex) {
            this.stableName = stableName;
            this.childTable = childTable;
            this.partitionIndex = partitionIndex;
        }
    }

    /**
     * Build partitions by splitting super tables into parallel groups.
     * When there are fewer super tables than parallel threads, list matching child tables
     * and create one partition per child table for direct parallel export.
     * Each partition queries a single child table directly (no tbname IN clause needed).
     */
    private List<Partition> buildPartitions(String database, List<String> stableNames,
                                             TdConnection metaConn, int parallelism) throws Exception {
        List<Partition> partitions = new ArrayList<>();

        if (stableNames == null || stableNames.isEmpty()) {
            log.warn("No super tables found, no partitions to build");
            return partitions;
        }

        if (stableNames.size() >= parallelism) {
            for (String stableName : stableNames) {
                partitions.add(new Partition(stableName, null, 0));
            }
        } else {
            for (String stableName : stableNames) {
                String conditions = properties.getCombinedConditions(stableName);
                List<String> allChildTables = getChildTableNames(database, stableName, metaConn, conditions);
                log.info("  Super table {} has {} child table(s) matching conditions", stableName, allChildTables.size());

                if (allChildTables.isEmpty()) {
                    partitions.add(new Partition(stableName, null, 0));
                } else {
                    // Create one partition per child table for direct parallel export
                    for (int i = 0; i < allChildTables.size(); i++) {
                        partitions.add(new Partition(stableName, allChildTables.get(i), i));
                    }
                    log.info("  Created {} partition(s) for {} (one per child table)", allChildTables.size(), stableName);
                }
            }
        }

        return partitions;
    }

    private List<String> getChildTableNames(String database, String stableName, TdConnection conn) {
        return getChildTableNames(database, stableName, conn, null);
    }

    /**
     * Get child table names for a super table, optionally filtered by conditions.
     * When {@code conditions} is non-empty, uses {@code WHERE {conditions} GROUP BY TBNAME}
     * to return only matching child tables (skips information_schema as it cannot filter by tag values).
     * When no conditions, uses information_schema first for fast metadata query, falling back
     * to GROUP BY TBNAME.
     */
    private List<String> getChildTableNames(String database, String stableName, TdConnection conn,
                                             String conditions) {
        List<String> names = new ArrayList<>();

        // When conditions are specified, query with WHERE to filter by tag values directly
        if (conditions != null && !conditions.isBlank()) {
            try {
                conn.query("SELECT TBNAME FROM " + database + "." + stableName
                        + " WHERE " + conditions + " GROUP BY TBNAME", rs -> {
                    while (rs.next()) {
                        String tbname = rs.getString(1);
                        if (tbname != null && !tbname.isBlank()) {
                            names.add(tbname);
                        }
                    }
                });
                log.debug("  Filtered child tables for {} with conditions: {} (found {})",
                        stableName, conditions, names.size());
            } catch (Exception e) {
                log.warn("  Could not list filtered child tables for {}: {}", stableName, e.getMessage());
            }
            return names;
        }

        // No conditions: use information_schema for fast metadata query (TDengine 3.x+)
        try {
            conn.query(
                "SELECT table_name FROM information_schema.ins_child_tables " +
                "WHERE db_name = '" + database + "' AND stable_name = '" + stableName + "'",
                rs -> {
                    while (rs.next()) {
                        String tbname = rs.getString(1);
                        if (tbname != null && !tbname.isBlank()) {
                            names.add(tbname);
                        }
                    }
                }
            );
        } catch (Exception e) {
            log.debug("  information_schema query failed for {}: {}", stableName, e.getMessage());
        }
        // Fallback to GROUP BY TBNAME if information_schema returned no results
        if (names.isEmpty()) {
            try {
                conn.query("SELECT TBNAME FROM " + database + "." + stableName + " GROUP BY TBNAME", rs -> {
                    while (rs.next()) {
                        String tbname = rs.getString(1);
                        if (tbname != null && !tbname.isBlank()) {
                            names.add(tbname);
                        }
                    }
                });
            } catch (Exception e) {
                log.debug("  Could not list child tables for {}: {}", stableName, e.getMessage());
            }
        }
        return names;
    }

    // ---- Time range resolution ----

    /**
     * Resolve the export time range from configuration or auto-detect from database.
     * Returns [start, end] as LocalDateTime.
     */
    private LocalDateTime[] resolveTimeRange(String database, List<String> stableNames,
                                              TdConnection conn, SchemaFile schemaFile) throws Exception {
        LocalDateTime start = properties.parseStartTime();
        LocalDateTime end = properties.parseEndTime();

        if (start != null && end != null) {
            return new LocalDateTime[]{start, end};
        }

        // Auto-detect from database
        log.info("Auto-detecting time range from database...");
        LocalDateTime globalMin = null;
        LocalDateTime globalMax = null;

        for (String stableName : stableNames) {
            try {
                SuperTableMeta meta = schemaFile.getSuperTables().get(stableName);
                String tsColumn = meta != null ? getTimestampColumnName(meta) : "ts";
                LocalDateTime[] range = queryTimeRange(database, stableName, conn, tsColumn);
                if (range[0] != null && (globalMin == null || range[0].isBefore(globalMin))) {
                    globalMin = range[0];
                }
                if (range[1] != null && (globalMax == null || range[1].isAfter(globalMax))) {
                    globalMax = range[1];
                }
            } catch (Exception e) {
                log.warn("  Could not query time range for {}: {}", stableName, e.getMessage());
            }
        }

        if (globalMin == null || globalMax == null) {
            throw new RuntimeException("Cannot determine time range: no data found in database. " +
                    "Please configure start-time and end-time.");
        }

        if (start == null) start = globalMin;
        if (end == null) end = globalMax.plusNanos(1); // Make end exclusive

        return new LocalDateTime[]{start, end};
    }

    private LocalDateTime[] queryTimeRange(String database, String stableName, TdConnection conn,
                                            String tsColumn) throws Exception {
        LocalDateTime[] result = new LocalDateTime[2];
        String quotedTs = quoteId(tsColumn);
        String sql = "SELECT MIN(" + quotedTs + "), MAX(" + quotedTs + ") FROM " + database + "." + stableName;
        conn.query(sql, rs -> {
            if (rs.next()) {
                Object minObj = rs.getObject(1);
                Object maxObj = rs.getObject(2);
                if (minObj instanceof Timestamp ts) {
                    result[0] = ts.toLocalDateTime();
                } else if (minObj instanceof Long ts) {
                    result[0] = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
                }
                if (maxObj instanceof Timestamp ts) {
                    result[1] = ts.toLocalDateTime();
                } else if (maxObj instanceof Long ts) {
                    result[1] = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.systemDefault());
                }
            }
        });
        return result;
    }

    private List<LocalDate> generateDayList(LocalDateTime start, LocalDateTime end) {
        List<LocalDate> days = new ArrayList<>();
        LocalDate current = start.toLocalDate();
        LocalDate endDay = end.toLocalDate();
        // If end is exactly midnight, the end day has no data (exclusive)
        if (end.toLocalTime().equals(java.time.LocalTime.MIDNIGHT) && !end.toLocalDate().equals(start.toLocalDate())) {
            endDay = endDay.minusDays(1);
        }
        while (!current.isAfter(endDay)) {
            days.add(current);
            current = current.plusDays(1);
        }
        return days;
    }

    // ---- Schema export ----

    private List<String> resolveSuperTables(String database, TdConnection conn) {
        List<String> configured = properties.getSuperTables();
        if (configured != null && !configured.isEmpty()) {
            return configured;
        }
        return conn.getSuperTableNames(database);
    }

    private SchemaFile exportSchema(String database, List<String> stableNames, Path dataDir,
                                     TdConnection conn, ProgressCheckpoint checkpoint) throws Exception {
        Path schemaPath = dataDir.resolve(database + "_schema.json");
        if (Files.exists(schemaPath)) {
            if (checkpoint != null) {
                boolean allDone = stableNames.stream().allMatch(
                        name -> checkpoint.getStables().containsKey(name)
                                && checkpoint.getStables().get(name).isSchemaDone());
                if (allDone) {
                    log.info("Schema already exported, loading existing file");
                    return objectMapper.readValue(schemaPath.toFile(), SchemaFile.class);
                }
            }
        }

        log.info("Exporting schema for database: {}", database);
        SchemaFile schemaFile = new SchemaFile();
        schemaFile.setDatabase(database);
        schemaFile.setExportTime(LocalDateTime.now());

        for (String stableName : stableNames) {
            SuperTableMeta meta = conn.getSuperTableMeta(database, stableName);
            schemaFile.getSuperTables().put(stableName, meta);
            log.info("  Exported schema for super table: {} ({} columns, {} tags)",
                    stableName, meta.getColumns().size(), meta.getTags().size());

            ProgressCheckpoint.StableProgress sp = checkpoint != null ? checkpoint.getOrCreateStable(stableName) : null;
            if (sp != null) {
                sp.setSchemaDone(true);
                checkpointManager.markDirty();
            }
        }

        objectMapper.writeValue(schemaPath.toFile(), schemaFile);
        log.info("Schema file written to: {}", schemaPath);
        checkpointManager.saveIfDirty();
        return schemaFile;
    }

    // ---- Child table manifest export ----

    private void exportChildTableManifest(String database, List<String> stableNames, Path dataDir,
                                           TdConnection conn, SchemaFile schemaFile) throws Exception {
        Path manifestPath = dataDir.resolve(database + "_childtables.json");
        if (Files.exists(manifestPath)) {
            log.info("Child table manifest already exists, skipping: {}", manifestPath);
            return;
        }

        ChildTableManifest manifest = new ChildTableManifest();
        manifest.setDatabase(database);
        manifest.setExportTime(LocalDateTime.now());

        for (String stableName : stableNames) {
            SuperTableMeta meta = schemaFile.getSuperTables().get(stableName);
            if (meta == null || meta.getTags() == null || meta.getTags().isEmpty()) {
                log.debug("  Skipping child table manifest for {} (no tags)", stableName);
                continue;
            }

            // Step 1: Get child table names using fast metadata query
            List<String> childNames = getChildTableNames(database, stableName, conn);
            if (childNames.isEmpty()) {
                log.warn("  No child tables found for {}, skipping manifest", stableName);
                continue;
            }
            log.info("  Found {} child table(s) for {}, querying tag values in batches...",
                    childNames.size(), stableName);

            // Step 2: Batch query tag values using WHERE tbname IN (...) to avoid
            // expensive full-table GROUP BY TBNAME.
            // Batch size kept small (50) to prevent HTTP 413 "Request Entity Too Large"
            // in REST API mode, where the SQL is sent as HTTP POST body.
            List<ChildTableMeta> childTables = new ArrayList<>();
            int batchSize = 50;
            int totalBatches = (childNames.size() + batchSize - 1) / batchSize;

            for (int batchIdx = 0; batchIdx < childNames.size(); batchIdx += batchSize) {
                int batchNum = batchIdx / batchSize + 1;
                int end = Math.min(batchIdx + batchSize, childNames.size());
                List<String> batch = childNames.subList(batchIdx, end);

                StringBuilder sql = new StringBuilder("SELECT TBNAME");
                for (DataColumn tag : meta.getTags()) {
                    sql.append(", `").append(tag.getName()).append("`");
                }
                sql.append(" FROM ").append(database).append(".").append(stableName)
                   .append(" WHERE tbname IN (");
                for (int j = 0; j < batch.size(); j++) {
                    if (j > 0) sql.append(", ");
                    String name = batch.get(j);
                    // Escape single quotes in tbname for SQL safety
                    sql.append("'").append(name.replace("'", "\\'")).append("'");
                }
                sql.append(") GROUP BY TBNAME");

                try {
                    final int currentBatchNum = batchNum;
                    final int currentTotalBatches = totalBatches;
                    conn.query(sql.toString(), rs -> {
                        while (rs.next()) {
                            ChildTableMeta ctm = new ChildTableMeta();
                            ctm.setTbname(rs.getString(1));
                            LinkedHashMap<String, String> tagVals = new LinkedHashMap<>();
                            int colIdx = 2;
                            for (DataColumn tag : meta.getTags()) {
                                Object val = rs.getObject(colIdx++);
                                tagVals.put(tag.getName(), val != null ? val.toString() : null);
                            }
                            ctm.setTagValues(tagVals);
                            synchronized (childTables) {
                                childTables.add(ctm);
                            }
                        }
                    });
                    log.debug("  Batch {}/{} for {}: queried {} child tables",
                            currentBatchNum, currentTotalBatches, stableName, batch.size());
                } catch (Exception e) {
                    log.warn("  Batch {}/{} for {} failed ({}): {}. Falling back to individual queries.",
                            batchNum, totalBatches, stableName,
                            e.getMessage() != null ? e.getMessage().substring(0, Math.min(e.getMessage().length(), 80)) : "unknown",
                            e.getClass().getSimpleName());
                    // Fallback: query each child table individually
                    for (String tbname : batch) {
                        StringBuilder sqlSingle = new StringBuilder("SELECT TBNAME");
                        for (DataColumn tag : meta.getTags()) {
                            sqlSingle.append(", `").append(tag.getName()).append("`");
                        }
                        sqlSingle.append(" FROM ").append(database).append(".").append(stableName)
                                .append(" WHERE tbname = '").append(tbname.replace("'", "\\'")).append("'")
                                .append(" GROUP BY TBNAME");
                        try {
                            conn.query(sqlSingle.toString(), rs -> {
                                while (rs.next()) {
                                    ChildTableMeta ctm = new ChildTableMeta();
                                    ctm.setTbname(rs.getString(1));
                                    LinkedHashMap<String, String> tagVals = new LinkedHashMap<>();
                                    int colIdx = 2;
                                    for (DataColumn tag : meta.getTags()) {
                                        Object val = rs.getObject(colIdx++);
                                        tagVals.put(tag.getName(), val != null ? val.toString() : null);
                                    }
                                    ctm.setTagValues(tagVals);
                                    synchronized (childTables) {
                                        childTables.add(ctm);
                                    }
                                }
                            });
                        } catch (Exception e2) {
                            log.debug("  Could not query tag values for child table '{}': {}",
                                    tbname, e2.getMessage());
                        }
                    }
                }
            }

            if (childTables.isEmpty()) {
                log.warn("  No tag values could be queried for {}, skipping manifest", stableName);
                continue;
            }

            manifest.getSuperTables().put(stableName, childTables);
            log.info("  Exported {} child table(s) for super table {}", childTables.size(), stableName);
        }

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(manifestPath.toFile(), manifest);
        log.info("Child table manifest written to: {} ({} super table(s))",
                manifestPath, manifest.getSuperTables().size());
    }

    // ---- Day-bounded SQL builders ----

    private String buildDayExportSql(String database, String stableName, LocalDate day,
                                      LocalDateTime rangeStart, LocalDateTime rangeEnd,
                                      ProgressCheckpoint.StableProgress sp, String tsColumn) {
        String quotedTs = quoteId(tsColumn);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT tbname, * FROM ").append(database).append(".").append(stableName);

        List<String> conditions = new ArrayList<>();

        // Day-bounded time condition
        LocalDateTime dayStart = day.atStartOfDay();
        LocalDateTime dayEnd = day.plusDays(1).atStartOfDay();
        // Clamp to overall range
        LocalDateTime effectiveStart = dayStart.isBefore(rangeStart) ? rangeStart : dayStart;
        LocalDateTime effectiveEnd = dayEnd.isAfter(rangeEnd) ? rangeEnd : dayEnd;

        if (effectiveStart.isBefore(effectiveEnd)) {
            conditions.add(quotedTs + " >= '" + effectiveStart.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    + "' AND " + quotedTs + " < '" + effectiveEnd.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'");
        }

        // Resume condition (only if resuming within current day)
        if (sp != null && sp.getCurrentDay() != null && sp.getCurrentDay().equals(day.toString())
                && sp.getLastExportTs() != null && !sp.getLastExportTs().isBlank()) {
            conditions.add(quotedTs + " > " + sp.getLastExportTs());
        }

        // Non-time user conditions
        String userConditions = properties.getCombinedConditions(stableName);
        if (!userConditions.isEmpty()) {
            conditions.add(userConditions);
        }

        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        sql.append(" ORDER BY ").append(quotedTs).append(" ASC");
        return sql.toString();
    }

    // ---- Single child table day export (direct child table query, no tbname IN needed) ----

    /**
     * Build SQL for a single child table export.
     * Queries the child table directly with only data columns (no tbname, no tags).
     */
    private String buildSingleChildTableExportSql(String database, String childTable, LocalDate day,
                                                   LocalDateTime rangeStart, LocalDateTime rangeEnd,
                                                   SuperTableMeta meta,
                                                   ProgressCheckpoint.StableProgress sp) {
        String tsColumn = getTimestampColumnName(meta);
        String quotedTs = quoteId(tsColumn);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ");
        boolean first = true;
        for (DataColumn col : meta.getColumns()) {
            if (!first) sql.append(", ");
            sql.append(quoteId(col.getName()));
            first = false;
        }
        sql.append(" FROM ").append(database).append(".").append(childTable);

        List<String> conditions = new ArrayList<>();

        // Day-bounded time condition
        LocalDateTime dayStart = day.atStartOfDay();
        LocalDateTime dayEnd = day.plusDays(1).atStartOfDay();
        LocalDateTime effectiveStart = dayStart.isBefore(rangeStart) ? rangeStart : dayStart;
        LocalDateTime effectiveEnd = dayEnd.isAfter(rangeEnd) ? rangeEnd : dayEnd;

        if (effectiveStart.isBefore(effectiveEnd)) {
            conditions.add(quotedTs + " >= '" + effectiveStart.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                    + "' AND " + quotedTs + " < '" + effectiveEnd.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "'");
        }

        // Resume condition
        if (sp != null && sp.getCurrentDay() != null && sp.getCurrentDay().equals(day.toString())
                && sp.getLastExportTs() != null && !sp.getLastExportTs().isBlank()) {
            conditions.add(quotedTs + " > " + sp.getLastExportTs());
        }

        if (!conditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", conditions));
        }

        sql.append(" ORDER BY ").append(quotedTs).append(" ASC");
        return sql.toString();
    }

    // ---- Single child table data export ----

    private void exportSingleChildTableData(String database, Partition partition, LocalDate day,
                                              LocalDateTime rangeStart, LocalDateTime rangeEnd,
                                              SuperTableMeta meta, Path dataDir,
                                              TdConnection conn) throws Exception {
        String stableName = partition.stableName;
        String childTable = partition.childTable;
        ProgressCheckpoint checkpoint = checkpointManager.getCheckpoint();
        ProgressCheckpoint.StableProgress sp = checkpoint != null ? checkpoint.getOrCreateStable(stableName) : null;

        String sql = buildSingleChildTableExportSql(database, childTable, day, rangeStart, rangeEnd, meta, sp);
        log.info("  [{}][{}][{}] Export query: {}", stableName, childTable, day, sql);

        ResultSet rs = conn.queryDirect(sql);
        ResultSetMetaData rsMeta = rs.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        String[] colLabels = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            colLabels[i] = rsMeta.getColumnLabel(i + 1);
        }

        // Build set of data column names (columns from meta, NOT tags)
        Set<String> dataColumnNames = new HashSet<>();
        for (DataColumn col : meta.getColumns()) {
            dataColumnNames.add(col.getName().toLowerCase());
        }

        String dayDirName = day.format(DATE_DIR_FMT);
        Path dayDir = dataDir.resolve(dayDirName);

        long totalRecords = 0;
        int fileIndex = 0;
        int blockSize = properties.getBlockSize();

        AtomicLong blockRecordCount = new AtomicLong(0);
        long lastProgressTime = System.currentTimeMillis();
        long lastCheckpointTime = System.currentTimeMillis();

        OutputStream currentOutputStream = null;
        BufferedWriter currentWriter = null;
        String currentFileName = null;
        String lastTsValue = null;

        while (rs.next()) {
            if (blockRecordCount.get() == 0 && totalRecords == 0) {
                Files.createDirectories(dayDir);
            }

            if (blockRecordCount.get() == 0) {
                closeWriter(currentWriter, currentOutputStream);

                Object tsObj = rs.getObject(1);
                String tsStr = formatTimestampForFileName(tsObj);
                fileIndex++;

                // File name includes child table name: stable_childTable_timestamp.gz
                currentFileName = stableName + "_" + childTable + "_" + tsStr + ".gz";
                Path filePath = dayDir.resolve(currentFileName);
                if (Files.exists(filePath)) {
                    currentFileName = stableName + "_" + childTable + "_" + tsStr + "_" + fileIndex + ".gz";
                    filePath = dayDir.resolve(currentFileName);
                }

                GzipParameters gzipParams = new GzipParameters();
                gzipParams.setCompressionLevel(6);
                currentOutputStream = new GzipCompressorOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(filePath)), gzipParams);
                currentWriter = new BufferedWriter(
                        new OutputStreamWriter(currentOutputStream, StandardCharsets.UTF_8), 131072);

                if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                    // Header: just data column names (no tbname, no tag_ prefix)
                    StringBuilder header = new StringBuilder();
                    for (int i = 0; i < colLabels.length; i++) {
                        if (i > 0) header.append(FIELD_SEP);
                        header.append(colLabels[i]);
                    }
                    currentWriter.write(header.toString());
                    currentWriter.newLine();
                }

                log.info("  [{}][{}][{}] Creating data file: {}/{} (block #{})",
                        stableName, childTable, day, dayDirName, currentFileName, fileIndex);

                if (sp != null) {
                    sp.setCurrentFile(dayDirName + "/" + currentFileName);
                    sp.setCurrentFileOffset(0);
                    checkpointManager.markDirty();
                }
            }

            if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                writeSimpleCsvLine(currentWriter, rs, columnCount);
            } else {
                writeSimpleJsonLine(currentWriter, rs, columnCount, colLabels);
            }

            Object tsVal = rs.getObject(1);
            if (tsVal != null) {
                lastTsValue = tsVal instanceof Timestamp t
                        ? String.valueOf(t.getTime())
                        : tsVal.toString();
            }

            totalRecords++;
            blockRecordCount.incrementAndGet();

            long now = System.currentTimeMillis();
            if (now - lastProgressTime >= PROGRESS_LOG_INTERVAL_MS) {
                log.info("  [{}][{}][{}] Exported {} records (block #{}: {} records)",
                        stableName, childTable, day, totalRecords, fileIndex, blockRecordCount.get());
                lastProgressTime = now;
            }

            if (now - lastCheckpointTime >= CHECKPOINT_SAVE_INTERVAL_MS && sp != null) {
                sp.setCurrentFileOffset(blockRecordCount.get());
                sp.setTotalRecords(sp.getTotalRecords() + totalRecords);
                sp.setLastExportTs(lastTsValue);
                checkpointManager.saveIfDirty();
                lastCheckpointTime = now;
            }

            if (blockRecordCount.get() >= blockSize) {
                closeWriter(currentWriter, currentOutputStream);
                currentWriter = null;
                currentOutputStream = null;

                if (sp != null && currentFileName != null) {
                    sp.markFileCompleted(dayDirName + "/" + currentFileName, blockRecordCount.get());
                    sp.setTotalRecords(sp.getTotalRecords() + totalRecords);
                    sp.setLastExportTs(lastTsValue);
                    checkpointManager.markDirty();
                }

                log.info("  [{}][{}][{}] Block #{} completed: {} records written to {}/{}",
                        stableName, childTable, day, fileIndex, blockRecordCount.get(), dayDirName, currentFileName);
                blockRecordCount.set(0);
            }
        }

        closeWriter(currentWriter, currentOutputStream);

        if (sp != null && currentFileName != null && blockRecordCount.get() > 0) {
            sp.markFileCompleted(dayDirName + "/" + currentFileName, blockRecordCount.get());
            sp.setTotalRecords(sp.getTotalRecords() + totalRecords);
            sp.setLastExportTs(lastTsValue);
            checkpointManager.markDirty();
        }

        if (totalRecords == 0) {
            log.info("  [{}][{}][{}] No data for this day", stableName, childTable, day);
        } else {
            log.info("  [{}][{}][{}] Day export completed: {} records",
                    stableName, childTable, day, totalRecords);
        }
    }

    // ---- Full stable day export (no child-table partitioning) ----
    // Used when stableNames.size() >= parallelism (no child-table splitting needed).

    private void exportStableDayData(String database, String stableName, LocalDate day,
                                      LocalDateTime rangeStart, LocalDateTime rangeEnd,
                                      SuperTableMeta meta, Path dataDir,
                                      TdConnection conn) throws Exception {
        ProgressCheckpoint checkpoint = checkpointManager.getCheckpoint();
        ProgressCheckpoint.StableProgress sp = checkpoint != null ? checkpoint.getOrCreateStable(stableName) : null;

        String tsColumn = getTimestampColumnName(meta);
        String sql = buildDayExportSql(database, stableName, day, rangeStart, rangeEnd, sp, tsColumn);
        log.info("  [{}][{}] Export query: {}", stableName, day, sql);

        ResultSet rs = conn.queryDirect(sql);
        ResultSetMetaData rsMeta = rs.getMetaData();
        int columnCount = rsMeta.getColumnCount();
        String[] colLabels = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            colLabels[i] = rsMeta.getColumnLabel(i + 1);
        }

        Set<String> tagNames = new HashSet<>();
        for (var tag : meta.getTags()) {
            tagNames.add(tag.getName().toLowerCase());
        }

        // Create day subdirectory
        String dayDirName = day.format(DATE_DIR_FMT);
        Path dayDir = dataDir.resolve(dayDirName);

        long totalRecords = 0;
        int fileIndex = 0;
        int blockSize = properties.getBlockSize();

        AtomicLong blockRecordCount = new AtomicLong(0);
        long lastProgressTime = System.currentTimeMillis();
        long lastCheckpointTime = System.currentTimeMillis();

        OutputStream currentOutputStream = null;
        BufferedWriter currentWriter = null;
        String currentFileName = null;
        String lastTsValue = null;

        while (rs.next()) {
            // Create day directory on first record (avoids empty directories)
            if (blockRecordCount.get() == 0 && totalRecords == 0) {
                Files.createDirectories(dayDir);
            }

            if (blockRecordCount.get() == 0) {
                closeWriter(currentWriter, currentOutputStream);

                Object tsObj = rs.getObject(1);
                String tsStr = formatTimestampForFileName(tsObj);
                fileIndex++;

                currentFileName = stableName + "_" + tsStr + ".gz";
                // Check for file name collision and append suffix if needed
                Path filePath = dayDir.resolve(currentFileName);
                if (Files.exists(filePath)) {
                    currentFileName = stableName + "_" + tsStr + "_" + fileIndex + ".gz";
                    filePath = dayDir.resolve(currentFileName);
                }

                GzipParameters gzipParams = new GzipParameters();
                gzipParams.setCompressionLevel(6);
                currentOutputStream = new GzipCompressorOutputStream(
                        new BufferedOutputStream(Files.newOutputStream(filePath)), gzipParams);
                currentWriter = new BufferedWriter(
                        new OutputStreamWriter(currentOutputStream, StandardCharsets.UTF_8), 131072);

                if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                    StringBuilder header = new StringBuilder();
                    header.append("tbname");
                    for (int i = 1; i < colLabels.length; i++) {
                        String label = colLabels[i];
                        header.append(FIELD_SEP);
                        if (tagNames.contains(label.toLowerCase())) {
                            header.append("tag_").append(label);
                        } else {
                            header.append(label);
                        }
                    }
                    currentWriter.write(header.toString());
                    currentWriter.newLine();
                }

                log.info("  [{}][{}] Creating data file: {}/{} (block #{})",
                        stableName, day, dayDirName, currentFileName, fileIndex);

                // Checkpoint: store relative path (dayDir/fileName)
                if (sp != null) {
                    sp.setCurrentFile(dayDirName + "/" + currentFileName);
                    sp.setCurrentFileOffset(0);
                    checkpointManager.markDirty();
                }
            }

            if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                writeCsvLine(currentWriter, rs, columnCount, colLabels);
            } else {
                writeJsonLine(currentWriter, rs, columnCount, colLabels);
            }

            Object tsVal = rs.getObject(1);
            if (tsVal != null) {
                lastTsValue = tsVal instanceof Timestamp t
                        ? String.valueOf(t.getTime())
                        : tsVal.toString();
            }

            totalRecords++;
            blockRecordCount.incrementAndGet();

            long now = System.currentTimeMillis();
            if (now - lastProgressTime >= PROGRESS_LOG_INTERVAL_MS) {
                log.info("  [{}][{}] Exported {} records (block #{}: {} records)",
                        stableName, day, totalRecords, fileIndex, blockRecordCount.get());
                lastProgressTime = now;
            }

            if (now - lastCheckpointTime >= CHECKPOINT_SAVE_INTERVAL_MS && sp != null) {
                sp.setCurrentFileOffset(blockRecordCount.get());
                sp.setTotalRecords(sp.getTotalRecords() + totalRecords);
                sp.setLastExportTs(lastTsValue);
                checkpointManager.saveIfDirty();
                lastCheckpointTime = now;
            }

            if (blockRecordCount.get() >= blockSize) {
                closeWriter(currentWriter, currentOutputStream);
                currentWriter = null;
                currentOutputStream = null;

                if (sp != null && currentFileName != null) {
                    sp.markFileCompleted(dayDirName + "/" + currentFileName, blockRecordCount.get());
                    sp.setTotalRecords(sp.getTotalRecords() + totalRecords);
                    sp.setLastExportTs(lastTsValue);
                    checkpointManager.markDirty();
                }

                log.info("  [{}][{}] Block #{} completed: {} records written to {}/{}",
                        stableName, day, fileIndex, blockRecordCount.get(), dayDirName, currentFileName);
                blockRecordCount.set(0);
            }
        }

        closeWriter(currentWriter, currentOutputStream);

        if (sp != null && currentFileName != null && blockRecordCount.get() > 0) {
            sp.markFileCompleted(dayDirName + "/" + currentFileName, blockRecordCount.get());
            sp.setTotalRecords(sp.getTotalRecords() + totalRecords);
            sp.setLastExportTs(lastTsValue);
            checkpointManager.markDirty();
        }

        if (totalRecords == 0) {
            log.info("  [{}][{}] No data for this day", stableName, day);
        } else {
            log.info("  [{}][{}] Day export completed: {} records, {} block file(s)",
                    stableName, day, totalRecords, fileIndex);
        }
    }

    // ---- CSV / JSON writers ----

    private void writeCsvLine(BufferedWriter writer, ResultSet rs, int columnCount,
                               String[] colLabels) throws Exception {
        StringBuilder sb = new StringBuilder(256);
        // First column: tbname (from SELECT tbname, *)
        Object tbObj = rs.getObject(1);
        if (tbObj == null) {
            sb.append(NULL_MARKER);
        } else {
            String tbStr = tbObj.toString();
            if (tbStr.contains("\t") || tbStr.contains("\n") || tbStr.contains("\r")) {
                sb.append('"').append(tbStr.replace("\"", "\"\"")).append('"');
            } else {
                sb.append(tbStr);
            }
        }
        for (int i = 2; i <= columnCount; i++) {
            String label = colLabels[i - 1];
            if (sb.length() > 0) sb.append(FIELD_SEP);
            Object value = rs.getObject(i);
            if (value == null) {
                sb.append(NULL_MARKER);
            } else if (value instanceof Timestamp ts) {
                sb.append(ts.getTime());
            } else if (value instanceof Number n) {
                sb.append(n);
            } else {
                String strVal = value.toString();
                if (strVal.contains("\t") || strVal.contains("\n") || strVal.contains("\r")) {
                    sb.append('"').append(strVal.replace("\"", "\"\"")).append('"');
                } else {
                    sb.append(strVal);
                }
            }
        }
        writer.write(sb.toString());
        writer.newLine();
    }

    private void writeJsonLine(BufferedWriter writer, ResultSet rs, int columnCount,
                                String[] colLabels) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        // First field: tbname
        Object tbObj = rs.getObject(1);
        record.put("tbname", tbObj != null ? tbObj.toString() : null);
        // Remaining fields from index 2
        for (int i = 2; i <= columnCount; i++) {
            String label = colLabels[i - 1];
            Object value = rs.getObject(i);
            if (value instanceof Timestamp ts) {
                record.put(label, ts.toInstant().toString());
            } else {
                record.put(label, value);
            }
        }
        writer.write(objectMapper.writeValueAsString(record));
        writer.newLine();
    }

    /**
     * Write a CSV line for a child-table-direct query (no tbname, no tag columns).
     * All columns are data values written in order.
     */
    private void writeSimpleCsvLine(BufferedWriter writer, ResultSet rs,
                                     int columnCount) throws Exception {
        StringBuilder sb = new StringBuilder(256);
        for (int i = 1; i <= columnCount; i++) {
            if (i > 1) sb.append(FIELD_SEP);
            Object value = rs.getObject(i);
            if (value == null) {
                sb.append(NULL_MARKER);
            } else if (value instanceof Timestamp ts) {
                sb.append(ts.getTime());
            } else if (value instanceof Number n) {
                sb.append(n);
            } else {
                String strVal = value.toString();
                if (strVal.contains("\t") || strVal.contains("\n") || strVal.contains("\r")) {
                    sb.append('"').append(strVal.replace("\"", "\"\"")).append('"');
                } else {
                    sb.append(strVal);
                }
            }
        }
        writer.write(sb.toString());
        writer.newLine();
    }

    /**
     * Write a JSON line for a child-table-direct query (no tbname, no tag columns).
     */
    private void writeSimpleJsonLine(BufferedWriter writer, ResultSet rs,
                                      int columnCount, String[] colLabels) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        for (int i = 1; i <= columnCount; i++) {
            String label = colLabels[i - 1];
            Object value = rs.getObject(i);
            if (value instanceof Timestamp ts) {
                record.put(label, ts.toInstant().toString());
            } else {
                record.put(label, value);
            }
        }
        writer.write(objectMapper.writeValueAsString(record));
        writer.newLine();
    }

    private String formatTimestampForFileName(Object tsValue) {
        if (tsValue instanceof Timestamp ts) {
            return TS_FILE_FMT.format(ts.toInstant());
        } else if (tsValue instanceof Long ts) {
            return TS_FILE_FMT.format(Instant.ofEpochMilli(ts));
        } else if (tsValue instanceof LocalDateTime ldt) {
            return ldt.format(DateTimeFormatter.ofPattern("HHmmssSSS"));
        } else {
            return String.valueOf(tsValue).replaceAll("[^a-zA-Z0-9_\\-]", "_");
        }
    }

    /**
     * Find the timestamp column name from SuperTableMeta.
     * Scans columns for TIMESTAMP type, falls back to "ts".
     */
    private String getTimestampColumnName(SuperTableMeta meta) {
        for (DataColumn col : meta.getColumns()) {
            if ("TIMESTAMP".equalsIgnoreCase(col.getType())) {
                return col.getName();
            }
        }
        return "ts"; // fallback default
    }

    /**
     * Quote a TDengine identifier with backticks to avoid reserved keyword issues.
     */
    private static String quoteId(String id) {
        return "`" + id + "`";
    }

    private void closeWriter(Writer writer, OutputStream outputStream) throws IOException {
        if (writer != null) {
            writer.flush();
            writer.close();
        }
        if (outputStream != null) {
            outputStream.close();
        }
    }
}
