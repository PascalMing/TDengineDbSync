package com.tdengine.dbsync.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tdengine.dbsync.config.SyncProperties;
import com.tdengine.dbsync.connection.TdConnection;
import com.tdengine.dbsync.connection.TdConnectionFactory;
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
import java.nio.file.DirectoryStream;
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

public class TdDataExporter implements DataExporter {

    private static final Logger log = LoggerFactory.getLogger(TdDataExporter.class);
    private static final DateTimeFormatter DATE_DIR_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter TS_FILE_FMT = DateTimeFormatter.ofPattern("HHmmssSSS")
            .withZone(ZoneId.systemDefault());
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000;
    private static final long CHECKPOINT_SAVE_INTERVAL_MS = 30_000;
    private static final char FIELD_SEP = '\t';
    private static final String NULL_MARKER = "\\N";
    private static final int MAX_PARTITIONS_PER_STABLE_MULTIPLIER = 4;
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
    }

    @Override
    public void exportData() throws Exception {
        String database = properties.getDatabase();
        log.info("========== Start exporting database: {} ==========", database);
        log.info("  Format: {}, Parallel: {}, File size: {} MB",
                properties.getFormat(), properties.getParallel(), properties.getFileSizeMb());
        long startTime = System.currentTimeMillis();

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

            cleanupLegacyExportFiles(dataDir, stableNames);

            SchemaFile schemaFile = exportSchema(database, stableNames, dataDir, metaConn, checkpoint);

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

            Map<String, Long> childTableCounts = exportChildTableStructureSet(database, stableNames, dataDir, metaConn, schemaFile, rangeStart, rangeEnd);

            // Build time-slice partitions covering the full time range
            int parallelism = Math.min(properties.getParallel(),
                    Runtime.getRuntime().availableProcessors() * 2);
            List<Partition> partitions = buildPartitions(database, stableNames, dataDir, rangeStart, rangeEnd);
            log.info("Created {} partition(s), running with {} parallel thread(s)", partitions.size(), parallelism);

            // Submit all partitions in parallel
            ExecutorService executor = Executors.newFixedThreadPool(parallelism,
                    r -> {
                        Thread t = new Thread(r, "export-partition-worker");
                        t.setDaemon(true);
                        return t;
                    });

            List<Future<Long>> futures = new ArrayList<>();
            for (Partition partition : partitions) {
                futures.add(executor.submit(() -> {
                    try (TdConnection dataConn = connectionFactory.create()) {
                        log.info("  [{}][{}] Starting partition export",
                                partition.stableName, partition.partitionDir.getFileName());
                        long records = exportPartitionData(database, partition,
                                schemaFile.getSuperTables().get(partition.stableName),
                                dataConn);
                        log.info("  [{}][{}] Partition completed",
                                partition.stableName, partition.partitionDir.getFileName());
                        return records;
                    } catch (Exception e) {
                        log.error("  [{}][{}] Partition FAILED: {}",
                                partition.stableName, partition.partitionDir.getFileName(), e.getMessage(), e);
                        throw e;
                    }
                }));
            }

            executor.shutdown();

            long totalRecords = 0;
            List<Exception> errors = new ArrayList<>();
            for (Future<Long> future : futures) {
                try {
                    totalRecords += future.get();
                } catch (ExecutionException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof Exception ex) {
                        errors.add(ex);
                    } else if (cause != null) {
                        errors.add(new RuntimeException(cause));
                    } else {
                        errors.add(new RuntimeException(e));
                    }
                }
            }

            executor.awaitTermination(1, TimeUnit.MINUTES);

            if (!errors.isEmpty()) {
                for (Exception ex : errors) {
                    log.error("Export partition failed: {}", ex.getMessage());
                }
                throw new RuntimeException(errors.size() + " partition(s) failed. First error: "
                        + errors.getFirst().getMessage());
            }

            long elapsedMs = System.currentTimeMillis() - startTime;
            long totalChildTables = childTableCounts.values().stream().mapToLong(Long::longValue).sum();
            log.info("========== Export completed for database: {} ==========", database);
            log.info("  Super tables : {}", stableNames.size());
            log.info("  Child tables : {}", totalChildTables);
            log.info("  Data records : {}", totalRecords);
            log.info("  Total time   : {}", formatDuration(elapsedMs));
        }

        checkpointManager.delete();
    }

    // ---- Partition definition with time window ----

    static class Partition {
        final String stableName;
        final LocalDateTime windowStart;
        final LocalDateTime windowEnd;
        final Path partitionDir;

        Partition(String stableName, LocalDateTime windowStart, LocalDateTime windowEnd, Path partitionDir) {
            this.stableName = stableName;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.partitionDir = partitionDir;
        }
    }

    /**
     * Build time-slice partitions covering the full time range.
     * Each partition covers a fixed window (default 5 minutes) and writes to its own subdirectory,
     * enabling true parallel export across windows.
     */
    private List<Partition> buildPartitions(String database, List<String> stableNames,
                                              Path dataDir,
                                              LocalDateTime rangeStart, LocalDateTime rangeEnd) throws Exception {
        List<Partition> partitions = new ArrayList<>();
        if (stableNames == null || stableNames.isEmpty()) {
            log.warn("No super tables found, no partitions to build");
            return partitions;
        }

        int windowMinutes = Math.max(1, properties.getPartitionWindowMinutes());
        for (String stableName : stableNames) {
            LocalDateTime wStart = rangeStart;
            int index = 0;
            while (wStart.isBefore(rangeEnd)) {
                LocalDateTime wEnd = wStart.plusMinutes(windowMinutes);
                if (wEnd.isAfter(rangeEnd)) wEnd = rangeEnd;

                // Subdirectory per partition: {yyyyMMdd}/slice_{HHmmss}_{index}/
                String dateStr = wStart.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
                String sliceDir = String.format("slice_%s_%02d",
                        wStart.format(DateTimeFormatter.ofPattern("HHmmss")), index);
                Path partitionDir = dataDir.resolve(stableName).resolve(dateStr).resolve(sliceDir);

                partitions.add(new Partition(stableName, wStart, wEnd, partitionDir));
                wStart = wEnd;
                index++;
            }
            log.info("  Super table {} split into {} partition(s) ({} min each)",
                    stableName, index, windowMinutes);
        }
        return partitions;
    }

    /**
     * Export child table structure manifests.
     * @return map of stableName → child table count
     */
    private Map<String, Long> exportChildTableStructureSet(String database, List<String> stableNames, Path dataDir,
                                              TdConnection conn, SchemaFile schemaFile,
                                              LocalDateTime rangeStart, LocalDateTime rangeEnd) throws Exception {
        Path structureDir = dataDir.resolve("structure");
        Files.createDirectories(structureDir);
        Map<String, Long> childTableCounts = new LinkedHashMap<>();

        for (String stableName : stableNames) {
            SuperTableMeta meta = schemaFile.getSuperTables().get(stableName);
            if (meta == null || meta.getTags() == null || meta.getTags().isEmpty()) {
                log.debug("  Skipping structure set for {} (no tags)", stableName);
                continue;
            }

            Path structurePath = structureDir.resolve(stableName + ".jsonl.gz");
            long exportedCount = 0;
            try (OutputStream fos = Files.newOutputStream(structurePath);
                 GzipCompressorOutputStream gzipOut = new GzipCompressorOutputStream(new BufferedOutputStream(fos));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(gzipOut, StandardCharsets.UTF_8), 131072)) {

                String conditions = properties.getCombinedConditions(stableName);

                // Parse conditions into Java-side filter predicates.
                // This avoids slow GROUP BY TBNAME queries that trigger HTTP gateway timeouts.
                List<TagPredicate> tagPredicates = (conditions != null && !conditions.isBlank())
                        ? parseConditionPredicates(conditions) : null;

                // SHOW TABLE TAGS FROM is a fast metadata operation - fetch all at once.
                // LIMIT/OFFSET is not supported on this statement in TDengine.
                String sql = "SHOW TABLE TAGS FROM " + database + "." + stableName;
                log.debug("  Querying child table tags for {}: {}", stableName, sql);

                ResultSet rs = conn.queryDirect(sql);
                try {
                    ResultSetMetaData rsmd = rs.getMetaData();
                    int columnCount = rsmd.getColumnCount();
                    List<String> tagColumnNames = new ArrayList<>();
                    for (int i = 2; i <= columnCount; i++) {
                        tagColumnNames.add(rsmd.getColumnLabel(i));
                    }

                    while (rs.next()) {
                        // Build tag value map for Java-side filtering
                        LinkedHashMap<String, String> tagVals = new LinkedHashMap<>();
                        for (int i = 0; i < tagColumnNames.size(); i++) {
                            Object val = rs.getObject(i + 2);
                            tagVals.put(tagColumnNames.get(i), val != null ? val.toString() : null);
                        }

                        // Apply condition predicates in Java (fast, avoids data scan + GROUP BY)
                        if (tagPredicates != null && !matchAllPredicates(tagPredicates, tagVals)) {
                            continue;
                        }

                        ChildTableMeta ctm = new ChildTableMeta();
                        ctm.setTbname(rs.getString(1));
                        ctm.setTagValues(tagVals);
                        writer.write(objectMapper.writeValueAsString(ctm));
                        writer.newLine();
                        exportedCount++;
                    }
                } finally {
                    rs.close();
                }

                if (exportedCount == 0) {
                    log.warn("  No child tables found for {}, skipping structure set", stableName);
                    childTableCounts.put(stableName, 0L);
                    continue;
                }
                childTableCounts.put(stableName, exportedCount);
            }

            log.info("  Exported structure set for {} ({} child tables) into {}",
                    stableName, exportedCount, structurePath.getFileName());
        }

        log.info("Structure sets written to: {}", structureDir);
        return childTableCounts;
    }

    private void cleanupLegacyExportFiles(Path dataDir, List<String> stableNames) throws IOException {
        if (stableNames == null || stableNames.isEmpty()) {
            return;
        }

        long deletedCount = 0;
        long deletedSliceDirs = 0;
        for (String stableName : stableNames) {
            // 1. Clean old flat files (legacy format)
            deletedCount += deleteLegacyFilesInDirectory(dataDir, stableName, false);

            // 2. Clean old date subdirectories (legacy format)
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
                for (Path entry : stream) {
                    if (!Files.isDirectory(entry)) {
                        continue;
                    }
                    String dirName = entry.getFileName().toString();
                    if (dirName.matches("\\d{8}")) {
                        deletedCount += deleteLegacyFilesInDirectory(entry, stableName, true);
                    }
                }
            }

            // 3. Clean up date/slice_* partition directories from previous runs
            Path stableDir = dataDir.resolve(stableName);
            if (Files.isDirectory(stableDir)) {
                try (DirectoryStream<Path> dateStream = Files.newDirectoryStream(stableDir, entry ->
                        Files.isDirectory(entry) && entry.getFileName().toString().matches("\\d{8}"))) {
                    for (Path dateDir : dateStream) {
                        try (DirectoryStream<Path> sliceStream = Files.newDirectoryStream(dateDir, entry ->
                                Files.isDirectory(entry) && entry.getFileName().toString().startsWith("slice_"))) {
                            for (Path sliceDir : sliceStream) {
                                deleteDirectoryRecursively(sliceDir);
                                deletedSliceDirs++;
                            }
                        }
                        // Clean up empty date directories
                        try (DirectoryStream<Path> remaining = Files.newDirectoryStream(dateDir)) {
                            if (!remaining.iterator().hasNext()) {
                                Files.deleteIfExists(dateDir);
                            }
                        }
                    }
                }
            }
        }

        if (deletedCount > 0) {
            log.info("Removed {} legacy export file(s) from {}", deletedCount, dataDir);
        }
        if (deletedSliceDirs > 0) {
            log.info("Cleaned {} partition subdirectories from previous export runs", deletedSliceDirs);
        }
    }

    /**
     * Recursively delete a directory and all its contents.
     */
    private void deleteDirectoryRecursively(Path dir) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    deleteDirectoryRecursively(entry);
                } else {
                    Files.deleteIfExists(entry);
                }
            }
        }
        Files.deleteIfExists(dir);
    }

    private long deleteLegacyFilesInDirectory(Path directory, String stableName, boolean dayDirectory) throws IOException {
        long deletedCount = 0;
        String filePattern = dayDirectory ? stableName + "_t*.gz" : stableName + "_*.gz";

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, filePattern)) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                if (fileName.startsWith(stableName + "_P")) {
                    continue;
                }
                Files.deleteIfExists(entry);
                deletedCount++;
                log.debug("Deleted legacy export file: {}", entry);
            }
        }

        return deletedCount;
    }

    private List<String> getChildTableNames(String database, String stableName, TdConnection conn) {
        return getChildTableNames(database, stableName, conn, null);
    }

    /**
     * Get child table names for a super table, optionally filtered by conditions.
     * When {@code conditions} is non-empty, uses {@code WHERE {conditions} GROUP BY TBNAME}
     * to return only matching child tables (skips SHOW TABLE TAGS FROM as it cannot filter by tag values).
     * When no conditions, uses {@code SHOW TABLE TAGS FROM} for direct child table listing, falling back
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

        // No conditions: use SHOW TABLE TAGS FROM for direct child table listing (TDengine 3.x+)
        try {
            conn.query("SHOW TABLE TAGS FROM " + database + "." + stableName, rs -> {
                while (rs.next()) {
                    String tbname = rs.getString(1);
                    if (tbname != null && !tbname.isBlank()) {
                        names.add(tbname);
                    }
                }
            });
        } catch (Exception e) {
            log.debug("  SHOW TABLE TAGS FROM failed for {}: {}", stableName, e.getMessage());
        }
        // Fallback to GROUP BY TBNAME if SHOW TABLE TAGS FROM returned no results
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

    // ---- Partition data export (time-slice pagination) ----

    private long exportPartitionData(String database, Partition partition,
                                     SuperTableMeta meta, TdConnection conn) throws Exception {
        ProgressCheckpoint checkpoint = checkpointManager.getCheckpoint();
        ProgressCheckpoint.StableProgress sp = checkpoint != null
                ? checkpoint.getOrCreateStable(partition.stableName) : null;

        String tsColumn = getTimestampColumnName(meta);
        String quotedTs = quoteId(tsColumn);

        // Non-time user conditions
        String userConditions = properties.getCombinedConditions(partition.stableName);
        DateTimeFormatter tsFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        int sliceSeconds = properties.getExportSliceSeconds();
        if (sliceSeconds <= 0) {
            sliceSeconds = 60;  // sensible default
        }

        // Build base SQL without time conditions (added per-slice below)
        String baseSql = "SELECT tbname, * FROM " + database + "." + partition.stableName
                + " WHERE 1=1";
        if (!userConditions.isEmpty()) {
            baseSql += " AND (" + userConditions + ")";
        }

        LocalDateTime sliceStart = partition.windowStart;

        boolean firstPage = true;
        int columnCount = 0;
        String[] colLabels = null;

        long totalRecords = 0;
        long maxFileBytes = Math.max(1, properties.getFileSizeMb()) * 1024L * 1024L;
        int fileIndex = 0;

        BufferedWriter writer = null;
        OutputStream outputStream = null;
        long currentFileBytes = 0;
        long currentFileRecords = 0;
        String currentFileName = null;
        String currentTsSuffix = null;

        long lastProgressTime = System.currentTimeMillis();
        long lastCheckpointTime = System.currentTimeMillis();
        String lastTsValue = null;

        Path partDir = partition.partitionDir;

        // Time-slice loop: each query covers [sliceStart, sliceEnd)
        while (sliceStart.isBefore(partition.windowEnd)) {
            LocalDateTime sliceEnd = sliceStart.plusSeconds(sliceSeconds);
            if (sliceEnd.isAfter(partition.windowEnd)) {
                sliceEnd = partition.windowEnd;
            }
            String querySql = baseSql
                    + " AND " + quotedTs + " >= '" + sliceStart.format(tsFmt) + "'"
                    + " AND " + quotedTs + " < '" + sliceEnd.format(tsFmt) + "'";
            log.debug("  [{}] Time slice [{}, {}): {}", partition.stableName,
                    sliceStart.format(tsFmt), sliceEnd.format(tsFmt), querySql);
            sliceStart = sliceEnd;

            ResultSet rs = conn.queryDirect(querySql);

            if (firstPage) {
                ResultSetMetaData rsMeta = rs.getMetaData();
                columnCount = rsMeta.getColumnCount();
                colLabels = new String[columnCount];
                for (int i = 0; i < columnCount; i++) {
                    colLabels[i] = rsMeta.getColumnLabel(i + 1);
                }
                firstPage = false;
            }

            while (rs.next()) {
                if (writer == null) {
                    Files.createDirectories(partDir);
                    Object tsObj = rs.getObject(2);
                    currentTsSuffix = formatTimestampForFileName(tsObj);
                    fileIndex++;
                    currentFileName = partition.stableName + "_" + currentTsSuffix + (fileIndex > 1 ? "_" + fileIndex : "") + ".gz";
                    Path filePath = partDir.resolve(currentFileName);
                    if (Files.exists(filePath)) {
                        currentFileName = partition.stableName + "_" + currentTsSuffix + "_" + fileIndex + ".gz";
                        filePath = partDir.resolve(currentFileName);
                    }
                    GzipParameters gzipParams = new GzipParameters();
                    gzipParams.setCompressionLevel(6);
                    outputStream = new GzipCompressorOutputStream(
                            new BufferedOutputStream(Files.newOutputStream(filePath)), gzipParams);
                    writer = new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8), 131072);
                    if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        StringBuilder header = new StringBuilder();
                        header.append("tbname");
                        for (int i = 2; i <= columnCount; i++) {
                            String label = colLabels[i - 1];
                            if (isTagColumn(meta, label)) {
                                continue;
                            }
                            header.append(FIELD_SEP).append(label);
                        }
                        writer.write(header.toString());
                        writer.newLine();
                        currentFileBytes += header.toString().getBytes(StandardCharsets.UTF_8).length + 1L;
                    }
                    log.info("  [{}][{}] Creating data file: {}", partition.stableName,
                            partDir.getFileName(), currentFileName);
                }

                Object tbObj = rs.getObject(1);
                Object tsObj = rs.getObject(2);
                int writtenBytes = properties.getFormat() == SyncProperties.DataFormat.CSV
                        ? writeDataOnlyCsvLine(writer, rs, columnCount, colLabels, meta)
                        : writeDataOnlyJsonLine(writer, rs, columnCount, colLabels, meta);

                Object tsVal = tsObj;
                if (tsVal != null) {
                    lastTsValue = tsVal instanceof Timestamp t
                            ? String.valueOf(t.getTime() * 1_000_000L + t.getNanos() % 1_000_000)
                            : tsVal.toString();
                }

                totalRecords++;
                currentFileRecords++;
                currentFileBytes += writtenBytes;

                long now = System.currentTimeMillis();
                if (now - lastProgressTime >= PROGRESS_LOG_INTERVAL_MS) {
                    log.info("  [{}][{}] Exported {} records", partition.stableName,
                            partDir.getFileName(), totalRecords);
                    lastProgressTime = now;
                }

                if (now - lastCheckpointTime >= CHECKPOINT_SAVE_INTERVAL_MS && sp != null) {
                    sp.setLastExportTs(lastTsValue);
                    checkpointManager.saveIfDirty();
                    lastCheckpointTime = now;
                }

                if (currentFileBytes >= maxFileBytes) {
                    closeWriter(writer, outputStream);
                    if (sp != null && currentFileName != null) {
                        String fileKey = partDir.getFileName() + "/" + currentFileName;
                        sp.markFileCompleted(fileKey, currentFileRecords);
                        sp.addTotalRecords(currentFileRecords);
                        sp.setLastExportTs(lastTsValue);
                        checkpointManager.markDirty();
                    }
                    log.info("  [{}][{}] File completed: {} records written to {} ({} bytes)",
                            partition.stableName, partDir.getFileName(), currentFileRecords,
                            currentFileName, currentFileBytes);
                    writer = null;
                    outputStream = null;
                    currentFileBytes = 0;
                    currentFileRecords = 0;
                }
            }

            rs.close();
        }

        if (writer != null) {
            closeWriter(writer, outputStream);
            if (sp != null && currentFileName != null && currentFileRecords > 0) {
                String fileKey = partDir.getFileName() + "/" + currentFileName;
                sp.markFileCompleted(fileKey, currentFileRecords);
                sp.addTotalRecords(currentFileRecords);
                sp.setLastExportTs(lastTsValue);
                checkpointManager.markDirty();
            }
        }

        if (totalRecords > 0) {
            log.info("  [{}][{}] Partition completed: {} records",
                    partition.stableName, partDir.getFileName(), totalRecords);
        }
        return totalRecords;
    }

    private boolean isTagColumn(SuperTableMeta meta, String columnName) {
        if (meta == null || meta.getTags() == null || columnName == null) {
            return false;
        }
        for (DataColumn tag : meta.getTags()) {
            if (tag.getName().equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }

    // ---- CSV / JSON writers ----

    private int writeDataOnlyCsvLine(BufferedWriter writer, ResultSet rs, int columnCount,
                                      String[] colLabels, SuperTableMeta meta) throws Exception {
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
            if (isTagColumn(meta, label)) {
                continue;
            }
            if (sb.length() > 0) sb.append(FIELD_SEP);
            Object value = rs.getObject(i);
            if (value == null) {
                sb.append(NULL_MARKER);
            } else if (value instanceof Timestamp ts) {
                sb.append(formatTimestampWithNanos(ts));
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
        String line = sb.toString();
        writer.write(line);
        writer.newLine();
        return line.getBytes(StandardCharsets.UTF_8).length + 1;
    }

    private int writeDataOnlyJsonLine(BufferedWriter writer, ResultSet rs, int columnCount,
                                       String[] colLabels, SuperTableMeta meta) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        // First field: tbname
        Object tbObj = rs.getObject(1);
        record.put("tbname", tbObj != null ? tbObj.toString() : null);
        // Remaining fields from index 2
        for (int i = 2; i <= columnCount; i++) {
            String label = colLabels[i - 1];
            if (isTagColumn(meta, label)) {
                continue;
            }
            Object value = rs.getObject(i);
            if (value instanceof Timestamp ts) {
                record.put(label, formatTimestampWithNanos(ts));
            } else {
                record.put(label, value);
            }
        }
        String line = objectMapper.writeValueAsString(record);
        writer.write(line);
        writer.newLine();
        return line.getBytes(StandardCharsets.UTF_8).length + 1;
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
                sb.append(formatTimestampWithNanos(ts));
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
                record.put(label, formatTimestampWithNanos(ts));
            } else {
                record.put(label, value);
            }
        }
        writer.write(objectMapper.writeValueAsString(record));
        writer.newLine();
    }

    /**
     * Format a Timestamp to ISO-8601 string with nanosecond precision.
     * Unlike Instant.toString() which only keeps milliseconds, this preserves full nanosecond precision.
     * Example: 2026-05-05T16:00:00.162000001Z
     */
    private String formatTimestampWithNanos(Timestamp ts) {
        LocalDateTime ldt = ts.toLocalDateTime();
        int nanos = ldt.getNano();
        if (nanos == 0) {
            // No fractional seconds
            return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'"));
        } else if (nanos % 1_000_000 == 0) {
            // Only milliseconds precision needed
            return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
        } else if (nanos % 1_000 == 0) {
            // Microseconds precision needed
            return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'"));
        } else {
            // Full nanoseconds precision
            return ldt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'"));
        }
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

    // ---- Condition predicate parsing for Java-side tag filtering ----

    @FunctionalInterface
    private interface TagPredicate {
        boolean matches(Map<String, String> tagValues);
    }

    static List<TagPredicate> parseConditionPredicates(String condition) {
        if (condition == null || condition.isBlank()) {
            return Collections.emptyList();
        }
        List<TagPredicate> predicates = new ArrayList<>();
        String[] parts = condition.split("(?i)\\s+AND\\s+");
        for (String part : parts) {
            part = part.trim();
            if (part.isEmpty()) continue;
            TagPredicate p = parseSinglePredicate(part);
            if (p == null) {
                log.warn("  Cannot parse condition fragment '{}', no filter applied", part);
                return Collections.emptyList();
            }
            predicates.add(p);
        }
        return predicates;
    }

    private static TagPredicate parseSinglePredicate(String expr) {
        java.util.regex.Matcher inMatcher = java.util.regex.Pattern.compile(
                "^(\\w+)\\s+IN\\s*\\((.+)\\)$",
                java.util.regex.Pattern.CASE_INSENSITIVE
        ).matcher(expr.trim());
        if (inMatcher.find()) {
            String tagName = inMatcher.group(1).toLowerCase();
            String valuesStr = inMatcher.group(2);
            Set<String> acceptedValues = new HashSet<>();
            java.util.regex.Matcher valMatcher = java.util.regex.Pattern.compile("'([^']*)'").matcher(valuesStr);
            while (valMatcher.find()) {
                acceptedValues.add(valMatcher.group(1));
            }
            if (!acceptedValues.isEmpty()) {
                return tagValues -> {
                    String v = getTagValueCI(tagValues, tagName);
                    if (v == null) return false;
                    return acceptedValues.contains(v);
                };
            }
        }

        java.util.regex.Matcher eqMatcher = java.util.regex.Pattern.compile(
                "^(\\w+)\\s*=\\s*'([^']*)'$"
        ).matcher(expr.trim());
        if (eqMatcher.find()) {
            String tagName = eqMatcher.group(1).toLowerCase();
            String expectedValue = eqMatcher.group(2);
            return tagValues -> {
                String v = getTagValueCI(tagValues, tagName);
                if (v == null) return false;
                return expectedValue.equals(v);
            };
        }

        java.util.regex.Matcher neqMatcher = java.util.regex.Pattern.compile(
                "^(\\w+)\\s*!=\\s*'([^']*)'$"
        ).matcher(expr.trim());
        if (neqMatcher.find()) {
            String tagName = neqMatcher.group(1).toLowerCase();
            String excludedValue = neqMatcher.group(2);
            return tagValues -> {
                String v = getTagValueCI(tagValues, tagName);
                if (v == null) return false;
                return !excludedValue.equals(v);
            };
        }

        return null;
    }

    static boolean matchAllPredicates(List<TagPredicate> predicates, Map<String, String> tagValues) {
        if (predicates == null || predicates.isEmpty()) return true;
        for (TagPredicate p : predicates) {
            if (!p.matches(tagValues)) return false;
        }
        return true;
    }

    private static String getTagValueCI(Map<String, String> tagValues, String lowerKey) {
        String v = tagValues.get(lowerKey);
        if (v != null) return v;
        for (Map.Entry<String, String> e : tagValues.entrySet()) {
            if (e.getKey().equalsIgnoreCase(lowerKey)) return e.getValue();
        }
        return null;
    }

    /** Format milliseconds as HH:mm:ss for display. */
    static String formatDuration(long millis) {
        long seconds = millis / 1000;
        long h = seconds / 3600;
        long m = (seconds % 3600) / 60;
        long s = seconds % 60;
        if (h > 0) {
            return String.format("%dh %02dm %02ds", h, m, s);
        } else if (m > 0) {
            return String.format("%dm %02ds", m, s);
        } else {
            return String.format("%ds", s);
        }
    }
}
