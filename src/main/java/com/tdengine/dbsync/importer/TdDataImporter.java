package com.tdengine.dbsync.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TdDataImporter implements DataImporter {

    private static final Logger log = LoggerFactory.getLogger(TdDataImporter.class);
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000;
    private static final long CHECKPOINT_SAVE_INTERVAL_MS = 30_000;
    private static final char FIELD_SEP = '\t';
    private static final String NULL_MARKER = "\\N";

    private final SyncProperties properties;
    private final TdConnectionFactory connectionFactory;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper objectMapper;
    private final Set<String> createdChildTables = ConcurrentHashMap.newKeySet();
    private boolean hasChildTableManifest = false;

    public TdDataImporter(SyncProperties properties, TdConnectionFactory connectionFactory,
                          CheckpointManager checkpointManager) {
        this.properties = properties;
        this.connectionFactory = connectionFactory;
        this.checkpointManager = checkpointManager;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public void importData() throws Exception {
        String sourceDb = properties.getDatabase();
        String targetDb = properties.getTargetDatabase();
        Path dataDir = Path.of(properties.getDataDir(), sourceDb);

        if (!Files.exists(dataDir)) {
            throw new RuntimeException("Data directory not found: " + dataDir);
        }

        log.info("========== Start importing into database: {} ==========", targetDb);
        log.info("  Source data directory: {} ({})", dataDir, sourceDb);
        log.info("  Format: {}, Batch size: {}, Pipeline queue: {}, Writer threads: {}",
                properties.getFormat(), properties.getBatchSize(),
                properties.getPipelineQueueSize(), properties.getParallel());

        ProgressCheckpoint checkpoint = checkpointManager.getCheckpoint();
        if (checkpoint != null && !checkpoint.getStables().isEmpty()) {
            log.info("Resuming from previous checkpoint...");
        }

        Path schemaPath = dataDir.resolve(sourceDb + "_schema.json");
        if (!Files.exists(schemaPath)) {
            throw new RuntimeException("Schema file not found: " + schemaPath);
        }

        SchemaFile schemaFile = objectMapper.readValue(schemaPath.toFile(), SchemaFile.class);
        log.info("Loaded schema file, exported at: {}, contains {} super table(s)",
                schemaFile.getExportTime(), schemaFile.getSuperTables().size());

        // Use a shared connection for metadata operations
        try (TdConnection metaConn = connectionFactory.create()) {
            metaConn.execute("CREATE DATABASE IF NOT EXISTS " + targetDb);
            metaConn.execute("USE " + targetDb);

            validateAndCreateSchema(targetDb, schemaFile, metaConn);
            loadAndCreateChildTables(sourceDb, targetDb, schemaFile, metaConn);
        }

        List<String> targetTables = resolveSuperTables(schemaFile);
        for (String stableName : targetTables) {
            importSuperTableData(targetDb, stableName, schemaFile.getSuperTables().get(stableName), dataDir, checkpoint);
        }

        checkpointManager.delete();
        log.info("========== Import completed into database: {} ==========", targetDb);
    }

    private List<String> resolveSuperTables(SchemaFile schemaFile) {
        List<String> configured = properties.getSuperTables();
        if (configured != null && !configured.isEmpty()) {
            return configured;
        }
        return new ArrayList<>(schemaFile.getSuperTables().keySet());
    }

    private void preloadExistingChildTables(String database, SchemaFile schemaFile, TdConnection conn) {
        log.info("Pre-loading existing child tables from target database...");
        int totalExisting = 0;

        for (String stableName : schemaFile.getSuperTables().keySet()) {
            try {
                List<String> childTables = new ArrayList<>();
                conn.query("SELECT TBNAME FROM " + database + "." + stableName + " GROUP BY TBNAME", rs -> {
                    while (rs.next()) {
                        String tbname = rs.getString(1);
                        if (tbname != null) {
                            childTables.add(tbname);
                        }
                    }
                });
                for (String ct : childTables) {
                    createdChildTables.add(stableName + ":" + ct);
                }
                totalExisting += childTables.size();
                log.info("  Found {} existing child table(s) for super table {}", childTables.size(), stableName);
            } catch (Exception e) {
                log.debug("  No existing child tables for super table {}", stableName);
            }
        }

        log.info("Pre-loaded {} existing child tables total", totalExisting);
    }

    private void loadAndCreateChildTables(String sourceDb, String targetDb, SchemaFile schemaFile, TdConnection conn) {
        Path manifestPath = Path.of(properties.getDataDir(), sourceDb, sourceDb + "_childtables.json");

        if (!Files.exists(manifestPath)) {
            log.info("No child table manifest found ({}), falling back to tag-based generation", manifestPath);
            preloadExistingChildTables(targetDb, schemaFile, conn);
            return;
        }

        try {
            ChildTableManifest manifest = objectMapper.readValue(manifestPath.toFile(), ChildTableManifest.class);
            int totalCreated = 0;

            for (Map.Entry<String, List<ChildTableMeta>> entry : manifest.getSuperTables().entrySet()) {
                String stableName = entry.getKey();
                List<ChildTableMeta> children = entry.getValue();
                int createdCount = 0;

                for (ChildTableMeta child : children) {
                    String tbname = child.getTbname();
                    if (tbname == null || tbname.isBlank()) continue;

                    LinkedHashMap<String, String> tagVals = child.getTagValues();

                    // Build: CREATE TABLE IF NOT EXISTS `tbname` USING `stableName` TAGS(v1, v2, ...)
                    StringBuilder ddl = new StringBuilder();
                    ddl.append("CREATE TABLE IF NOT EXISTS `").append(tbname)
                       .append("` USING `").append(stableName).append("` TAGS (");

                    boolean first = true;
                    for (String tagVal : tagVals.values()) {
                        if (!first) ddl.append(", ");
                        ddl.append(formatSqlValue(tagVal));
                        first = false;
                    }
                    ddl.append(")");

                    conn.execute(ddl.toString());
                    createdChildTables.add(stableName + ":" + tbname);
                    createdCount++;
                }

                totalCreated += createdCount;
                log.info("  Created/verified {} child table(s) for super table {}", createdCount, stableName);
            }

            hasChildTableManifest = true;
            log.info("Child table manifest loaded: {} child table(s) across {} super table(s) created/verified",
                    totalCreated, manifest.getSuperTables().size());
        } catch (Exception e) {
            log.warn("Failed to load child table manifest '{}', falling back: {}",
                    manifestPath, e.getMessage());
            preloadExistingChildTables(targetDb, schemaFile, conn);
        }
    }

    private void validateAndCreateSchema(String database, SchemaFile schemaFile, TdConnection conn) throws Exception {
        log.info("Validating schema consistency...");

        for (Map.Entry<String, SuperTableMeta> entry : schemaFile.getSuperTables().entrySet()) {
            String stableName = entry.getKey();
            SuperTableMeta sourceMeta = entry.getValue();

            List<String> existingStables = conn.getSuperTableNames(database);
            if (existingStables.contains(stableName)) {
                SuperTableMeta targetMeta = conn.getSuperTableMeta(database, stableName);
                validateConsistency(stableName, sourceMeta, targetMeta);
            } else {
                log.info("  Creating super table: {}", stableName);
                String createStmt = sourceMeta.getCreateStmt();
                if (createStmt != null && !createStmt.isBlank()) {
                    conn.execute(sanitizeCreateStable(createStmt));
                    log.info("  Super table created: {}", stableName);
                } else {
                    throw new RuntimeException("Cannot create super table " + stableName
                            + ": CREATE statement is missing in schema file");
                }
            }
        }

        log.info("Schema validation passed");
    }

    private void validateConsistency(String stableName, SuperTableMeta source, SuperTableMeta target) {
        List<String> inconsistencies = new ArrayList<>();
        compareColumns("column", source.getColumns(), target.getColumns(), inconsistencies);
        compareColumns("tag", source.getTags(), target.getTags(), inconsistencies);

        if (!inconsistencies.isEmpty()) {
            log.error("Schema inconsistency detected for super table: {}", stableName);
            for (String issue : inconsistencies) {
                log.error("  - {}", issue);
            }
            throw new RuntimeException("Schema inconsistency for super table '" + stableName
                    + "'. Import aborted. Details:\n" + String.join("\n", inconsistencies));
        }

        log.info("  Super table {} schema is consistent", stableName);
    }

    private void compareColumns(String type, List<DataColumn> source, List<DataColumn> target,
                                 List<String> inconsistencies) {
        Map<String, DataColumn> targetMap = new LinkedHashMap<>();
        for (DataColumn col : target) {
            targetMap.put(col.getName().toLowerCase(), col);
        }

        for (DataColumn srcCol : source) {
            DataColumn tgtCol = targetMap.get(srcCol.getName().toLowerCase());
            if (tgtCol == null) {
                inconsistencies.add(String.format("Missing %s in target: %s", type, srcCol.getName()));
            } else if (!srcCol.getType().equalsIgnoreCase(tgtCol.getType())) {
                inconsistencies.add(String.format("%s type mismatch for '%s': source=%s, target=%s",
                        type, srcCol.getName(), srcCol.getType(), tgtCol.getType()));
            } else if (srcCol.getLength() != tgtCol.getLength() && srcCol.getLength() > 0 && tgtCol.getLength() > 0) {
                inconsistencies.add(String.format("%s length mismatch for '%s': source=%d, target=%d",
                        type, srcCol.getName(), srcCol.getLength(), tgtCol.getLength()));
            }
        }

        Map<String, DataColumn> sourceMap = new LinkedHashMap<>();
        for (DataColumn col : source) {
            sourceMap.put(col.getName().toLowerCase(), col);
        }
        for (DataColumn tgtCol : target) {
            if (!sourceMap.containsKey(tgtCol.getName().toLowerCase())) {
                log.warn("  Extra {} in target super table: {} (type={}) - will be NULL for imported data",
                        type, tgtCol.getName(), tgtCol.getType());
            }
        }
    }

    /**
     * Import data for a super table using multi-writer pipeline.
     * - Producer: reads .gz files, parses lines, accumulates batches
     * - Multiple consumers: each has its own DB connection, takes batches and INSERTs
     */
    private void importSuperTableData(String database, String stableName,
                                       SuperTableMeta meta, Path dataDir,
                                       ProgressCheckpoint checkpoint) throws Exception {
        ProgressCheckpoint.StableProgress sp = checkpoint != null ? checkpoint.getOrCreateStable(stableName) : null;

        log.info("Importing data for super table: {}", stableName);

        List<Path> dataFiles = findDataFiles(stableName, dataDir);
        if (dataFiles.isEmpty()) {
            log.warn("  No data files found for super table: {}", stableName);
            return;
        }

        log.info("  Found {} data file(s) for super table: {}", dataFiles.size(), stableName);

        int batchSize = properties.getBatchSize();
        int queueSize = properties.getPipelineQueueSize();
        int writerThreads = properties.getParallel();

        BlockingQueue<List<String>> batchQueue = new LinkedBlockingQueue<>(queueSize);
        List<String> POISON = List.of("__POISON__");

        AtomicBoolean error = new AtomicBoolean(false);
        AtomicLong totalRecords = new AtomicLong(sp != null ? sp.getTotalRecords() : 0);
        AtomicLong lastProgressTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong lastCheckpointTime = new AtomicLong(System.currentTimeMillis());

        Set<String> tagColumnNames = new HashSet<>();
        for (DataColumn tag : meta.getTags()) {
            tagColumnNames.add(tag.getName().toLowerCase());
        }

        // Start multiple consumer threads, each with its own connection
        int consumers = Math.min(writerThreads, Runtime.getRuntime().availableProcessors());
        List<Thread> consumerThreads = new ArrayList<>();

        for (int c = 0; c < consumers; c++) {
            final int consumerId = c;
            Thread consumer = Thread.startVirtualThread(() -> {
                try (TdConnection writeConn = connectionFactory.create()) {
                    writeConn.execute("USE " + database);

                    while (true) {
                        List<String> batch = batchQueue.take();
                        if (batch == POISON) break;
                        if (error.get()) break;

                        try {
                            int inserted = insertBatch(stableName, tagColumnNames, batch, writeConn);
                            totalRecords.addAndGet(inserted);

                            long now = System.currentTimeMillis();
                            if (now - lastProgressTime.get() >= PROGRESS_LOG_INTERVAL_MS) {
                                log.info("  [{}] Imported {} records (writer-{})", stableName, totalRecords.get(), consumerId);
                                lastProgressTime.set(now);
                            }

                            if (now - lastCheckpointTime.get() >= CHECKPOINT_SAVE_INTERVAL_MS && sp != null) {
                                sp.setTotalRecords(totalRecords.get());
                                checkpointManager.saveIfDirty();
                                lastCheckpointTime.set(now);
                            }
                        } catch (Exception e) {
                            log.error("  [{}] Batch insert failed (writer-{}): {}", stableName, consumerId, e.getMessage());
                            error.set(true);
                            break;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.error("  [{}] Writer connection error (writer-{}): {}", stableName, consumerId, e.getMessage());
                    error.set(true);
                }
            });
            consumerThreads.add(consumer);
        }

        log.info("  Started {} writer thread(s) for super table {}", consumers, stableName);

        // Producer: read files, parse lines, put batches into queue
        try {
            for (Path dataFile : dataFiles) {
                if (error.get()) break;

                String fileKey = buildFileKey(dataFile, dataDir);

                if (sp != null && sp.isFileCompleted(fileKey)) {
                    long completedRecords = sp.getCompletedFiles().get(fileKey);
                    totalRecords.addAndGet(completedRecords);
                    log.info("  [{}] Skipping already imported file: {} ({} records)",
                            stableName, fileKey, completedRecords);
                    continue;
                }

                log.info("  [{}] Processing file: {}", stableName, fileKey);

                if (sp != null) {
                    sp.setCurrentFile(fileKey);
                    sp.setCurrentFileOffset(0);
                    checkpointManager.markDirty();
                }

                long fileRecordCount = 0;

                try (InputStream fis = Files.newInputStream(dataFile);
                     GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(fis);
                     BufferedReader reader = new BufferedReader(
                             new InputStreamReader(gzipIn, StandardCharsets.UTF_8), 131072) {

                         // Override to track line count
                     }) {

                    // Read header line and parse tag column positions
                    String headerLine = reader.readLine();
                    if (headerLine != null && properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        // Cache header for batch processing
                        cachedHeader = parseCsvLine(headerLine);
                        // Detect if CSV includes tbname (native JDBC mode) or not (REST API mode)
                        hasTbname = cachedHeader.length > 0 && "tbname".equals(cachedHeader[0]);
                        tagColumnIndices = new HashSet<>();
                        for (int i = 0; i < cachedHeader.length; i++) {
                            if (cachedHeader[i].startsWith("tag_")) {
                                tagColumnIndices.add(i);
                            }
                        }
                    }

                    // Resume partial file
                    long fileOffset = (sp != null && fileKey.equals(sp.getCurrentFile()))
                            ? sp.getCurrentFileOffset() : 0;
                    if (fileOffset > 0) {
                        for (long i = 0; i < fileOffset; i++) {
                            if (reader.readLine() == null) break;
                        }
                        fileRecordCount = fileOffset;
                        log.info("  [{}] Resumed file {} from line {}", stableName, fileKey, fileOffset);
                    }

                    List<String> batchLines = new ArrayList<>(batchSize);
                    String line;

                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;
                        if (error.get()) break;

                        batchLines.add(line);
                        fileRecordCount++;

                        if (batchLines.size() >= batchSize) {
                            batchQueue.put(new ArrayList<>(batchLines));
                            batchLines.clear();
                        }
                    }

                    if (!batchLines.isEmpty()) {
                        batchQueue.put(new ArrayList<>(batchLines));
                    }
                }

                if (sp != null && !error.get()) {
                    sp.markFileCompleted(fileKey, fileRecordCount);
                    sp.setTotalRecords(totalRecords.get());
                    checkpointManager.markDirty();
                }

                log.info("  [{}] File {} completed: {} records", stableName, fileKey, fileRecordCount);
            }
        } finally {
            // Send one POISON per consumer to signal all to stop
            for (int c = 0; c < consumers; c++) {
                batchQueue.put(POISON);
            }
            for (Thread t : consumerThreads) {
                t.join(60_000);
            }
        }

        if (error.get()) {
            throw new RuntimeException("Import failed for super table " + stableName);
        }

        checkpointManager.saveIfDirty();
        log.info("  [{}] Import completed: total {} records", stableName, totalRecords.get());
    }

    // Header tracking fields (set per file by producer)
    private volatile String[] cachedHeader;
    private volatile Set<Integer> tagColumnIndices;
    /** Whether the CSV header includes 'tbname' as the first column (native JDBC mode). 
     *  REST API mode does not return tbname pseudo-column. */
    private volatile boolean hasTbname = true;

    private List<Path> findDataFiles(String stableName, Path dataDir) throws IOException {
        List<Path> files = new ArrayList<>();
        String prefix = stableName + "_";

        // 1. Scan date subdirectories (new format: {yyyyMMdd}/)
        List<Path> dateDirs = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir)) {
            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    String dirName = entry.getFileName().toString();
                    if (dirName.matches("\\d{8}")) {
                        dateDirs.add(entry);
                    }
                }
            }
        }
        // Sort date directories chronologically
        dateDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

        for (Path dateDir : dateDirs) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dateDir, "*.gz")) {
                List<Path> dayFiles = new ArrayList<>();
                for (Path entry : stream) {
                    String fileName = entry.getFileName().toString();
                    if (fileName.startsWith(prefix)) {
                        dayFiles.add(entry);
                    }
                }
                dayFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));
                files.addAll(dayFiles);
            }
        }

        // 2. Also scan flat directory (backward compat with old format)
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataDir, "*.gz")) {
            List<Path> flatFiles = new ArrayList<>();
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                if (fileName.startsWith(prefix)) {
                    flatFiles.add(entry);
                }
            }
            if (!flatFiles.isEmpty()) {
                flatFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));
                files.addAll(flatFiles);
                log.info("  Found {} file(s) in flat directory (old format) for {}", flatFiles.size(), stableName);
            }
        }

        return files;
    }

    /**
     * Build a checkpoint file key from a data file path relative to dataDir.
     * New format: "20240115/st1_120000.gz"
     * Old format: "st1_20240115_120000.gz"
     */
    private String buildFileKey(Path dataFile, Path dataDir) {
        Path relative = dataDir.relativize(dataFile);
        return relative.toString().replace('\\', '/');
    }

    private String[] parseCsvLine(String line) {
        List<String> fields = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (inQuotes) {
                if (c == '"') {
                    if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                        sb.append('"');
                        i++;
                    } else {
                        inQuotes = false;
                    }
                } else {
                    sb.append(c);
                }
            } else {
                if (c == '"') {
                    inQuotes = true;
                } else if (c == FIELD_SEP) {
                    fields.add(sb.toString());
                    sb.setLength(0);
                } else {
                    sb.append(c);
                }
            }
        }
        fields.add(sb.toString());
        return fields.toArray(new String[0]);
    }

    /**
     * Insert a batch of records. Now takes a connection parameter for multi-writer support.
     */
    private int insertBatch(String stableName, Set<String> tagColumnNames, List<String> batchLines,
                             TdConnection conn) {
        if (batchLines.isEmpty()) return 0;

        try {
            // Group by child table (tbname)
            Map<String, List<String>> grouped = new LinkedHashMap<>();

            for (String line : batchLines) {
                String tbname;
                if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                    if (hasTbname) {
                        int tabIdx = line.indexOf(FIELD_SEP);
                        tbname = tabIdx > 0 ? line.substring(0, tabIdx) : line;
                    } else {
                        // No tbname in CSV (REST API mode) - generate from tag values
                        tbname = generateTbnameFromCsv(line);
                    }
                } else {
                    tbname = extractJsonField(line, "tbname");
                    if (tbname == null) tbname = stableName + "_auto";
                }
                grouped.computeIfAbsent(tbname, k -> new ArrayList<>()).add(line);
            }

            StringBuilder sql = new StringBuilder(batchLines.size() * 128);
            sql.append("INSERT INTO ");

            boolean firstTable = true;

            // Track child tables created ON THIS CONNECTION to avoid USING TAGS on confirmed tables
            Set<String> locallyCreated = new HashSet<>();
            // If child table manifest is loaded, pre-populate with all known child tables for this super table
            if (hasChildTableManifest) {
                String prefix = stableName + ":";
                for (String key : createdChildTables) {
                    if (key.startsWith(prefix)) {
                        locallyCreated.add(key);
                    }
                }
            }

            for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
                String tbname = entry.getKey();
                List<String> tableRecords = entry.getValue();
                if (tableRecords.isEmpty()) continue;

                if (!firstTable) sql.append(' ');
                firstTable = false;

                String childTableKey = stableName + ":" + tbname;

                sql.append('`').append(tbname).append('`');

                // Use connection-local cache to avoid TDengine REST mode visibility issues:
                // If this connection has already created this child table locally, no USING needed.
                // Otherwise, always use USING TAGS to be safe (table may exist globally but not
                // yet visible on this connection in REST mode).
                if (!locallyCreated.contains(childTableKey)) {
                    locallyCreated.add(childTableKey);
                    createdChildTables.add(childTableKey); // global tracking (best-effort)

                    sql.append(" USING ").append('`').append(stableName).append('`').append(" TAGS (");

                    if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        appendTagValuesFromCsv(sql, tableRecords.getFirst());
                    } else {
                        appendTagValuesFromJson(sql, tableRecords.getFirst(), tagColumnNames);
                    }

                    sql.append(')');
                }

                sql.append(" VALUES ");

                for (int i = 0; i < tableRecords.size(); i++) {
                    if (i > 0) sql.append(' ');

                    if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        appendCsvValues(sql, tableRecords.get(i));
                    } else {
                        appendJsonValues(sql, tableRecords.get(i), tagColumnNames);
                    }
                }
            }

            String fullSql = sql.toString();
            try {
                conn.execute(fullSql);
            } catch (Exception e) {
                // Log full SQL for debugging, then rethrow
                log.error("Batch insert FAILED for super table {}:\n  SQL (first 2000 chars): {}\n  Error: {}",
                        stableName, fullSql.substring(0, Math.min(fullSql.length(), 2000)), e.getMessage());
                throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
            }
            return batchLines.size();
        } catch (Exception e) {
            log.error("Batch insert failed for super table {}: {}", stableName, e.getMessage());
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
    }

    /**
     * Append tag values from a CSV line using cached header for tag column detection.
     */
    private void appendTagValuesFromCsv(StringBuilder sql, String line) {
        String[] fields = parseCsvLine(line);
        String[] header = this.cachedHeader;
        Set<Integer> tagIndices = this.tagColumnIndices;

        if (header == null || tagIndices == null || tagIndices.isEmpty()) {
            // Fallback: no header info, can't identify tags
            return;
        }

        boolean firstTag = true;
        // Tag columns are identified by "tag_" prefix in header
        // Iterate in header order to maintain consistent TAGS order
        for (int i = 0; i < header.length && i < fields.length; i++) {
            if (tagIndices.contains(i)) {
                if (!firstTag) sql.append(", ");
                String val = fields[i];
                if (NULL_MARKER.equals(val) || val == null) {
                    sql.append("NULL");
                } else {
                    sql.append(formatSqlValue(val));
                }
                firstTag = false;
            }
        }
    }

    private void appendTagValuesFromJson(StringBuilder sql, String line, Set<String> tagColumnNames) {
        boolean firstTag = true;
        for (String tagName : tagColumnNames) {
            String val = extractJsonField(line, "tag_" + tagName);
            if (val == null) val = extractJsonField(line, tagName);
            if (!firstTag) sql.append(", ");
            sql.append(val != null ? formatSqlValue(val) : "NULL");
            firstTag = false;
        }
    }

    /**
     * Append data values from CSV line, excluding tbname (when present) and tag columns.
     */
    private void appendCsvValues(StringBuilder sql, String line) {
        String[] fields = parseCsvLine(line);
        Set<Integer> tagIndices = this.tagColumnIndices;

        sql.append('(');
        boolean first = true;
        int startIdx = hasTbname ? 1 : 0;
        for (int i = startIdx; i < fields.length; i++) {
            // Skip tag columns (they go in TAGS clause)
            if (tagIndices != null && tagIndices.contains(i)) continue;

            if (!first) sql.append(", ");
            String val = fields[i];
            if (NULL_MARKER.equals(val)) {
                sql.append("NULL");
            } else {
                sql.append(formatSqlValue(val));
            }
            first = false;
        }
        sql.append(')');
    }

    private void appendJsonValues(StringBuilder sql, String line, Set<String> tagColumnNames) {
        sql.append('(');
        boolean first = true;

        String trimmed = line.trim();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }

        String[] pairs = trimmed.split(",");
        for (String pair : pairs) {
            int colonIdx = pair.indexOf(':');
            if (colonIdx < 0) continue;

            String key = pair.substring(0, colonIdx).trim()
                    .replace("\"", "").replace("{", "").trim();
            String value = pair.substring(colonIdx + 1).trim()
                    .replace("\"", "").replace("}", "").trim();

            if ("tbname".equalsIgnoreCase(key)) continue;
            if (key.startsWith("tag_")) continue;
            if (tagColumnNames.contains(key.toLowerCase())) continue;

            if (!first) sql.append(", ");
            if ("null".equalsIgnoreCase(value) || value.isEmpty()) {
                sql.append("NULL");
            } else {
                sql.append(formatSqlValue(value));
            }
            first = false;
        }
        sql.append(')');
    }

    private String extractJsonField(String json, String fieldName) {
        String searchKey = "\"" + fieldName + "\":";
        int idx = json.indexOf(searchKey);
        if (idx < 0) {
            searchKey = "\"" + fieldName + "\" : ";
            idx = json.indexOf(searchKey);
        }
        if (idx < 0) return null;

        int start = idx + searchKey.length();
        while (start < json.length() && json.charAt(start) == ' ') start++;
        if (start >= json.length()) return null;

        if (json.charAt(start) == '"') {
            int end = json.indexOf('"', start + 1);
            if (end < 0) return null;
            return json.substring(start + 1, end);
        } else {
            int end = start;
            while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') end++;
            String val = json.substring(start, end).trim();
            return "null".equalsIgnoreCase(val) ? null : val;
        }
    }

    private String formatSqlValue(String value) {
        if (value == null || NULL_MARKER.equals(value) || "null".equalsIgnoreCase(value)) {
            return "NULL";
        }
        if (isUnquotedNumber(value)) return value;
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            return value.toLowerCase();
        }
        // Escape backslashes first, then single quotes.
        // TDengine treats both \ and ' as escape characters in string literals:
        //   \\ → literal backslash, \' → literal single quote
        // Without backslash escaping, a value like "d:\" produces 'd:\' which TDengine
        // interprets as string "d:" followed by unexpected closing parenthesis.
        return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'";
    }

    /**
     * Generate a deterministic child table name from tag values in a CSV line.
     * Used when the CSV does not contain tbname (REST API mode).
     */
    private String generateTbnameFromCsv(String line) {
        String[] fields = parseCsvLine(line);
        Set<Integer> tagIndices = this.tagColumnIndices;
        if (tagIndices != null && !tagIndices.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (int idx : tagIndices) {
                if (idx < fields.length) {
                    String val = fields[idx];
                    if (val != null && !NULL_MARKER.equals(val) && !val.isEmpty()) {
                        if (sb.length() > 0) sb.append('_');
                        // Sanitize for TDengine identifier: replace non-alphanum with underscore
                        String sanitized = val.replaceAll("[^a-zA-Z0-9_]", "_");
                        sb.append(sanitized);
                    }
                }
            }
            if (sb.length() > 0) return sb.toString();
        }
        // Fallback: timestamp-based unique name
        return "_imp_" + System.nanoTime();
    }

    private boolean isUnquotedNumber(String value) {
        if (value.isEmpty()) return false;
        boolean hasDigit = false;
        boolean hasDot = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '-' && i == 0) continue;
            if (c == '.' && !hasDot) { hasDot = true; continue; }
            if ((c == 'e' || c == 'E') && i > 0) continue;
            if (c == '+' && i > 0 && (value.charAt(i - 1) == 'e' || value.charAt(i - 1) == 'E')) continue;
            if (Character.isDigit(c)) { hasDigit = true; continue; }
            return false;
        }
        return hasDigit;
    }

    /**
     * Sanitize a CREATE STABLE statement by removing trailing options (e.g. SECURE_DELETE, WATERMARK)
     * that may not be supported by the target TDengine version.
     * Keeps only the core DDL: CREATE STABLE ... (columns) TAGS (tags)
     */
    static String sanitizeCreateStable(String createStmt) {
        int tagsIdx = createStmt.indexOf("TAGS (");
        if (tagsIdx < 0) {
            // Try lowercase
            tagsIdx = createStmt.toUpperCase().indexOf("TAGS (");
            if (tagsIdx < 0) return createStmt.trim();
        }

        // Find the matching closing paren after "TAGS ("
        int parenDepth = 0;
        boolean inParen = false;
        for (int i = tagsIdx; i < createStmt.length(); i++) {
            char c = createStmt.charAt(i);
            if (c == '(') {
                parenDepth++;
                inParen = true;
            } else if (c == ')') {
                parenDepth--;
                if (inParen && parenDepth == 0) {
                    // Found the closing paren of TAGS clause
                    return createStmt.substring(0, i + 1).trim();
                }
            }
        }

        // Fallback: if we can't parse properly, just trim the input
        return createStmt.trim();
    }
}
