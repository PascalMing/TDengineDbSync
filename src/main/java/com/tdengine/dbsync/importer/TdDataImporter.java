package com.tdengine.dbsync.importer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
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
import java.util.concurrent.atomic.AtomicInteger;

public class TdDataImporter implements DataImporter {

    private static final Logger log = LoggerFactory.getLogger(TdDataImporter.class);
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000;
    private static final long CHECKPOINT_SAVE_INTERVAL_MS = 30_000;
    private static final char FIELD_SEP = '\t';
    private static final String NULL_MARKER = "\\N";
    private static final int CHILD_TABLE_BATCH_SIZE = 200;

    private static final class FileBatch {
        final String fileKey;
        final int batchIndex;
        final List<String> lines;
        final FileWorkTracker tracker;
        final boolean poison;

        private FileBatch(String fileKey, int batchIndex, List<String> lines, FileWorkTracker tracker, boolean poison) {
            this.fileKey = fileKey;
            this.batchIndex = batchIndex;
            this.lines = lines;
            this.tracker = tracker;
            this.poison = poison;
        }

        static FileBatch data(String fileKey, int batchIndex, List<String> lines, FileWorkTracker tracker) {
            return new FileBatch(fileKey, batchIndex, lines, tracker, false);
        }

        static FileBatch poison() {
            return new FileBatch(null, -1, Collections.emptyList(), null, true);
        }
    }

    private static final class FileWorkTracker {
        private final AtomicInteger pendingBatches = new AtomicInteger(0);
        private final AtomicLong insertedRows = new AtomicLong(0);
        private final Map<Integer, Integer> batchSizes = new ConcurrentHashMap<>();
        private final Set<Integer> completedBatches = ConcurrentHashMap.newKeySet();
        private final AtomicInteger nextCommittedBatch = new AtomicInteger(0);
        private final AtomicLong committedOffset = new AtomicLong(0);

        void addBatch(int batchIndex, int batchSize) {
            batchSizes.put(batchIndex, batchSize);
            pendingBatches.incrementAndGet();
        }

        long batchCompleted(int batchIndex, long inserted) {
            insertedRows.addAndGet(inserted);
            completedBatches.add(batchIndex);
            synchronized (this) {
                advanceCommittedOffsetLocked();
                if (pendingBatches.decrementAndGet() <= 0) {
                    notifyAll();
                }
            }
            return committedOffset.get();
        }

        void batchFailed() {
            synchronized (this) {
                if (pendingBatches.decrementAndGet() <= 0) {
                    notifyAll();
                }
            }
        }

        long getInsertedRows() {
            return insertedRows.get();
        }

        long getCommittedOffset() {
            return committedOffset.get();
        }

        void awaitCompletion(AtomicBoolean errorFlag) throws InterruptedException {
            synchronized (this) {
                while (pendingBatches.get() > 0 && !errorFlag.get()) {
                    wait(250L);
                }
            }
        }

        private void advanceCommittedOffsetLocked() {
            while (true) {
                int nextBatch = nextCommittedBatch.get();
                if (!completedBatches.contains(nextBatch)) {
                    return;
                }
                Integer batchSize = batchSizes.remove(nextBatch);
                if (batchSize == null) {
                    return;
                }
                completedBatches.remove(nextBatch);
                committedOffset.addAndGet(batchSize);
                nextCommittedBatch.incrementAndGet();
            }
        }
    }

    private final SyncProperties properties;
    private final TdConnectionFactory connectionFactory;
    private final CheckpointManager checkpointManager;
    private final ObjectMapper objectMapper;
    private final Set<String> createdChildTables = ConcurrentHashMap.newKeySet();

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
            for (var entry : checkpoint.getStables().entrySet()) {
                var sp = entry.getValue();
                log.info("  [{}] completedFiles={}, currentFile={}, currentFileOffset={}, totalRecords={}",
                        entry.getKey(), sp.getCompletedFiles().size(), sp.getCurrentFile(),
                        sp.getCurrentFileOffset(), sp.getTotalRecords());
            }
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
            loadAndReconcileChildTables(sourceDb, targetDb, schemaFile, metaConn);
        }

        List<String> targetTables = resolveSuperTables(schemaFile);

        if (targetTables.isEmpty()) {
            log.warn("No super tables to import");
            return;
        }

        int parallel = Math.min(properties.getParallel(), targetTables.size());
        log.info("Importing {} super table(s) with {} concurrent pipeline(s)", targetTables.size(), parallel);

        ExecutorService pipelineExecutor = Executors.newFixedThreadPool(parallel, r -> {
            Thread t = new Thread(r, "import-pps");
            t.setDaemon(true);
            return t;
        });

        List<Future<Void>> futures = new ArrayList<>();
        for (String stableName : targetTables) {
            final String stable = stableName;
            futures.add(pipelineExecutor.submit(() -> {
                importSuperTableData(targetDb, stable, schemaFile.getSuperTables().get(stable), dataDir, checkpoint);
                return null;
            }));
        }
        pipelineExecutor.shutdown();

        // Collect errors
        List<Exception> errors = new ArrayList<>();
        for (Future<Void> f : futures) {
            try {
                f.get();
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

        if (!errors.isEmpty()) {
            for (Exception err : errors) {
                log.error("Import error: {}: {}", err.getClass().getName(), err.getMessage(), err);
            }
            Exception first = errors.getFirst();
            throw new RuntimeException("Import failed: " + errors.size() + " super table(s) had errors. " +
                    "First error: " + first.getClass().getName() + ": " + first.getMessage(), first);
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
                conn.query("SHOW TABLE TAGS FROM " + database + "." + stableName, rs -> {
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

    private void loadAndReconcileChildTables(String sourceDb, String targetDb, SchemaFile schemaFile, TdConnection conn) {
        Path manifestDir = Path.of(properties.getDataDir(), sourceDb, "structure");

        for (Map.Entry<String, SuperTableMeta> entry : schemaFile.getSuperTables().entrySet()) {
            String stableName = entry.getKey();
            SuperTableMeta meta = entry.getValue();

            if (meta.getTags() == null || meta.getTags().isEmpty()) {
                log.debug("  Skipping child table reconcile for {} (no tags)", stableName);
                continue;
            }

            Path manifestPath = manifestDir.resolve(stableName + ".jsonl.gz");
            if (!Files.exists(manifestPath)) {
                throw new RuntimeException("Child table manifest not found: " + manifestPath);
            }

            Set<String> existingChildTables = queryExistingChildTables(targetDb, stableName, conn);
            createdChildTables.addAll(existingChildTables);
            log.info("  [{}] Existing child tables in target: {}", stableName, existingChildTables.size());

            reconcileChildTablesForStable(targetDb, stableName, meta, manifestPath, existingChildTables, conn);
        }
    }

    private Set<String> queryExistingChildTables(String database, String stableName, TdConnection conn) {
        Set<String> names = new LinkedHashSet<>();
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
            log.warn("  SHOW TABLE TAGS FROM failed for existing child tables of {}: {}", stableName, e.getMessage());
        }

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
                log.debug("  No existing child tables found for {}: {}", stableName, e.getMessage());
            }
        }

        return names;
    }

    private void reconcileChildTablesForStable(String database, String stableName, SuperTableMeta meta,
                                               Path manifestPath, Set<String> existingChildTables,
                                               TdConnection conn) {
        int createdCount = 0;
        int resetCount = 0;
        int scannedCount = 0;

        try {
            List<ChildTableMeta> allChildren = readChildTableManifest(manifestPath, stableName);
            if (allChildren.isEmpty()) {
                throw new RuntimeException("Child table manifest is empty: " + manifestPath);
            }

            List<ChildTableMeta> batch = new ArrayList<>(CHILD_TABLE_BATCH_SIZE);
            for (ChildTableMeta child : allChildren) {
                if (child == null || child.getTbname() == null || child.getTbname().isBlank()) {
                    continue;
                }
                batch.add(child);
                scannedCount++;
                if (batch.size() >= CHILD_TABLE_BATCH_SIZE) {
                    int[] counts = reconcileChildTableBatch(database, stableName, meta, batch, existingChildTables, conn);
                    createdCount += counts[0];
                    resetCount += counts[1];
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                int[] counts = reconcileChildTableBatch(database, stableName, meta, batch, existingChildTables, conn);
                createdCount += counts[0];
                resetCount += counts[1];
            }

            log.info("  [{}] Reconciled {} child table(s): created {}, reset {}",
                    stableName, scannedCount, createdCount, resetCount);
        } catch (Exception e) {
            throw new RuntimeException("Failed to reconcile child tables for " + stableName + ": " + e.getMessage(), e);
        }
    }

    private List<ChildTableMeta> readChildTableManifest(Path manifestPath, String stableName) throws Exception {
        try (InputStream fis = Files.newInputStream(manifestPath);
             GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(fis);
             BufferedInputStream bufferedIn = new BufferedInputStream(gzipIn, 131072);
             JsonParser parser = objectMapper.getFactory().createParser(bufferedIn)) {

            JsonNode firstNode = objectMapper.readTree(parser);
            if (firstNode == null) {
                return Collections.emptyList();
            }

            if (firstNode.has("superTables")) {
                ChildTableManifest manifest = objectMapper.treeToValue(firstNode, ChildTableManifest.class);
                List<ChildTableMeta> children = manifest.getSuperTables().get(stableName);
                return children == null ? Collections.emptyList() : new ArrayList<>(children);
            }

            List<ChildTableMeta> children = new ArrayList<>();
            children.add(objectMapper.treeToValue(firstNode, ChildTableMeta.class));

            MappingIterator<ChildTableMeta> iterator = objectMapper.readerFor(ChildTableMeta.class).readValues(parser);
            while (iterator.hasNextValue()) {
                children.add(iterator.nextValue());
            }

            return children;
        }
    }

    private int[] reconcileChildTableBatch(String database, String stableName, SuperTableMeta meta,
                                           List<ChildTableMeta> batch, Set<String> existingChildTables,
                                           TdConnection conn) throws Exception {
        List<ChildTableMeta> missing = new ArrayList<>();
        List<ChildTableMeta> existing = new ArrayList<>();

        for (ChildTableMeta child : batch) {
            if (existingChildTables.contains(child.getTbname())) {
                existing.add(child);
            } else {
                missing.add(child);
            }
        }

        int created = 0;
        int reset = 0;

        if (!missing.isEmpty()) {
            for (ChildTableMeta child : missing) {
                conn.execute(buildCreateChildTableSql(stableName, child, meta));
            }
            for (ChildTableMeta child : missing) {
                String key = stableName + ":" + child.getTbname();
                createdChildTables.add(key);
                existingChildTables.add(child.getTbname());
            }
            created = missing.size();
        }

        if (!existing.isEmpty()) {
            Map<String, LinkedHashMap<String, String>> currentTags = queryChildTableTags(database, stableName, meta, existing, conn);
            List<ChildTableMeta> needReset = new ArrayList<>();
            for (ChildTableMeta child : existing) {
                LinkedHashMap<String, String> current = currentTags.get(child.getTbname());
                if (!tagsEqual(meta, current, child.getTagValues())) {
                    needReset.add(child);
                }
            }

            if (!needReset.isEmpty()) {
                for (ChildTableMeta child : needReset) {
                    conn.execute(buildAlterChildTableTagSql(child, meta));
                }
                for (ChildTableMeta child : needReset) {
                    createdChildTables.add(stableName + ":" + child.getTbname());
                }
                reset = needReset.size();
            } else {
                for (ChildTableMeta child : existing) {
                    createdChildTables.add(stableName + ":" + child.getTbname());
                }
            }
        }

        return new int[]{created, reset};
    }

    private Map<String, LinkedHashMap<String, String>> queryChildTableTags(String database, String stableName,
                                                                           SuperTableMeta meta,
                                                                           List<ChildTableMeta> children,
                                                                           TdConnection conn) {
        Map<String, LinkedHashMap<String, String>> result = new LinkedHashMap<>();
        if (children.isEmpty()) {
            return result;
        }

        List<String> names = new ArrayList<>(children.size());
        for (ChildTableMeta child : children) {
            names.add(child.getTbname());
        }

        StringBuilder sql = new StringBuilder("SELECT TBNAME");
        for (DataColumn tag : meta.getTags()) {
            sql.append(", `").append(tag.getName()).append("`");
        }
        sql.append(" FROM ").append(database).append('.').append(stableName).append(" WHERE tbname IN (");
        for (int i = 0; i < names.size(); i++) {
            if (i > 0) sql.append(", ");
            sql.append('\'').append(names.get(i).replace("'", "\\'")).append('\'');
        }
        sql.append(") GROUP BY TBNAME");

        conn.query(sql.toString(), rs -> {
            while (rs.next()) {
                String tbname = rs.getString(1);
                if (tbname == null || tbname.isBlank()) continue;
                LinkedHashMap<String, String> tagVals = new LinkedHashMap<>();
                int colIdx = 2;
                for (DataColumn tag : meta.getTags()) {
                    Object val = rs.getObject(colIdx++);
                    tagVals.put(tag.getName(), val != null ? val.toString() : null);
                }
                result.put(tbname, tagVals);
            }
        });

        return result;
    }

    private boolean tagsEqual(SuperTableMeta meta, LinkedHashMap<String, String> current,
                              LinkedHashMap<String, String> expected) {
        if (current == null || expected == null) {
            return false;
        }
        for (DataColumn tag : meta.getTags()) {
            String name = tag.getName();
            if (!Objects.equals(current.get(name), expected.get(name))) {
                return false;
            }
        }
        return true;
    }

    private String buildCreateChildTableSql(String stableName, ChildTableMeta child, SuperTableMeta meta) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS `").append(child.getTbname()).append('`')
                .append(" USING `").append(stableName).append("` TAGS (");
        appendTagValues(sql, meta, child.getTagValues());
        sql.append(')');
        return sql.toString();
    }

    private String buildAlterChildTableTagSql(ChildTableMeta child, SuperTableMeta meta) {
        StringBuilder sql = new StringBuilder();
        sql.append("ALTER TABLE `").append(child.getTbname()).append("` SET TAG ");
        appendNamedTagValues(sql, meta, child.getTagValues());
        return sql.toString();
    }

    private void appendTagValues(StringBuilder sql, SuperTableMeta meta, LinkedHashMap<String, String> tagValues) {
        boolean first = true;
        for (DataColumn tag : meta.getTags()) {
            if (!first) sql.append(", ");
            sql.append(formatSqlValue(tagValues.get(tag.getName())));
            first = false;
        }
    }

    private void appendNamedTagValues(StringBuilder sql, SuperTableMeta meta, LinkedHashMap<String, String> tagValues) {
        boolean first = true;
        for (DataColumn tag : meta.getTags()) {
            if (!first) sql.append(", ");
            sql.append(tag.getName()).append('=').append(formatSqlValue(tagValues.get(tag.getName())));
            first = false;
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

        BlockingQueue<FileBatch> batchQueue = new LinkedBlockingQueue<>(queueSize);
        FileBatch POISON = FileBatch.poison();

        AtomicBoolean error = new AtomicBoolean(false);
        AtomicLong totalRecords = new AtomicLong(sp != null ? sp.getTotalRecords() : 0);
        AtomicLong lastProgressTime = new AtomicLong(System.currentTimeMillis());
        AtomicLong lastCheckpointSaveTime = new AtomicLong(System.currentTimeMillis());

        // Per-pipeline context (NOT shared across super tables)
        PipelineCtx ctx = new PipelineCtx();

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
                        FileBatch job = batchQueue.take();
                        if (job.poison) break;
                        if (error.get()) break;

                        try {
                            int inserted = insertBatch(stableName, meta, tagColumnNames, job.lines, writeConn, ctx);
                            totalRecords.addAndGet(inserted);
                            long committedOffset = job.tracker != null ? job.tracker.batchCompleted(job.batchIndex, inserted) : 0;
                            if (job.tracker != null) {
                                maybeUpdateFileCheckpoint(sp, job.fileKey, committedOffset, totalRecords, lastCheckpointSaveTime);
                            }

                            long now = System.currentTimeMillis();
                            if (now - lastProgressTime.get() >= PROGRESS_LOG_INTERVAL_MS) {
                                log.info("  [{}] Imported {} records (writer-{})", stableName, totalRecords.get(), consumerId);
                                lastProgressTime.set(now);
                            }
                        } catch (Exception e) {
                            log.error("  [{}] Batch insert failed (writer-{}): {}", stableName, consumerId, e.getMessage());
                            error.set(true);
                            if (job.tracker != null) {
                                job.tracker.batchFailed();
                            }
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
                FileWorkTracker fileTracker = new FileWorkTracker();
                int batchIndex = 0;

                try (InputStream fis = Files.newInputStream(dataFile);
                     GzipCompressorInputStream gzipIn = new GzipCompressorInputStream(fis);
                     BufferedReader reader = new BufferedReader(
                             new InputStreamReader(gzipIn, StandardCharsets.UTF_8), 131072) {

                         // Override to track line count
                     }) {

                    // Read header line and parse tag column positions
                    String headerLine = reader.readLine();
                    if (headerLine != null && properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        ctx.initFromHeader(headerLine);
                        // Detect per-child-table format (new export style): no tbname, no tags
                        if (!ctx.hasTbname && (ctx.tagColumnIndices == null || ctx.tagColumnIndices.isEmpty())) {
                            String fileName = dataFile.getFileName().toString();
                            String childTable = extractChildTableName(fileName, stableName);
                            ctx.currentFileChildTable = childTable;
                            if (childTable != null) {
                                log.info("  [{}] Detected per-child-table file format, child table: {}",
                                        stableName, childTable);
                            } else {
                                log.warn("  [{}] Could not extract child table name from filename: {}",
                                        stableName, fileName);
                            }
                        } else {
                            ctx.currentFileChildTable = null;
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
                    } else {
                        log.info("  [{}] Starting file {} from the beginning", stableName, fileKey);
                    }

                    List<String> batchLines = new ArrayList<>(batchSize);
                    String line;

                    while ((line = reader.readLine()) != null) {
                        if (line.isBlank()) continue;
                        if (error.get()) break;

                        batchLines.add(line);
                        fileRecordCount++;

                        if (batchLines.size() >= batchSize) {
                            fileTracker.addBatch(batchIndex, batchLines.size());
                            batchQueue.put(FileBatch.data(fileKey, batchIndex, new ArrayList<>(batchLines), fileTracker));
                            batchLines.clear();
                            batchIndex++;
                        }
                    }

                    if (!batchLines.isEmpty()) {
                        fileTracker.addBatch(batchIndex, batchLines.size());
                        batchQueue.put(FileBatch.data(fileKey, batchIndex, new ArrayList<>(batchLines), fileTracker));
                    }
                }

                try {
                    fileTracker.awaitCompletion(error);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while waiting for file batches to finish: " + fileKey, e);
                }

                long insertedRows = fileTracker.getInsertedRows();
                log.info("  [{}] File {} finished: read {} rows, inserted {} rows",
                        stableName, fileKey, fileRecordCount, insertedRows);
                log.info("  [{}] File {} checkpoint state: currentFile={}, currentFileOffset={}, completedFiles={}",
                        stableName, fileKey,
                        sp != null ? sp.getCurrentFile() : null,
                        sp != null ? sp.getCurrentFileOffset() : 0,
                        sp != null ? sp.getCompletedFiles().size() : 0);

                if (sp != null && !error.get()) {
                    sp.setCurrentFileOffset(fileTracker.getCommittedOffset());
                    sp.setCurrentFile(fileTracker.getCommittedOffset() >= fileRecordCount ? null : fileKey);
                    checkpointManager.markDirty();
                }

                if (sp != null && !error.get() && fileRecordCount == insertedRows) {
                    sp.markFileCompleted(fileKey, fileRecordCount);
                    sp.setTotalRecords(totalRecords.get());
                    checkpointManager.markDirty();
                } else if (sp != null && !error.get()) {
                    log.warn("  [{}] File {} not checkpointed because read/inserted mismatch: {} vs {}",
                            stableName, fileKey, fileRecordCount, insertedRows);
                }

                if (fileRecordCount != insertedRows) {
                    log.warn("  [{}] File {} mismatch: read {} rows, inserted {} rows",
                            stableName, fileKey, fileRecordCount, insertedRows);
                }
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

    private void maybeUpdateFileCheckpoint(ProgressCheckpoint.StableProgress sp, String fileKey,
                                           long committedOffset, AtomicLong totalRecords,
                                           AtomicLong lastCheckpointSaveTime) {
        if (sp == null) {
            return;
        }
        sp.setCurrentFile(fileKey);
        sp.setCurrentFileOffset(committedOffset);
        sp.setTotalRecords(totalRecords.get());
        checkpointManager.markDirty();

        long now = System.currentTimeMillis();
        if (now - lastCheckpointSaveTime.get() >= CHECKPOINT_SAVE_INTERVAL_MS) {
            checkpointManager.saveIfDirty();
            lastCheckpointSaveTime.set(now);
        }
    }

    // ---- Pipeline context (per-super-table) ----

    /**
     * Per-pipeline mutable state scoped to one {@link #importSuperTableData} call.
     * Not thread-safe; only the producer thread writes, consumer threads read after
     * happens-before edges established by {@link BlockingQueue}.
     */
    private static class PipelineCtx {
        String[] header;
        Set<Integer> tagColumnIndices;
        boolean hasTbname;
        /** Per-child-table mode: all lines belong to this child table. */
        String currentFileChildTable;

        void initFromHeader(String headerLine) {
            this.header = parseCsvLine(headerLine);
            this.hasTbname = header.length > 0 && "tbname".equals(header[0]);
            this.tagColumnIndices = new HashSet<>();
            for (int i = 0; i < header.length; i++) {
                if (header[i].startsWith("tag_")) {
                    tagColumnIndices.add(i);
                }
            }
        }
    }

    /**
     * Extract child table name from a per-child-table export filename.
     * Format: {stableName}_{childTable}_{timestamp}.gz
     * Algorithm: remove .gz → strip trailing all-digit segments → remove stableName_ prefix
     */
    private String extractChildTableName(String fileName, String stableName) {
        String name = fileName;
        if (name.endsWith(".gz")) {
            name = name.substring(0, name.length() - 3);
        }
        // Strip trailing all-digit segments (timestamp + optional collision suffix)
        while (true) {
            int lastUnderscore = name.lastIndexOf('_');
            if (lastUnderscore < 0) break;
            String lastPart = name.substring(lastUnderscore + 1);
            if (lastPart.matches("\\d+")) {
                name = name.substring(0, lastUnderscore);
            } else {
                break;
            }
        }
        // Remove stable name prefix
        String prefix = stableName + "_";
        if (name.startsWith(prefix)) {
            name = name.substring(prefix.length());
        }
        return name.isEmpty() ? null : name;
    }

    private List<Path> findDataFiles(String stableName, Path dataDir) throws IOException {
        List<Path> files = new ArrayList<>();
        String prefix = stableName + "_";

        // 1. Scan date/slice_* partition subdirectories (new time-window format)
        //    Path: {dataDir}/{stableName}/{yyyyMMdd}/slice_{HHmmss}_{index}/*.gz
        Path stableDir = dataDir.resolve(stableName);
        if (Files.isDirectory(stableDir)) {
            List<Path> dateDirs = new ArrayList<>();
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(stableDir, entry ->
                    Files.isDirectory(entry) && entry.getFileName().toString().matches("\\d{8}"))) {
                for (Path entry : stream) {
                    dateDirs.add(entry);
                }
            }
            dateDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

            for (Path dateDir : dateDirs) {
                List<Path> sliceDirs = new ArrayList<>();
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(dateDir, entry ->
                        Files.isDirectory(entry) && entry.getFileName().toString().startsWith("slice_"))) {
                    for (Path entry : stream) {
                        sliceDirs.add(entry);
                    }
                }
                sliceDirs.sort(Comparator.comparing(p -> p.getFileName().toString()));

                for (Path sliceDir : sliceDirs) {
                    try (DirectoryStream<Path> sliceStream = Files.newDirectoryStream(sliceDir, "*.gz")) {
                        List<Path> sliceFiles = new ArrayList<>();
                        for (Path entry : sliceStream) {
                            String fileName = entry.getFileName().toString();
                            if (fileName.startsWith(prefix)) {
                                sliceFiles.add(entry);
                            }
                        }
                        sliceFiles.sort(Comparator.comparing(p -> p.getFileName().toString()));
                        files.addAll(sliceFiles);
                    }
                }
            }
            if (!files.isEmpty()) {
                log.info("  Found {} file(s) in partition subdirectories for {}", files.size(), stableName);
            }
        }

        // 2. Scan date subdirectories (backward compat: {yyyyMMdd}/)
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

        // 3. Also scan flat directory (backward compat with old format)
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

    private static String[] parseCsvLine(String line) {
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
    private int insertBatch(String stableName, SuperTableMeta meta, Set<String> tagColumnNames, List<String> batchLines,
                              TdConnection conn, PipelineCtx ctx) {
        if (batchLines.isEmpty()) return 0;

        try {
            // Per-child-table format (new export style): all lines belong to one child table
            if (ctx.currentFileChildTable != null) {
                return insertBatchToChildTable(stableName, meta, batchLines, conn, ctx);
            }

            // Group by child table (tbname)
            Map<String, List<String>> grouped = new LinkedHashMap<>();

            for (String line : batchLines) {
                String tbname;
                if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                    if (ctx.hasTbname) {
                        int tabIdx = line.indexOf(FIELD_SEP);
                        tbname = tabIdx > 0 ? line.substring(0, tabIdx) : line;
                    } else {
                        // No tbname in CSV (REST API mode) - generate from tag values
                        tbname = generateTbnameFromCsv(line, ctx);
                    }
                } else {
                    tbname = extractJsonField(line, "tbname");
                    if (tbname == null) tbname = stableName + "_auto";
                }
                grouped.computeIfAbsent(tbname, k -> new ArrayList<>()).add(line);
            }

            // Track child tables created ON THIS CONNECTION to avoid USING TAGS on confirmed tables
            Set<String> locallyCreated = new HashSet<>();
            // If child table manifest is loaded, pre-populate with all known child tables for this super table
            locallyCreated.addAll(createdChildTables);

            for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
                String tbname = entry.getKey();
                List<String> tableRecords = entry.getValue();
                if (tableRecords.isEmpty()) continue;

                String childTableKey = stableName + ":" + tbname;

                StringBuilder sql = new StringBuilder(tableRecords.size() * 96 + 64);
                sql.append("INSERT INTO `").append(tbname).append('`');

                // Use connection-local cache to avoid TDengine REST mode visibility issues:
                // If this connection has already created this child table locally, no USING needed.
                // Otherwise, always use USING TAGS to be safe (table may exist globally but not
                // yet visible on this connection in REST mode).
                if (!locallyCreated.contains(childTableKey)) {
                    locallyCreated.add(childTableKey);
                    createdChildTables.add(childTableKey); // global tracking (best-effort)

                    sql.append(" USING ").append('`').append(stableName).append('`').append(" TAGS (");

                    if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        appendTagValuesFromCsv(sql, tableRecords.getFirst(), ctx);
                    } else {
                        appendTagValuesFromJson(sql, tableRecords.getFirst(), tagColumnNames);
                    }

                    sql.append(')');
                }

                sql.append(" VALUES ");

                for (int i = 0; i < tableRecords.size(); i++) {
                    if (i > 0) sql.append(' ');

                    if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                        appendCsvValues(sql, tableRecords.get(i), ctx, meta);
                    } else {
                        appendJsonValues(sql, tableRecords.get(i), tagColumnNames);
                    }
                }

                String groupSql = sql.toString();
                try {
                    conn.execute(groupSql);
                } catch (Exception e) {
                    // Log full SQL for debugging, then rethrow
                    log.error("Batch insert FAILED for super table {}, child table {}:\n  SQL (first 2000 chars): {}\n  Error: {}",
                            stableName, tbname, groupSql.substring(0, Math.min(groupSql.length(), 2000)), e.getMessage());
                    throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
                }
            }
            return batchLines.size();
        } catch (Exception e) {
            log.error("Batch insert failed for super table {}: {}", stableName, e.getMessage());
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
    }

    /**
     * Insert a batch of records into a known child table (per-child-table format).
     * All lines in the batch belong to one child table, values are data columns only.
     * The child table is already created by the manifest, so no USING TAGS needed.
     */
    private int insertBatchToChildTable(String stableName, SuperTableMeta meta, List<String> batchLines,
                                          TdConnection conn, PipelineCtx ctx) {
        String childTable = ctx.currentFileChildTable;
        int estimated = Math.max(batchLines.size() * 64, 256);
        StringBuilder sql = new StringBuilder(estimated);
        sql.append("INSERT INTO `").append(childTable).append("` VALUES ");

        for (int i = 0; i < batchLines.size(); i++) {
            if (i > 0) sql.append(' ');
            if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                appendCsvValuesForChildTable(sql, batchLines.get(i), ctx, meta);
            } else {
                // JSON: exclude tbname and tag_ fields
                appendJsonValuesForChildTable(sql, batchLines.get(i));
            }
        }

        try {
            conn.execute(sql.toString());
        } catch (Exception e) {
            log.error("Batch insert FAILED for child table {}:\n  SQL (first 2000 chars): {}\n  Error: {}",
                    childTable, sql.substring(0, Math.min(sql.length(), 2000)), e.getMessage());
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
        return batchLines.size();
    }

    /**
     * Append a CSV line for per-child-table files.
     * Newer exports may still include tbname/tag columns in the file, so we skip them here.
     */
    private void appendCsvValuesForChildTable(StringBuilder sql, String line, PipelineCtx ctx, SuperTableMeta meta) {
        String[] fields = parseCsvLine(line);
        String[] header = ctx.header;
        int dataColumnCount = meta != null && meta.getColumns() != null ? meta.getColumns().size() : fields.length;

        sql.append('(');
        boolean first = true;

        if (header != null && header.length == fields.length) {
            int startIdx = ctx.hasTbname ? 1 : 0;
            int endIdx = Math.min(fields.length, startIdx + dataColumnCount);
            for (int i = startIdx; i < endIdx; i++) {
                if (!first) sql.append(", ");
                String val = fields[i];
                if (NULL_MARKER.equals(val)) {
                    sql.append("NULL");
                } else {
                    sql.append(formatSqlValue(val));
                }
                first = false;
            }
        } else {
            // Fallback: assume file already contains only data columns.
            int endIdx = Math.min(fields.length, dataColumnCount);
            for (int i = 0; i < endIdx; i++) {
                if (!first) sql.append(", ");
                String val = fields[i];
                if (NULL_MARKER.equals(val)) {
                    sql.append("NULL");
                } else {
                    sql.append(formatSqlValue(val));
                }
                first = false;
            }
        }

        sql.append(')');
    }

    /**
     * Append data values from JSON excluding tbname and tag_ fields.
     * Used for per-child-table JSON format where only data columns are in the record.
     */
    private void appendJsonValuesForChildTable(StringBuilder sql, String line) {
        sql.append('(');
        String trimmed = line.trim();
        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            trimmed = trimmed.substring(1, trimmed.length() - 1);
        }
        boolean first = true;
        String[] pairs = trimmed.split(",");
        for (String pair : pairs) {
            int colonIdx = pair.indexOf(':');
            if (colonIdx < 0) continue;
            String key = pair.substring(0, colonIdx).trim().replace("\"", "").trim();
            String value = pair.substring(colonIdx + 1).trim().replace("\"", "").trim();
            if ("tbname".equalsIgnoreCase(key)) continue;
            if (key.startsWith("tag_")) continue;
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

    /**
     * Append tag values from a CSV line using cached header for tag column detection.
     */
    private void appendTagValuesFromCsv(StringBuilder sql, String line, PipelineCtx ctx) {
        String[] fields = parseCsvLine(line);
        String[] header = ctx.header;
        Set<Integer> tagIndices = ctx.tagColumnIndices;

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
    private void appendCsvValues(StringBuilder sql, String line, PipelineCtx ctx, SuperTableMeta meta) {
        String[] fields = parseCsvLine(line);
        int dataColumnCount = meta != null && meta.getColumns() != null ? meta.getColumns().size() : fields.length;

        sql.append('(');
        boolean first = true;
        int startIdx = ctx.hasTbname ? 1 : 0;
        int endIdx = Math.min(fields.length, startIdx + dataColumnCount);
        for (int i = startIdx; i < endIdx; i++) {
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
    private String generateTbnameFromCsv(String line, PipelineCtx ctx) {
        String[] fields = parseCsvLine(line);
        Set<Integer> tagIndices = ctx.tagColumnIndices;
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
