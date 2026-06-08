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
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TdDataImporter implements DataImporter {

    private static final Logger log = LoggerFactory.getLogger(TdDataImporter.class);
    private static final long PROGRESS_LOG_INTERVAL_MS = 10_000;
    private static final long CHECKPOINT_SAVE_INTERVAL_MS = 30_000;
    private static final char FIELD_SEP = '\t';
    private static final String NULL_MARKER = "\\N";

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
    /** Child tables excluded by import conditions — their data rows are skipped */
    private final Set<String> filteredOutChildTables = ConcurrentHashMap.newKeySet();
    /** Global summary counters (updated by pipeline threads, read after all complete). */
    private final AtomicLong globalTotalRead = new AtomicLong(0);
    private final AtomicLong globalTotalFiltered = new AtomicLong(0);
    /** Actual rows reported by TDengine executeUpdate() (may differ from read due to ts dedup). */
    private final AtomicLong globalTotalInserted = new AtomicLong(0);

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

        // Validate import conditions against schema file (column/tag definitions)
        validateImportConditions(schemaFile);

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

        // Global import summary with actual DB-acknowledged counts
        long globalRead = globalTotalRead.get();
        long globalFiltered = globalTotalFiltered.get();
        long globalSentToDb = globalRead - globalFiltered;
        long globalAcknowledged = globalTotalInserted.get();
        String timeRange = properties.hasTimeRange()
                ? String.format("%s ~ %s",
                    properties.getStartTime() != null ? properties.getStartTime() : "*",
                    properties.getEndTime() != null ? properties.getEndTime() : "*")
                : "auto-detect";
        log.info("+===============================================");
        log.info("|  IMPORT COMPLETED -- Global Summary");
        log.info("|  Target database    : {}", targetDb);
        log.info("|  Time range         : {}", timeRange);
        log.info("|  Rows read (files)  : {}", globalRead);
        if (globalFiltered > 0) {
            log.info("|  Rows filtered      : {} (by import conditions)", globalFiltered);
        }
        log.info("|  Rows sent to DB    : {}", globalSentToDb);
        if (globalAcknowledged != globalSentToDb) {
            log.warn("|  [WARN] DB acknowledged fewer rows ({}) than sent ({}).",
                    globalAcknowledged, globalSentToDb);
            log.warn("|    Diff {} -- TDengine silently deduplicates (tbname, ts).",
                    globalSentToDb - globalAcknowledged);
        }
        log.info("|  Rows acknowledged   : {}", globalAcknowledged);

        // Post-import DB row count verification (one count per super table)
        //
        // IMPORTANT: CSV timestamps are formatted as UTC ISO-8601 (ts.toInstant().toString()),
        // e.g. "2026-05-05T16:00:00.162Z". TDengine stores these as UTC internally.
        // The configured start-time / end-time are in the local system timezone.
        // We must convert the local time range to UTC before building the COUNT SQL,
        // otherwise the WHERE clause compares local-time strings against UTC-stored data.
        try (TdConnection verifyConn = connectionFactory.create()) {
            verifyConn.execute("USE " + targetDb);
            long dbTotal = 0;

            // Pre-compute UTC time range for DB COUNT verification
            String utcStart = null;
            String utcEnd = null;
            if (properties.hasTimeRange()) {
                LocalDateTime startLdt = properties.parseStartTime();
                LocalDateTime endLdt = properties.parseEndTime();
                if (startLdt != null && endLdt != null) {
                    ZoneId localZone = ZoneId.systemDefault();
                    DateTimeFormatter utcFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
                            .withZone(ZoneId.of("UTC"));
                    utcStart = utcFmt.format(startLdt.atZone(localZone).toInstant());
                    utcEnd = utcFmt.format(endLdt.atZone(localZone).toInstant());
                    log.info("|  DB COUNT time range (UTC): {} ~ {}", utcStart, utcEnd);
                }
            }

            for (String stable : targetTables) {
                try {
                    SuperTableMeta meta = schemaFile.getSuperTables().get(stable);
                    String tsCol = getTimestampColumn(meta);
                    String countSql = "SELECT COUNT(*) FROM " + targetDb + "." + stable;
                    if (utcStart != null && utcEnd != null) {
                        countSql += " WHERE " + quoteId(tsCol) + " >= '" + utcStart + "' AND " + quoteId(tsCol) + " < '" + utcEnd + "'";
                    }
                    final String finalSql = countSql;
                    long[] result = new long[1];
                    verifyConn.query(finalSql, rs -> {
                        if (rs.next()) result[0] = rs.getLong(1);
                    });
                    dbTotal += result[0];
                    log.info("|  DB COUNT({})  : {}", stable, result[0]);
                } catch (Exception e) {
                    log.warn("|  DB COUNT({})  : query failed -- {}", stable, e.getMessage());
                }
            }
            log.info("|  DB COUNT(total)    : {}", dbTotal);
            if (dbTotal != globalSentToDb) {
                log.warn("|  [WARN] DB total ({}) != rows sent ({}). Diff: {} rows missing in DB.",
                        dbTotal, globalSentToDb, globalSentToDb - dbTotal);
            }
        } catch (Exception e) {
            log.warn("|  DB COUNT verification failed: {}", e.getMessage());
        }
        log.info("+===============================================");
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
        try {
            List<ChildTableMeta> allChildren = readChildTableManifest(manifestPath, stableName);
            if (allChildren.isEmpty()) {
                throw new RuntimeException("Child table manifest is empty: " + manifestPath);
            }

            // Filter by import conditions (TAG-based): exclude child tables that don't match
            String importCond = properties.getCombinedImportConditions(stableName);
            if (importCond != null && !importCond.isBlank()) {
                int before = allChildren.size();
                allChildren = filterChildrenByImportConditions(allChildren, meta, importCond, stableName);
                if (allChildren.isEmpty()) {
                    log.warn("  [{}] All {} child tables filtered out by import conditions: {}",
                            stableName, before, importCond);
                    return;  // nothing to create or verify for this stable
                }
                log.info("  [{}] Import condition filtered: {} → {} child tables (cond: {})",
                        stableName, before, allChildren.size(), importCond);
            }

            // Classify all children into missing/existing in one pass
            List<ChildTableMeta> missing = new ArrayList<>();
            List<ChildTableMeta> existing = new ArrayList<>();
            for (ChildTableMeta child : allChildren) {
                if (child == null || child.getTbname() == null || child.getTbname().isBlank()) {
                    continue;
                }
                if (existingChildTables.contains(child.getTbname())) {
                    existing.add(child);
                } else {
                    missing.add(child);
                }
            }

            log.info("  [{}] Manifest: {} child tables ({} missing, {} existing)",
                    stableName, allChildren.size(), missing.size(), existing.size());

            // Phase 1: Create missing tables in parallel using multiple REST connections
            int created = 0;
            if (!missing.isEmpty()) {
                ProgressCheckpoint.StableProgress sp = checkpointManager.getCheckpoint().getOrCreateStable(stableName);
                sp.setTagsVerified(false);  // new tables require re-verification
                created = createChildTablesParallel(database, stableName, meta, missing, existingChildTables);
            }

            // Phase 2: Verify existing tags (parallel, batched for memory efficiency)
            int reset = 0;
            if (!existing.isEmpty()) {
                ProgressCheckpoint.StableProgress sp = checkpointManager.getCheckpoint().getOrCreateStable(stableName);
                if (sp.isTagsVerified()) {
                    log.info("  [{}] Tags already verified from previous run, skipping {} existing table(s)",
                            stableName, existing.size());
                    for (ChildTableMeta child : existing) {
                        createdChildTables.add(stableName + ":" + child.getTbname());
                    }
                } else {
                    reset = verifyExistingChildTables(database, stableName, meta, existing, conn);
                    if (reset == 0) {
                        sp.setTagsVerified(true);
                        checkpointManager.markDirty();
                    }
                }
            }

            log.info("  [{}] Reconciled {} child table(s): created {}, reset {}",
                    stableName, missing.size() + existing.size(), created, reset);
        } catch (Exception e) {
            throw new RuntimeException("Failed to reconcile child tables for " + stableName + ": " + e.getMessage(), e);
        }
    }

    /**
     * Filter child table manifest entries by import conditions.
     * Evaluates TAG-referenced parts of conditions against manifest TAG values.
     * Child tables that don't match are tracked in {@link #filteredOutChildTables}
     * so their data rows can be skipped at insert time.
     * <p>
     * Matching strategy: by child table name. The condition is evaluated
     * against the TAG values from the manifest. Only child tables whose TAGs
     * satisfy the condition are kept. Data-column-only conditions (e.g. {@code value > 100})
     * cannot filter at manifest level — all child tables pass through, and filtering
     * relies on the export side having already applied the data-column condition.
     */
    private List<ChildTableMeta> filterChildrenByImportConditions(
            List<ChildTableMeta> children, SuperTableMeta meta,
            String conditions, String stableName) {
        // Parse condition fields
        Set<String> conditionFields = extractConditionFields(conditions);
        Set<String> tagNames = new LinkedHashSet<>();
        for (DataColumn tag : meta.getTags()) {
            tagNames.add(tag.getName().toLowerCase());
        }

        // Determine which referenced fields are TAG columns
        Set<String> referencedTags = new LinkedHashSet<>();
        boolean hasNonTagRefs = false;
        for (String field : conditionFields) {
            if (tagNames.contains(field.toLowerCase())) {
                referencedTags.add(field.toLowerCase());
            } else if (!"tbname".equalsIgnoreCase(field)) {
                hasNonTagRefs = true;
            }
        }

        if (referencedTags.isEmpty()) {
            // Pure data-column or tbname conditions — cannot filter at manifest level.
            // Data-column filtering is handled by the export side; here we keep all children.
            if (hasNonTagRefs) {
                log.info("  [{}] Import conditions reference only data columns — "
                        + "all {} child tables pass manifest filter (row-level handled at insert)",
                        stableName, children.size());
            }
            return new ArrayList<>(children);
        }

        List<ChildTableMeta> result = new ArrayList<>();
        for (ChildTableMeta child : children) {
            if (child == null || child.getTbname() == null) continue;
            if (evaluateConditionsOnTags(child, meta, conditions, referencedTags)) {
                result.add(child);
            } else {
                filteredOutChildTables.add(stableName + ":" + child.getTbname());
            }
        }
        return result;
    }

    /**
     * Evaluate SQL-like WHERE conditions against a child table's TAG values.
     * Only TAG-referenced parts are evaluated; data column references are treated
     * as always-true (they are handled by the export-side filtering).
     * <p>
     * Supports: =, !=, IN, AND, OR, IS NULL, IS NOT NULL.
     */
    private boolean evaluateConditionsOnTags(ChildTableMeta child, SuperTableMeta meta,
                                              String conditions, Set<String> referencedTags) {
        String evaluable = conditions;

        // Replace tag name references with their literal values
        for (DataColumn tag : meta.getTags()) {
            String tagName = tag.getName();
            if (!referencedTags.contains(tagName.toLowerCase())) continue;
            String val = child.getTagValues() != null ? child.getTagValues().get(tagName) : null;
            // Replace bare and backtick-quoted tag name references with quoted literal.
            // \b ensures we don't match substrings (e.g. "dev" won't match "device")
            String replacement = val != null
                    ? "'" + val.replace("\\", "\\\\").replace("'", "''") + "'"
                    : "NULL";
            evaluable = evaluable.replaceAll("(?i)\\b`?" + java.util.regex.Pattern.quote(tagName) + "`?\\b",
                    java.util.regex.Matcher.quoteReplacement(replacement));
        }

        // Remove any remaining identifiers (data column refs) by replacing them with TRUE,
        // and remove dangling AND/OR operators around them (simple heuristic)
        // Actually, we use a simpler strategy: evaluate with a basic expression parser
        return evaluateSimpleCondition(evaluable);
    }

    /**
     * Evaluate a simplified WHERE expression where all identifiers have been replaced
     * with literal values. Supports: =, !=, <>, IN (...), IS NULL, IS NOT NULL,
     * AND, OR, parentheses.
     */
    private boolean evaluateSimpleCondition(String expr) {
        if (expr == null || expr.isBlank()) return true;
        try {
            return evaluateOr(expr.trim());
        } catch (Exception e) {
            log.debug("  Condition evaluation failed for '{}': {} — defaulting to include", expr, e.getMessage());
            return true;  // on parse failure, include the row (conservative)
        }
    }

    private boolean evaluateOr(String expr) {
        // Split by top-level OR
        List<String> parts = splitTopLevel(expr, "OR");
        for (String part : parts) {
            if (evaluateAnd(part.trim())) return true;
        }
        return parts.isEmpty();
    }

    private boolean evaluateAnd(String expr) {
        List<String> parts = splitTopLevel(expr, "AND");
        for (String part : parts) {
            if (!evaluateAtom(part.trim())) return false;
        }
        return true;
    }

    private boolean evaluateAtom(String expr) {
        if (expr.isEmpty()) return true;
        if (expr.equalsIgnoreCase("TRUE")) return true;
        if (expr.equalsIgnoreCase("FALSE")) return false;

        // Strip outer parentheses
        String trimmed = expr.trim();
        while (trimmed.startsWith("(") && trimmed.endsWith(")")) {
            // Check if parens are balanced (not like "(a) OR (b)")
            int depth = 0;
            boolean balanced = true;
            for (int i = 0; i < trimmed.length() - 1; i++) {
                if (trimmed.charAt(i) == '(') depth++;
                else if (trimmed.charAt(i) == ')') depth--;
                if (depth == 0) { balanced = false; break; }
            }
            if (balanced && depth == 1) {
                trimmed = trimmed.substring(1, trimmed.length() - 1).trim();
            } else {
                break;
            }
        }

        // IS NULL / IS NOT NULL
        if (trimmed.toUpperCase().endsWith(" IS NOT NULL")) {
            String left = trimmed.substring(0, trimmed.length() - 11).trim();
            return !isNullLiteral(left);
        }
        if (trimmed.toUpperCase().endsWith(" IS NULL")) {
            String left = trimmed.substring(0, trimmed.length() - 8).trim();
            return isNullLiteral(left);
        }

        // NOT IN (v1, v2, ...) — MUST be checked before IN,
        // otherwise "IN" matches the "IN" inside "NOT IN"
        int notInIdx = findOperatorIndex(trimmed, "NOT IN");
        if (notInIdx >= 0) {
            String left = trimmed.substring(0, notInIdx).trim();
            String right = trimmed.substring(notInIdx + 6).trim();
            if (right.startsWith("(") && right.endsWith(")")) {
                right = right.substring(1, right.length() - 1);
                String[] values = right.split(",");
                for (String v : values) {
                    if (valuesEqual(left.trim(), v.trim().replaceAll("^'|'$", ""))) return false;
                }
                return true;
            }
        }

        // IN (v1, v2, ...)
        int inIdx = findOperatorIndex(trimmed, "IN");
        if (inIdx >= 0) {
            String left = trimmed.substring(0, inIdx).trim();
            String right = trimmed.substring(inIdx + 2).trim();
            if (right.startsWith("(") && right.endsWith(")")) {
                right = right.substring(1, right.length() - 1);
                String[] values = right.split(",");
                for (String v : values) {
                    if (valuesEqual(left.trim(), v.trim().replaceAll("^'|'$", ""))) return true;
                }
                return false;
            }
        }

        // != / <>
        int neIdx = findOperatorIndex(trimmed, "!=");
        if (neIdx < 0) neIdx = findOperatorIndex(trimmed, "<>");
        if (neIdx >= 0) {
            String left = trimmed.substring(0, neIdx).trim();
            String right = trimmed.substring(neIdx + 2).trim();
            return !valuesEqual(left, unquote(right));
        }

        // =
        int eqIdx = findOperatorIndex(trimmed, "=");
        if (eqIdx >= 0) {
            String left = trimmed.substring(0, eqIdx).trim();
            String right = trimmed.substring(eqIdx + 1).trim();
            return valuesEqual(left, unquote(right));
        }

        // Fallback: if it's a recursive OR/AND, try again
        if (trimmed.toUpperCase().contains(" OR ")) return evaluateOr(trimmed);
        if (trimmed.toUpperCase().contains(" AND ")) return evaluateAnd(trimmed);

        return true;
    }

    private List<String> splitTopLevel(String expr, String op) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        int lastEnd = 0;
        String upper = expr.toUpperCase();
        String upperOp = op.toUpperCase();
        int i = 0;
        while (i < expr.length()) {
            char c = expr.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == '\'' || c == '"') {
                // Skip quoted string
                char q = c;
                i++;
                while (i < expr.length() && expr.charAt(i) != q) i++;
            } else if (depth == 0 && i + op.length() <= expr.length()) {
                boolean match = upper.startsWith(upperOp, i);
                // Ensure word boundaries: before and after the operator
                boolean beforeOk = i == 0 || !Character.isLetterOrDigit(expr.charAt(i - 1));
                int after = i + op.length();
                boolean afterOk = after >= expr.length() || !Character.isLetterOrDigit(expr.charAt(after));
                if (match && beforeOk && afterOk) {
                    parts.add(expr.substring(lastEnd, i).trim());
                    i += op.length();
                    lastEnd = i;
                    continue;
                }
            }
            i++;
        }
        parts.add(expr.substring(lastEnd).trim());
        return parts;
    }

    private int findOperatorIndex(String expr, String op) {
        int depth = 0;
        String upper = expr.toUpperCase();
        String upperOp = op.toUpperCase();
        for (int i = 0; i < expr.length() - op.length() + 1; i++) {
            char c = expr.charAt(i);
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == '\'' || c == '"') {
                char q = c;
                i++;
                while (i < expr.length() && expr.charAt(i) != q) i++;
            } else if (depth == 0) {
                boolean beforeOk = i == 0 || !Character.isLetterOrDigit(expr.charAt(i - 1));
                int after = i + op.length();
                boolean afterOk = after >= expr.length() || !Character.isLetterOrDigit(expr.charAt(after));
                if (upper.startsWith(upperOp, i) && beforeOk && afterOk) {
                    return i;
                }
            }
        }
        return -1;
    }

    private boolean valuesEqual(String a, String b) {
        String na = unquote(a.trim());
        String nb = unquote(b.trim());
        if (isNullLiteral(na) && isNullLiteral(nb)) return true;
        if (isNullLiteral(na) || isNullLiteral(nb)) return false;
        // Numeric comparison: try parse as numbers
        try {
            return Double.compare(Double.parseDouble(na), Double.parseDouble(nb)) == 0;
        } catch (NumberFormatException ignored) {}
        return na.equals(nb);
    }

    private String unquote(String s) {
        if (s == null) return null;
        String t = s.trim();
        if (t.startsWith("'") && t.endsWith("'")) {
            // Strip outer quotes and unescape SQL-style '' → '
            return t.substring(1, t.length() - 1).replace("''", "'");
        }
        if (t.startsWith("\"") && t.endsWith("\"")) {
            return t.substring(1, t.length() - 1);
        }
        return t;
    }

    private boolean isNullLiteral(String s) {
        return s == null || s.equalsIgnoreCase("NULL") || s.equalsIgnoreCase("\\N");
    }

    /**
     * Extract field/column names referenced in a SQL WHERE condition.
     */
    private Set<String> extractConditionFields(String condition) {
        Set<String> fields = new LinkedHashSet<>();
        if (condition == null || condition.isBlank()) return fields;

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
            if (token.isBlank()) continue;
            if (token.startsWith("'") || token.startsWith("\"")) continue;
            if (token.matches("(?i)and|or|not|in|like|is|null|between|exists|true|false")) continue;
            if (token.matches("[0-9.]+")) continue;
            fields.add(token.replace("`", ""));
        }
        return fields;
    }

    /**
     * Create child tables in parallel using multiple REST API connections.
     * Each thread creates tables from a sublist and reports results via AtomicInteger.
     */
    private int createChildTablesParallel(String database, String stableName, SuperTableMeta meta,
                                          List<ChildTableMeta> missing, Set<String> existingChildTables) throws Exception {
        int threadCount = Math.min(properties.getParallel(), missing.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount,
                r -> { Thread t = new Thread(r, "ddl-" + stableName); t.setDaemon(true); return t; });
        AtomicInteger created = new AtomicInteger(0);
        List<Future<Void>> futures = new ArrayList<>();

        // Distribute missing tables evenly across threads
        int chunkSize = Math.max(1, (missing.size() + threadCount - 1) / threadCount);
        for (int start = 0; start < missing.size(); start += chunkSize) {
            int end = Math.min(start + chunkSize, missing.size());
            List<ChildTableMeta> chunk = missing.subList(start, end);
            futures.add(executor.submit(() -> {
                try (TdConnection createConn = connectionFactory.create()) {
                    createConn.execute("USE " + database);
                    for (ChildTableMeta child : chunk) {
                        createConn.execute(buildCreateChildTableSql(stableName, child, meta));
                        String key = stableName + ":" + child.getTbname();
                        createdChildTables.add(key);
                        // existingChildTables is only modified during creation; safe to synchronize on it
                        synchronized (existingChildTables) {
                            existingChildTables.add(child.getTbname());
                        }
                        created.incrementAndGet();
                    }
                }
                return null;
            }));
        }
        executor.shutdown();

        // Collect errors
        List<Exception> errors = new ArrayList<>();
        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception ex) {
                    errors.add(ex);
                }
            }
        }

        if (!errors.isEmpty()) {
            for (Exception err : errors) {
                log.error("  [{}] Parallel create error: {}", stableName, err.getMessage());
            }
            Exception first = errors.getFirst();
            throw new RuntimeException("Parallel child table creation failed: " + errors.size()
                    + " thread(s) had errors. First: " + first.getMessage(), first);
        }

        return created.get();
    }

    /**
     * Verify existing child tables tag values and reset mismatched ones.
     * Batches are processed in parallel using multiple REST connections.
     */
    private int verifyExistingChildTables(String database, String stableName, SuperTableMeta meta,
                                          List<ChildTableMeta> existing, TdConnection conn) throws Exception {
        // Pre-compute all batches
        List<List<ChildTableMeta>> batches = new ArrayList<>();
        List<ChildTableMeta> batch = new ArrayList<>(properties.getChildTableBatchSize());
        for (ChildTableMeta child : existing) {
            batch.add(child);
            if (batch.size() >= properties.getChildTableBatchSize()) {
                batches.add(batch);
                batch = new ArrayList<>(properties.getChildTableBatchSize());
            }
        }
        if (!batch.isEmpty()) {
            batches.add(batch);
        }

        if (batches.size() <= 1) {
            int reset = 0;
            for (List<ChildTableMeta> b : batches) {
                reset += verifyChildTableTagBatch(database, stableName, meta, b, conn);
            }
            return reset;
        }

        // Parallel execution: each thread uses its own REST connection
        int threadCount = Math.min(properties.getParallel(), batches.size());
        ExecutorService executor = Executors.newFixedThreadPool(threadCount, r -> {
            Thread t = new Thread(r, "tag-verify-" + stableName);
            t.setDaemon(true);
            return t;
        });

        AtomicInteger totalReset = new AtomicInteger(0);
        List<Future<Void>> futures = new ArrayList<>();

        for (List<ChildTableMeta> taskBatch : batches) {
            futures.add(executor.submit(() -> {
                try (TdConnection taskConn = connectionFactory.create()) {
                    taskConn.execute("USE " + database);
                    int reset = verifyChildTableTagBatch(database, stableName, meta, taskBatch, taskConn);
                    totalReset.addAndGet(reset);
                }
                return null;
            }));
        }
        executor.shutdown();

        // Collect errors
        List<Exception> errors = new ArrayList<>();
        for (Future<Void> f : futures) {
            try {
                f.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof Exception ex) {
                    errors.add(ex);
                }
            }
        }

        if (!errors.isEmpty()) {
            for (Exception err : errors) {
                log.error("  [{}] Parallel tag verify error: {}", stableName, err.getMessage());
            }
            Exception first = errors.get(0);
            throw new RuntimeException("Parallel tag verification failed: " + errors.size()
                    + " thread(s) had errors. First: " + first.getMessage(), first);
        }

        return totalReset.get();
    }

    private int verifyChildTableTagBatch(String database, String stableName, SuperTableMeta meta,
                                         List<ChildTableMeta> batch, TdConnection conn) throws Exception {
        Map<String, LinkedHashMap<String, String>> currentTags = queryChildTableTags(database, stableName, meta, batch, conn);
        List<ChildTableMeta> needReset = new ArrayList<>();

        for (ChildTableMeta child : batch) {
            LinkedHashMap<String, String> current = currentTags.get(child.getTbname());
            if (!tagsEqual(meta, current, child.getTagValues())) {
                needReset.add(child);
            }
        }

        if (!needReset.isEmpty()) {
            // Batch DDL: join ALTER TABLE statements with semicolons
            // to reduce REST API round-trips. Fall back to individual execution
            // if the batch fails (e.g., connection-specific limitation).
            StringBuilder batchSql = new StringBuilder();
            for (ChildTableMeta child : needReset) {
                batchSql.append(buildAlterChildTableTagSql(child, meta)).append(";");
            }
            try {
                conn.execute(batchSql.toString());
            } catch (Exception batchEx) {
                log.debug("  [{}] Batch ALTER TABLE failed, falling back to individual: {}",
                        stableName, batchEx.getMessage());
                for (ChildTableMeta child : needReset) {
                    conn.execute(buildAlterChildTableTagSql(child, meta));
                }
            }
            for (ChildTableMeta child : needReset) {
                createdChildTables.add(stableName + ":" + child.getTbname());
            }
        } else {
            for (ChildTableMeta child : batch) {
                createdChildTables.add(stableName + ":" + child.getTbname());
            }
        }

        return needReset.size();
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
            String cur = current.get(name);
            String exp = expected.get(name);
            if (!normalizedEquals(tag.getType(), cur, exp)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compare two tag values with type-aware normalization, so that different
     * string representations of the same logical value are treated as equal.
     * <p>
     * The manifest (expected) values come from JSON deserialization.
     * The current values come from {@code rs.getObject().toString()} over REST.
     * These two paths can produce different strings for the same value, e.g.:
     * TIMESTAMP: {@code "2026-05-05T00:00:00"} vs {@code "2026-05-05 00:00:00.0"}
     * DOUBLE:    {@code "1.0"} vs {@code "1"}
     * Without normalization, every existing child table would be ALTER TABLE'd
     * on every re-import, even though the tags are already correct.
     */
    private boolean normalizedEquals(String type, String a, String b) {
        if (isNullish(a) && isNullish(b)) return true;
        if (isNullish(a) || isNullish(b)) return false;

        String ut = type.toUpperCase();

        if (isNumericType(ut)) {
            return normalizedNumericEquals(a, b);
        }
        if (ut.equals("TIMESTAMP")) {
            return normalizedTimestampEquals(a, b);
        }
        if (ut.equals("BOOL") || ut.equals("BOOLEAN")) {
            return a.equalsIgnoreCase(b);
        }
        // String types: NCHAR, BINARY, VARCHAR, etc.
        return Objects.equals(a, b);
    }

    private boolean isNullish(String s) {
        return s == null || s.isEmpty() || "null".equalsIgnoreCase(s) || NULL_MARKER.equals(s);
    }

    private boolean isNumericType(String type) {
        return type.equals("TINYINT") || type.startsWith("TINYINT UNSIGNED")
                || type.equals("SMALLINT") || type.startsWith("SMALLINT UNSIGNED")
                || type.equals("INT") || type.startsWith("INT UNSIGNED")
                || type.equals("BIGINT") || type.startsWith("BIGINT UNSIGNED")
                || type.equals("FLOAT") || type.equals("DOUBLE")
                || type.startsWith("DECIMAL") || type.startsWith("NUMBER");
    }

    private boolean normalizedNumericEquals(String a, String b) {
        try {
            BigDecimal da = new BigDecimal(a.trim());
            BigDecimal db = new BigDecimal(b.trim());
            return da.compareTo(db) == 0;
        } catch (NumberFormatException e) {
            return Objects.equals(a, b);
        }
    }

    private boolean normalizedTimestampEquals(String a, String b) {
        // Canonicalize: replace T with space, strip trailing .0 / .00 / .000 etc.
        String na = a.trim().replace('T', ' ').replaceAll("\\.0+$", "");
        String nb = b.trim().replace('T', ' ').replaceAll("\\.0+$", "");
        return Objects.equals(na, nb);
    }

    private String buildCreateChildTableSql(String stableName, ChildTableMeta child, SuperTableMeta meta) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE IF NOT EXISTS `").append(child.getTbname()).append('`')
                .append(" USING `").append(stableName).append("` (");
        appendTagNames(sql, meta);
        sql.append(") TAGS (");
        appendTagValues(sql, meta, child.getTagValues());
        sql.append(')');
        return sql.toString();
    }

    private void appendTagNames(StringBuilder sql, SuperTableMeta meta) {
        boolean first = true;
        for (DataColumn tag : meta.getTags()) {
            if (!first) sql.append(", ");
            sql.append('`').append(tag.getName()).append('`');
            first = false;
        }
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

    /**
     * Validate import conditions against the schema file's column/tag definitions.
     * This runs before any DB connection is created, so it doesn't require
     * the target database or super tables to exist.
     */
    private void validateImportConditions(SchemaFile schemaFile) {
        Map<String, String> importStableConditions = properties.getImportStableConditions();
        String importConditions = properties.getImportConditions();
        if ((importStableConditions == null || importStableConditions.isEmpty())
                && (importConditions == null || importConditions.isBlank())) {
            return;
        }

        // Collect stable names from per-table conditions + configured list
        Set<String> stableNames = new LinkedHashSet<>();
        if (importStableConditions != null) {
            stableNames.addAll(importStableConditions.keySet());
        }
        if (stableNames.isEmpty() && importConditions != null && !importConditions.isBlank()) {
            List<String> configured = properties.getSuperTables();
            if (configured != null && !configured.isEmpty()) {
                stableNames.addAll(configured);
            } else {
                // Validate against all super tables in the schema file
                stableNames.addAll(schemaFile.getSuperTables().keySet());
            }
        }

        for (String stableName : stableNames) {
            SuperTableMeta meta = schemaFile.getSuperTables().get(stableName);
            if (meta == null) {
                log.warn("  Import condition references unknown super table '{}' — skipping validation", stableName);
                continue;
            }
            Set<String> validColumns = new LinkedHashSet<>();
            meta.getColumns().forEach(c -> validColumns.add(c.getName().toLowerCase()));
            meta.getTags().forEach(t -> validColumns.add(t.getName().toLowerCase()));

            // Validate per-table condition
            String perTableCond = importStableConditions != null
                    ? importStableConditions.getOrDefault(stableName, null) : null;
            if (perTableCond != null && !perTableCond.isBlank()) {
                Set<String> referenced = extractConditionFields(perTableCond);
                for (String field : referenced) {
                    if (!validColumns.contains(field.toLowerCase())) {
                        throw new IllegalArgumentException(
                                "Invalid import-stable-conditions field '" + field + "' for "
                                        + stableName + ". Valid columns/tags: " + validColumns);
                    }
                }
            }

            // Validate common condition against this super table
            if (importConditions != null && !importConditions.isBlank()) {
                Set<String> referenced = extractConditionFields(importConditions);
                for (String field : referenced) {
                    if (!validColumns.contains(field.toLowerCase())) {
                        throw new IllegalArgumentException(
                                "Invalid import-conditions field '" + field + "' for "
                                        + stableName + ". Valid columns/tags: " + validColumns);
                    }
                }
            }
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
        AtomicLong totalRead = new AtomicLong(0);
        AtomicLong totalFiltered = new AtomicLong(0);
        AtomicLong totalInsertedThisRun = new AtomicLong(0);
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
                            totalInsertedThisRun.addAndGet(inserted);
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

                    // Skip entire file if child table was filtered out by import conditions
                    if (ctx.currentFileChildTable != null
                            && filteredOutChildTables.contains(stableName + ":" + ctx.currentFileChildTable)) {
                        log.info("  [{}] Skipping file {} (child table {} filtered by import conditions)",
                                stableName, fileKey, ctx.currentFileChildTable);
                        if (sp != null) {
                            sp.markFileCompleted(fileKey, 0);
                            checkpointManager.markDirty();
                        }
                        continue;
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
                long fileFiltered = ctx.filteredInBatch.getAndSet(0);
                totalRead.addAndGet(fileRecordCount);
                totalFiltered.addAndGet(fileFiltered);
                log.info("  [{}] File {} finished: read {} rows, inserted {} rows, filtered {} rows",
                        stableName, fileKey, fileRecordCount, insertedRows, fileFiltered);
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
        long read = totalRead.get();
        long filtered = totalFiltered.get();
        long sentToDb = read - filtered;
        long acknowledgedByDb = totalInsertedThisRun.get();
        long totalIncludingCheckpoint = totalRecords.get();
        globalTotalRead.addAndGet(read);
        globalTotalFiltered.addAndGet(filtered);
        globalTotalInserted.addAndGet(acknowledgedByDb);
        log.info("  +---------------------------------------------");
        log.info("  | [{}] Import Summary", stableName);
        log.info("  |   Rows read from data files : {}", read);
        if (filtered > 0) {
            log.info("  |   Rows filtered by import cond: {}", filtered);
        }
        log.info("  |   Rows sent to DB            : {}", sentToDb);
        if (acknowledgedByDb != sentToDb) {
            log.warn("  |   [WARN] DB acknowledged fewer rows ({}) than sent ({}) -- TDengine may have deduplicated {}",
                    acknowledgedByDb, sentToDb, sentToDb - acknowledgedByDb);
        }
        log.info("  |   Rows acknowledged by DB    : {}", acknowledgedByDb);
        if (totalIncludingCheckpoint != acknowledgedByDb) {
            log.info("  |   Total incl. checkpoint     : {}", totalIncludingCheckpoint);
        }
        log.info("  +---------------------------------------------");
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
        /** Rows filtered out by import conditions in the current file (reset per file). */
        final AtomicLong filteredInBatch = new AtomicLong(0);

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
                // Quick date-level skip for time range filtering
                if (isDateDirOutsideRange(dateDir.getFileName().toString())) {
                    continue;
                }
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
            // Quick date-level skip for time range filtering
            if (isDateDirOutsideRange(dateDir.getFileName().toString())) {
                continue;
            }
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

        if (properties.hasTimeRange() && !files.isEmpty()) {
            files = filterByTimeRange(files, dataDir);
        }

        return files;
    }

    /**
     * Filter data files to only those within the configured time range [startTime, endTime).
     * Uses directory structure (yyyyMMdd) and filename timestamp (HHmmssSSS) for fast
     * identification without reading file contents.
     */
    private List<Path> filterByTimeRange(List<Path> files, Path dataDir) {
        LocalDateTime start = properties.parseStartTime();
        LocalDateTime end = properties.parseEndTime();

        List<Path> filtered = new ArrayList<>();
        int skipped = 0;

        for (Path file : files) {
            if (!isFileOutsideRange(file, dataDir, start, end)) {
                filtered.add(file);
            } else {
                skipped++;
            }
        }

        if (skipped > 0) {
            log.info("  Filtered out {} file(s) outside time range, {} file(s) to import",
                    skipped, filtered.size());
        }

        return filtered;
    }

    /**
     * Determine if a data file is outside the configured time range.
     * Extracts date from directory name and time from file name without reading contents.
     *
     * <p>Path formats supported:
     * <ul>
     *   <li>New: {@code {stableName}/{yyyyMMdd}/slice_{HHmmss}_{idx}/{stableName}_{HHmmssSSS}.gz}</li>
     *   <li>Old: {@code {yyyyMMdd}/{stableName}_{HHmmssSSS}.gz}</li>
     *   <li>Flat: {@code {stableName}_{HHmmssSSS}.gz} — no date dir, kept (conservative)</li>
     * </ul>
     */
    private boolean isFileOutsideRange(Path file, Path dataDir, LocalDateTime start, LocalDateTime end) {
        if (start == null && end == null) return false;

        Path relative = dataDir.relativize(file);
        int count = relative.getNameCount();
        String fileName = relative.getFileName().toString();

        LocalDate fileDate = null;
        LocalTime fileTime = null;

        if (count >= 4) {
            // New format: stableName/yyyyMMdd/slice_HHmmss_index/stableName_HHmmssSSS.gz
            String dateStr = relative.getName(1).toString();
            if (dateStr.matches("\\d{8}")) {
                try {
                    fileDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                } catch (DateTimeParseException ignored) {}
            }
            fileTime = extractTimeFromFileName(fileName);
        } else if (count >= 2) {
            // Old format: yyyyMMdd/stableName_HHmmssSSS.gz
            String dateStr = relative.getName(0).toString();
            if (dateStr.matches("\\d{8}")) {
                try {
                    fileDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                } catch (DateTimeParseException ignored) {}
            }
            fileTime = extractTimeFromFileName(fileName);
        }
        // Flat format: no date directory → can't determine date, include file

        if (fileDate == null) {
            return false;
        }

        // Quick date-level check
        if (start != null && fileDate.isBefore(start.toLocalDate())) return true;
        if (end != null && fileDate.isAfter(end.toLocalDate())) return true;

        // For boundary dates (first/last day of range), use time-level precision
        // when the filename contains a parsable HHmmssSSS timestamp.
        if (fileTime == null) return false;

        boolean onStartDay = start != null && fileDate.equals(start.toLocalDate());
        boolean onEndDay = end != null && fileDate.equals(end.toLocalDate());

        if (onStartDay || onEndDay) {
            LocalDateTime fileDateTime = fileDate.atTime(fileTime);
            if (onStartDay && fileDateTime.isBefore(start)) return true;
            if (onEndDay && !fileDateTime.isBefore(end)) return true;
        }

        return false;
    }

    /**
     * Extract HHmmssSSS (9 digits) from filename like {@code stable_history_data_144501158.gz}
     * or {@code stable_history_data_144501158_2.gz} (with file rotation index).
     *
     * @return parsed LocalTime or null if no valid timestamp found
     */
    private LocalTime extractTimeFromFileName(String fileName) {
        Pattern p = Pattern.compile("_(\\d{9})(?:_\\d+)?\\.gz$");
        Matcher m = p.matcher(fileName);
        if (m.find()) {
            String timeStr = m.group(1);
            try {
                int h = Integer.parseInt(timeStr.substring(0, 2));
                int mi = Integer.parseInt(timeStr.substring(2, 4));
                int s = Integer.parseInt(timeStr.substring(4, 6));
                int ms = Integer.parseInt(timeStr.substring(6, 9));
                return LocalTime.of(h, mi, s, ms * 1_000_000);
            } catch (Exception ignored) {}
        }
        return null;
    }

    /**
     * Quick pure-date check: is a yyyyMMdd date directory completely outside
     * the configured time range? Used for fast directory-level skipping before
     * scanning files within a date directory.
     */
    private boolean isDateDirOutsideRange(String dateStr) {
        if (!properties.hasTimeRange()) return false;
        try {
            LocalDate dirDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDateTime start = properties.parseStartTime();
            LocalDateTime end = properties.parseEndTime();
            if (start != null && dirDate.isBefore(start.toLocalDate())) return true;
            if (end != null && dirDate.isAfter(end.toLocalDate())) return true;
        } catch (DateTimeParseException ignored) {}
        return false;
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

            // All child tables already created during reconciliation — no USING TAGS needed

            int actualInserted = 0;
            for (Map.Entry<String, List<String>> entry : grouped.entrySet()) {
                String tbname = entry.getKey();
                List<String> tableRecords = entry.getValue();
                if (tableRecords.isEmpty()) continue;

                // Skip child tables filtered out by import conditions
                if (filteredOutChildTables.contains(stableName + ":" + tbname)) {
                    ctx.filteredInBatch.addAndGet(tableRecords.size());
                    continue;
                }

                // Split into chunks to avoid TDengine SQL length / row-count limits
                for (int chunkStart = 0; chunkStart < tableRecords.size(); chunkStart += MAX_VALUES_PER_INSERT) {
                    int chunkEnd = Math.min(chunkStart + MAX_VALUES_PER_INSERT, tableRecords.size());
                    int chunkSize = chunkEnd - chunkStart;

                    StringBuilder sql = new StringBuilder(chunkSize * 96 + 64);
                    sql.append("INSERT INTO `").append(tbname).append("` VALUES ");

                    for (int i = chunkStart; i < chunkEnd; i++) {
                        if (i > chunkStart) sql.append(' ');

                        if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                            appendCsvValues(sql, tableRecords.get(i), ctx, meta);
                        } else {
                            appendJsonValues(sql, tableRecords.get(i), tagColumnNames);
                        }
                    }

                    try {
                        int affected = conn.executeUpdate(sql.toString());
                        if (affected != chunkSize) {
                            log.warn("INSERT row count mismatch for {}, child table {}: expected={}, actual={}",
                                    stableName, tbname, chunkSize, affected);
                        }
                        actualInserted += affected;
                    } catch (Exception e) {
                        log.error("Batch insert FAILED for super table {}, child table {} (chunk {}..{}):\n  SQL (first 2000 chars): {}\n  Error: {}",
                                stableName, tbname, chunkStart, chunkEnd - 1,
                                sql.substring(0, Math.min(sql.length(), 2000)), e.getMessage());
                        throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
                    }
                }
            }
            return actualInserted;
        } catch (Exception e) {
            log.error("Batch insert failed for super table {}: {}", stableName, e.getMessage());
            throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
        }
    }

    /** Max value tuples per INSERT to avoid TDengine SQL length / row-count limits. */
    private static final int MAX_VALUES_PER_INSERT = 200;

    /**
     * Insert a batch of records into a known child table (per-child-table format).
     * All lines in the batch belong to one child table, values are data columns only.
     * Splits large batches into multiple INSERT statements to stay within TDengine limits.
     * The child table is already created by the manifest, so no USING TAGS needed.
     */
    private int insertBatchToChildTable(String stableName, SuperTableMeta meta, List<String> batchLines,
                                          TdConnection conn, PipelineCtx ctx) {
        String childTable = ctx.currentFileChildTable;
        int totalInserted = 0;

        for (int start = 0; start < batchLines.size(); start += MAX_VALUES_PER_INSERT) {
            int end = Math.min(start + MAX_VALUES_PER_INSERT, batchLines.size());
            int chunkSize = end - start;

            StringBuilder sql = new StringBuilder(chunkSize * 96 + 64);
            sql.append("INSERT INTO `").append(childTable).append("` VALUES ");

            for (int i = start; i < end; i++) {
                if (i > start) sql.append(' ');
                if (properties.getFormat() == SyncProperties.DataFormat.CSV) {
                    appendCsvValuesForChildTable(sql, batchLines.get(i), ctx, meta);
                } else {
                    // JSON: exclude tbname and tag_ fields
                    appendJsonValuesForChildTable(sql, batchLines.get(i));
                }
            }

            try {
                int affected = conn.executeUpdate(sql.toString());
                if (affected != chunkSize) {
                    log.warn("INSERT row count mismatch for {}, child table {} (chunk {}..{}): expected={}, actual={}",
                            stableName, childTable, start, end - 1, chunkSize, affected);
                }
                totalInserted += affected;
            } catch (Exception e) {
                log.error("Batch insert FAILED for child table {} (chunk {}..{}):\n  SQL (first 2000 chars): {}\n  Error: {}",
                        childTable, start, end - 1,
                        sql.substring(0, Math.min(sql.length(), 2000)), e.getMessage());
                throw new RuntimeException("Batch insert failed: " + e.getMessage(), e);
            }
        }
        return totalInserted;
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
        boolean hasExp = false;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '-' && i == 0) continue;
            if (c == '.' && !hasDot && !hasExp) { hasDot = true; continue; }
            if ((c == 'e' || c == 'E') && hasDigit && !hasExp) { hasExp = true; continue; }
            if (c == '+' && hasExp && i > 0
                    && (value.charAt(i - 1) == 'e' || value.charAt(i - 1) == 'E')) continue;
            if (Character.isDigit(c)) { hasDigit = true; continue; }
            return false;
        }
        // Must contain at least one digit AND must end with a digit (not e/E/+/-/.)
        return hasDigit && Character.isDigit(value.charAt(value.length() - 1));
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
            if (tagsIdx < 0) {
                // No TAGS clause — inject IF NOT EXISTS and return
                return injectIfNotExists(createStmt.trim());
            }
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
                    return injectIfNotExists(createStmt.substring(0, i + 1).trim());
                }
            }
        }

        // Fallback: if we can't parse properly, just trim and inject IF NOT EXISTS
        return injectIfNotExists(createStmt.trim());
    }

    /**
     * Inject IF NOT EXISTS into a CREATE STABLE statement if not already present.
     */
    private static String injectIfNotExists(String ddl) {
        if (ddl == null || ddl.isBlank()) return ddl;
        if (ddl.toUpperCase().contains("IF NOT EXISTS")) return ddl;
        return ddl.replaceFirst("(?i)CREATE\\s+STABLE\\s+", "CREATE STABLE IF NOT EXISTS ");
    }

    private static String getTimestampColumn(SuperTableMeta meta) {
        if (meta == null || meta.getColumns() == null) return "ts";
        for (DataColumn col : meta.getColumns()) {
            if ("TIMESTAMP".equalsIgnoreCase(col.getType())) {
                return col.getName();
            }
        }
        return "ts";
    }

    private static String quoteId(String id) {
        return "`" + id + "`";
    }
}
