package com.tdengine.dbsync.exporter;

/**
 * Data exporter interface.
 */
public interface DataExporter {

    /**
     * Execute the export operation.
     */
    void exportData() throws Exception;
}
