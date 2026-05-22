package com.tdengine.dbsync.importer;

/**
 * Data importer interface.
 */
public interface DataImporter {

    /**
     * Execute the import operation.
     */
    void importData() throws Exception;
}
