package com.pascalming.tdenginedbsync;

import java.nio.file.Path;

interface TdengineDataTransferService {

    void exportData(String database, String table, Path filePath);

    void importData(String database, String table, Path filePath, int batchSize);
}
