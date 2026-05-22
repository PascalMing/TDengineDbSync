package com.pascalming.tdenginedbsync;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Service
class JdbcTdengineDataTransferService implements TdengineDataTransferService {

    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z0-9_]+");

    private final DataSource dataSource;

    JdbcTdengineDataTransferService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public void exportData(String database, String table, Path filePath) {
        Path parent = filePath.getParent();
        if (parent != null) {
            try {
                Files.createDirectories(parent);
            } catch (IOException ex) {
                throw new IllegalStateException("Failed to create export directory: " + parent, ex);
            }
        }

        String query = "SELECT * FROM " + qualifiedTable(database, table);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query);
             var writer = Files.newBufferedWriter(filePath)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            List<String> headers = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                headers.add(metaData.getColumnName(i));
            }

            try (CSVPrinter printer = CSVFormat.DEFAULT.builder().setHeader(headers.toArray(String[]::new)).build().print(writer)) {
                while (resultSet.next()) {
                    List<Object> row = new ArrayList<>(columnCount);
                    for (int i = 1; i <= columnCount; i++) {
                        row.add(resultSet.getObject(i));
                    }
                    printer.printRecord(row);
                }
            }
        } catch (SQLException | IOException ex) {
            throw new IllegalStateException("Failed to export data from " + database + "." + table, ex);
        }
    }

    @Override
    public void importData(String database, String table, Path filePath, int batchSize) {
        if (!Files.exists(filePath)) {
            throw new IllegalArgumentException("Import file does not exist: " + filePath);
        }

        String qualifiedTable = qualifiedTable(database, table);

        try (var reader = Files.newBufferedReader(filePath);
             CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build().parse(reader);
             Connection connection = dataSource.getConnection()) {

            Map<String, Integer> headerMap = parser.getHeaderMap();
            if (headerMap == null || headerMap.isEmpty()) {
                throw new IllegalArgumentException("CSV file has no header columns: " + filePath);
            }

            List<String> columns = headerMap.keySet().stream().toList();
            String sql = buildInsertSql(qualifiedTable, columns);

            try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                int currentBatchCount = 0;
                for (CSVRecord record : parser) {
                    for (int i = 0; i < columns.size(); i++) {
                        preparedStatement.setString(i + 1, record.get(i));
                    }
                    preparedStatement.addBatch();
                    currentBatchCount++;

                    if (currentBatchCount == batchSize) {
                        preparedStatement.executeBatch();
                        currentBatchCount = 0;
                    }
                }

                if (currentBatchCount > 0) {
                    preparedStatement.executeBatch();
                }
            }

        } catch (SQLException | IOException ex) {
            throw new IllegalStateException("Failed to import data into " + database + "." + table, ex);
        }
    }

    private String buildInsertSql(String qualifiedTable, List<String> columns) {
        String columnNames = columns.stream().map(this::safeIdentifier).map(this::wrapIdentifier).reduce((left, right) -> left + "," + right).orElseThrow();
        String placeholders = columns.stream().map(col -> "?").reduce((left, right) -> left + "," + right).orElseThrow();
        return "INSERT INTO " + qualifiedTable + " (" + columnNames + ") VALUES (" + placeholders + ")";
    }

    private String qualifiedTable(String database, String table) {
        return wrapIdentifier(safeIdentifier(database)) + "." + wrapIdentifier(safeIdentifier(table));
    }

    private String safeIdentifier(String value) {
        if (!SAFE_IDENTIFIER.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid identifier: " + value);
        }
        return value;
    }

    private String wrapIdentifier(String identifier) {
        return "`" + identifier + "`";
    }
}
