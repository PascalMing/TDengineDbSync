package com.pascalming.tdenginedbsync;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CliRunnerTest {

    @Test
    void shouldRouteExportCommand() {
        TdengineDataTransferService service = mock(TdengineDataTransferService.class);
        CliRunner runner = new CliRunner(service);

        runner.run(
                "--mode=export",
                "--database=test_db",
                "--table=test_table",
                "--file=/tmp/export.csv"
        );

        verify(service).exportData("test_db", "test_table", Path.of("/tmp/export.csv"));
    }

    @Test
    void shouldRouteImportCommand() {
        TdengineDataTransferService service = mock(TdengineDataTransferService.class);
        CliRunner runner = new CliRunner(service);

        runner.run(
                "--mode=import",
                "--database=test_db",
                "--table=test_table",
                "--file=/tmp/import.csv",
                "--batch-size=100"
        );

        verify(service).importData("test_db", "test_table", Path.of("/tmp/import.csv"), 100);
    }
}
