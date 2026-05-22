package com.pascalming.tdenginedbsync;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CliOptionsTest {

    @Test
    void shouldParseExportMode() {
        CliOptions options = CliOptions.parse(new String[]{
                "--mode=export",
                "--database=test_db",
                "--table=test_table",
                "--file=/tmp/test.csv"
        });

        assertEquals(CliAction.EXPORT, options.action());
        assertEquals("test_db", options.database());
        assertEquals("test_table", options.table());
        assertEquals("/tmp/test.csv", options.file().toString());
        assertEquals(500, options.batchSize());
    }

    @Test
    void shouldRejectMissingRequiredArgument() {
        assertThrows(IllegalArgumentException.class, () -> CliOptions.parse(new String[]{
                "--mode=import",
                "--database=test_db",
                "--file=/tmp/test.csv"
        }));
    }
}
