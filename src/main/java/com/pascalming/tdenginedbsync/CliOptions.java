package com.pascalming.tdenginedbsync;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

record CliOptions(CliAction action, String database, String table, Path file, int batchSize) {

    static CliOptions parse(String[] args) {
        Map<String, String> values = new HashMap<>();
        for (String arg : args) {
            if (!arg.startsWith("--") || !arg.contains("=")) {
                throw new IllegalArgumentException("Arguments must be provided as --key=value: " + arg);
            }
            int separatorIndex = arg.indexOf('=');
            String key = arg.substring(2, separatorIndex);
            String value = arg.substring(separatorIndex + 1);
            values.put(key, value);
        }

        String mode = require(values, "mode");
        CliAction action = switch (mode.toLowerCase()) {
            case "export" -> CliAction.EXPORT;
            case "import" -> CliAction.IMPORT;
            default -> throw new IllegalArgumentException("mode must be export or import");
        };

        int batchSize = values.containsKey("batch-size")
                ? parsePositiveInt(values.get("batch-size"), "batch-size")
                : 500;

        return new CliOptions(
                action,
                require(values, "database"),
                require(values, "table"),
                Path.of(require(values, "file")),
                batchSize
        );
    }

    private static String require(Map<String, String> values, String key) {
        String value = values.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required argument: --" + key + "=<value>");
        }
        return value;
    }

    private static int parsePositiveInt(String value, String key) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException("--" + key + " must be greater than 0");
            }
            return parsed;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("--" + key + " must be an integer", ex);
        }
    }
}
