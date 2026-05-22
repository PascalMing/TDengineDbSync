package com.pascalming.tdenginedbsync;

import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
class CliRunner implements CommandLineRunner {

    private final TdengineDataTransferService dataTransferService;

    CliRunner(TdengineDataTransferService dataTransferService) {
        this.dataTransferService = dataTransferService;
    }

    @Override
    public void run(String... args) {
        CliOptions options = CliOptions.parse(args);

        switch (options.action()) {
            case EXPORT -> dataTransferService.exportData(options.database(), options.table(), options.file());
            case IMPORT -> dataTransferService.importData(options.database(), options.table(), options.file(), options.batchSize());
        }
    }
}
