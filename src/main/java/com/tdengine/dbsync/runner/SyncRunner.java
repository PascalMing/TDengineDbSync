package com.tdengine.dbsync.runner;

import com.tdengine.dbsync.service.SyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class SyncRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(SyncRunner.class);

    private final SyncService syncService;

    public SyncRunner(SyncService syncService) {
        this.syncService = syncService;
    }

    @Override
    public void run(String... args) {
        try {
            syncService.execute();
        } catch (Exception e) {
            log.error("Execution failed, exiting with error");
            System.exit(1);
        }

        log.info("TDengine DbSync completed successfully, exiting");
        System.exit(0);
    }
}
