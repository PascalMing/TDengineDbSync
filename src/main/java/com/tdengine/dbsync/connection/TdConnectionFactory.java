package com.tdengine.dbsync.connection;

import com.tdengine.dbsync.config.SyncProperties;
import org.springframework.stereotype.Component;

/**
 * Factory for creating TdConnection instances based on configuration.
 */
@Component
public class TdConnectionFactory {

    private final SyncProperties properties;

    public TdConnectionFactory(SyncProperties properties) {
        this.properties = properties;
    }

    /**
     * Create a new TdConnection based on the configured connection mode.
     */
    public TdConnection create() {
        return switch (properties.getConnectionMode()) {
            case JDBC -> new JdbcConnection(properties.getJdbc());
            case RESTFUL -> new RestApiConnection(properties.getRestful());
        };
    }
}
