package com.tdengine.dbsync.connection;

import org.springframework.stereotype.Component;

/**
 * Factory for creating {@link TdConnection} instances.
 * <p>
 * Delegates to {@link TdConnectionPool} for pooled connections when
 * {@code tdengine.connection-pool-size > 0} (default 50), or creates
 * direct un-pooled connections when the pool is disabled.
 */
@Component
public class TdConnectionFactory {

    private final TdConnectionPool pool;

    public TdConnectionFactory(TdConnectionPool pool) {
        this.pool = pool;
    }

    /**
     * Obtain a {@link TdConnection}. When pooling is enabled the returned
     * connection is a {@link PooledTdConnection} whose {@code close()} will
     * return the underlying connection to the pool rather than destroying it.
     */
    public TdConnection create() {
        return pool.borrow();
    }
}
