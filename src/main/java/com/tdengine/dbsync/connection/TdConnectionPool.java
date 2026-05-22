package com.tdengine.dbsync.connection;

import com.tdengine.dbsync.config.SyncProperties;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * A simple blocking connection pool for {@link TdConnection} instances.
 * <p>
 * Uses a {@link Semaphore} to bound the maximum number of in-flight connections,
 * and a {@link ConcurrentLinkedQueue} to hold idle connections for reuse.
 * When {@code maxSize <= 0} pooling is disabled and each borrow creates a fresh
 * connection (backward compatible).
 */
@Component
public class TdConnectionPool {

    private static final Logger log = LoggerFactory.getLogger(TdConnectionPool.class);

    private final int maxSize;
    private final Semaphore semaphore;
    private final Queue<TdConnection> idle = new ConcurrentLinkedQueue<>();
    private final Supplier<TdConnection> factory;
    private volatile boolean destroyed;

    public TdConnectionPool(SyncProperties properties) {
        this.maxSize = properties.getConnectionPoolSize();
        this.semaphore = maxSize > 0 ? new Semaphore(maxSize, true) : null;
        this.factory = () -> {
            SyncProperties.ConnectionMode mode = properties.getConnectionMode();
            return switch (mode) {
                case JDBC -> new JdbcConnection(properties.getJdbc());
                case RESTFUL -> new RestApiConnection(properties.getRestful());
            };
        };
        if (maxSize > 0) {
            log.info("Connection pool enabled, max size: {}", maxSize);
        } else {
            log.info("Connection pool disabled (connection-pool-size <= 0)");
        }
    }

    /**
     * Borrow a connection from the pool.
     * <p>
     * When pooling is enabled, this blocks until a connection is available
     * (up to 30 seconds), then returns a {@link PooledTdConnection} whose
     * {@code close()} will return the underlying connection to the pool.
     * <p>
     * When pooling is disabled, returns a fresh un-pooled connection directly.
     */
    public TdConnection borrow() {
        if (maxSize <= 0) {
            return factory.get();
        }

        // Acquire a permit — blocks until one is available (or timeout)
        try {
            if (!semaphore.tryAcquire(30, TimeUnit.SECONDS)) {
                throw new RuntimeException(
                        "Connection pool timeout: all " + maxSize + " connections are busy " +
                                "and none became available within 30 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Connection pool interrupted while borrowing", e);
        }

        // Permit acquired — try idle queue first, then create new
        TdConnection conn = idle.poll();
        if (conn != null) {
            return new PooledTdConnection(conn, this);
        }

        // No idle connection — create a new one (permit already acquired)
        try {
            TdConnection newConn = factory.get();
            newConn.testConnection();
            return new PooledTdConnection(newConn, this);
        } catch (Exception e) {
            // Creation failed: release permit so others can try
            semaphore.release();
            throw new RuntimeException(
                    "Failed to establish pooled connection: " + e.getMessage(), e);
        }
    }

    /**
     * Called by {@link PooledTdConnection#close()} to return a connection to the pool.
     * Package-private.
     */
    void returnConnection(TdConnection conn) {
        if (destroyed) {
            closeQuietly(conn);
            return;
        }
        idle.offer(conn);
        semaphore.release();
    }

    /**
     * Shut down the pool: drain all idle connections and close them.
     * Borrowed connections still in use will be closed on return (via
     * {@link #returnConnection}) since {@code destroyed} is set to {@code true}.
     */
    @PreDestroy
    public void destroy() {
        destroyed = true;
        log.info("Shutting down connection pool...");
        int drained = 0;
        TdConnection conn;
        while ((conn = idle.poll()) != null) {
            closeQuietly(conn);
            drained++;
        }
        int remaining = maxSize - semaphore.availablePermits();
        if (drained > 0 || remaining > 0) {
            log.info("Connection pool shut down: drained {} idle connection(s), {} still in use",
                    drained, remaining);
        } else {
            log.info("Connection pool shut down (no connections were in use)");
        }
    }

    private void closeQuietly(TdConnection conn) {
        try {
            conn.close();
        } catch (Exception e) {
            log.warn("Error closing pooled connection: {}", e.getMessage());
        }
    }
}
