package com.tdengine.dbsync.connection;

import com.tdengine.dbsync.model.SuperTableMeta;

import java.sql.ResultSet;
import java.util.List;

/**
 * A wrapper around a {@link TdConnection} that intercepts {@link #close()}
 * to return the underlying connection to the pool instead of actually closing it.
 */
public class PooledTdConnection implements TdConnection {

    private final TdConnection delegate;
    private final TdConnectionPool pool;
    private volatile boolean closed;

    PooledTdConnection(TdConnection delegate, TdConnectionPool pool) {
        this.delegate = delegate;
        this.pool = pool;
    }

    private void checkNotClosed() {
        if (closed) {
            throw new IllegalStateException("Connection has been returned to pool");
        }
    }

    @Override
    public void testConnection() {
        checkNotClosed();
        delegate.testConnection();
    }

    @Override
    public List<String> getSuperTableNames(String database) {
        checkNotClosed();
        return delegate.getSuperTableNames(database);
    }

    @Override
    public SuperTableMeta getSuperTableMeta(String database, String stableName) {
        checkNotClosed();
        return delegate.getSuperTableMeta(database, stableName);
    }

    @Override
    public void query(String sql, ResultSetHandler handler) {
        checkNotClosed();
        delegate.query(sql, handler);
    }

    @Override
    public void execute(String sql) {
        checkNotClosed();
        delegate.execute(sql);
    }

    @Override
    public int executeUpdate(String sql) {
        checkNotClosed();
        return delegate.executeUpdate(sql);
    }

    @Override
    public ResultSet queryDirect(String sql) {
        checkNotClosed();
        return delegate.queryDirect(sql);
    }

    /**
     * Returns the underlying connection to the pool instead of closing it.
     * Once called, this wrapper is no longer usable.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        pool.returnConnection(delegate);
    }
}
