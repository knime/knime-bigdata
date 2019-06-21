/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Jun 20, 2019 by mareike
 */
package org.knime.bigdata.spark.local.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

/**
 * This Statement class wraps a HiveStatment to overwrite the ResultSet of the DESCRIBE FORMATTED Query.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class LocalHiveStatement implements Statement {
    private final Statement m_statement;

    /**
     * Constructs a LocalHiveStatement from a given {@link Statement}
     *
     * @param statement the statement to wrap
     */
    public LocalHiveStatement(final Statement statement) {
        m_statement = statement;
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return m_statement.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return m_statement.isWrapperFor(iface);
    }

    @SuppressWarnings("resource")
    @Override
    public ResultSet executeQuery(final String sql) throws SQLException {
        if (sql.startsWith("DESCRIBE FORMATTED")) {
            //Create a custom ResultSet the mimics the behavior
            //of the usual Hive ResultSet for the DESCRIBE FORMATTED Query
            final ResultSet result = m_statement.executeQuery(sql);
            return new LocalHiveDescribeResultSet(result);
        }
        return m_statement.executeQuery(sql);
    }

    @Override
    public int executeUpdate(final String sql) throws SQLException {
        return m_statement.executeUpdate(sql);
    }

    @Override
    public void close() throws SQLException {
        m_statement.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return m_statement.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(final int max) throws SQLException {
        m_statement.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return m_statement.getMaxRows();
    }

    @Override
    public void setMaxRows(final int max) throws SQLException {
        m_statement.setMaxRows(max);

    }

    @Override
    public void setEscapeProcessing(final boolean enable) throws SQLException {
        m_statement.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return m_statement.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(final int seconds) throws SQLException {
        m_statement.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        m_statement.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return m_statement.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        m_statement.clearWarnings();

    }

    @Override
    public void setCursorName(final String name) throws SQLException {
        m_statement.setCursorName(name);
    }

    @Override
    public boolean execute(final String sql) throws SQLException {
        return m_statement.execute(sql);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return m_statement.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return m_statement.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return m_statement.getMoreResults();
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        m_statement.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return m_statement.getFetchDirection();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        m_statement.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return m_statement.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return m_statement.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return m_statement.getResultSetType();
    }

    @Override
    public void addBatch(final String sql) throws SQLException {
        m_statement.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        m_statement.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return m_statement.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return m_statement.getConnection();
    }

    @Override
    public boolean getMoreResults(final int current) throws SQLException {
        return m_statement.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return m_statement.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(final String sql, final int autoGeneratedKeys) throws SQLException {
        return m_statement.executeUpdate(sql, autoGeneratedKeys);
    }

    @Override
    public int executeUpdate(final String sql, final int[] columnIndexes) throws SQLException {
        return m_statement.executeUpdate(sql, columnIndexes);
    }

    @Override
    public int executeUpdate(final String sql, final String[] columnNames) throws SQLException {
        return m_statement.executeUpdate(sql, columnNames);
    }

    @Override
    public boolean execute(final String sql, final int autoGeneratedKeys) throws SQLException {
        return m_statement.execute(sql, autoGeneratedKeys);
    }

    @Override
    public boolean execute(final String sql, final int[] columnIndexes) throws SQLException {
        return m_statement.execute(sql, columnIndexes);
    }

    @Override
    public boolean execute(final String sql, final String[] columnNames) throws SQLException {
        return m_statement.execute(sql, columnNames);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return m_statement.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return m_statement.isClosed();
    }

    @Override
    public void setPoolable(final boolean poolable) throws SQLException {
        m_statement.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return m_statement.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        m_statement.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return m_statement.isCloseOnCompletion();
    }

}
