/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   16.04.2019 (Mareike Hoeger, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.bigdata.spark.local.db;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.knime.database.connection.wrappers.AbstractArrayWrapper;
import org.knime.database.connection.wrappers.AbstractCallableStatementWrapper;
import org.knime.database.connection.wrappers.AbstractConnectionWrapper;
import org.knime.database.connection.wrappers.AbstractDatabaseMetaDataWrapper;
import org.knime.database.connection.wrappers.AbstractPreparedStatementWrapper;
import org.knime.database.connection.wrappers.AbstractResultSetWrapper;
import org.knime.database.connection.wrappers.AbstractStatementWrapper;
import org.knime.database.util.Wrappers;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class LocalHiveWrappedConnection extends AbstractConnectionWrapper implements LocalHiveWrapper {

    private class LocalHiveArrayWrapper extends AbstractArrayWrapper implements LocalHiveWrapper {
        LocalHiveArrayWrapper(final Array array) {
            super(array);
        }

        @Override
        protected ResultSet wrap(final ResultSet resultSet) throws SQLException {
            return LocalHiveWrappedConnection.this.wrap(resultSet.getStatement(), resultSet);
        }
    }

    private class LocalHiveCallableStatementWrapper extends AbstractCallableStatementWrapper
        implements LocalHiveWrapper {
        LocalHiveCallableStatementWrapper(final CallableStatement statement) {
            super(LocalHiveWrappedConnection.this, statement);
        }

        @Override
        protected ResultSet wrap(final ResultSet resultSet) throws SQLException {
            return LocalHiveWrappedConnection.this.wrap(this, resultSet);
        }
    }

    private class LocalHiveDatabaseMetaDataWrapper extends AbstractDatabaseMetaDataWrapper implements LocalHiveWrapper {
        LocalHiveDatabaseMetaDataWrapper(final DatabaseMetaData databaseMetaData) {
            super(LocalHiveWrappedConnection.this, databaseMetaData);
        }

        @Override
        public String getSQLKeywords() throws SQLException {
            try {
                return super.getSQLKeywords();
            } catch (final Exception e) {
                //method not supported by driver return empty string instead
                return StringUtils.EMPTY;
            }
        }

        @Override
        public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException {
            if (types != null && types[0].equals("TABLE") || tableNamePattern != null && types == null) {
                final StringBuilder query = new StringBuilder("SHOW TABLES");
                if (schemaPattern == null || schemaPattern.isEmpty()) {
                    schemaPattern = getConnection().getSchema();
                }
                query.append(" IN ").append(schemaPattern);
                if (tableNamePattern != null && !tableNamePattern.equals("%")) {
                    query.append(" LIKE '").append(tableNamePattern).append('\'');
                }
                final Statement statement = getConnection().createStatement();
                try {
                    final ResultSet resultSet = statement.executeQuery(query.toString());
                    try {
                        return wrapTableQuerying(statement, resultSet);
                    } catch (final Throwable throwable) {
                        try (ResultSet resultSetToClose = resultSet) {
                            throw throwable;
                        }
                    }
                } catch (final Throwable throwable) {
                    try (Statement statementToClose = statement) {
                        throw throwable;
                    }
                }
            }
            return super.getTables(catalog, schemaPattern, tableNamePattern, types);
        }

        @Override
        protected ResultSet wrap(final ResultSet resultSet) throws SQLException {
            return LocalHiveWrappedConnection.this.wrap(resultSet.getStatement(), resultSet);
        }
    }

    private class LocalHiveDescriptionResultSetWrapper extends LocalHiveResultSetWrapper
        implements LocalHiveDescriptionWrapper {
        private final List<String> m_namesList = new ArrayList<>();

        private final List<String> m_typesList = new ArrayList<>();

        private int m_position = -1;

        LocalHiveDescriptionResultSetWrapper(final Statement statement, final ResultSet resultSet) throws SQLException {
            super(statement, resultSet);
            final List<String> partitionNames = new ArrayList<>();
            m_namesList.add("# col_name");
            m_typesList.add("");
            //First search the normal columns
            while (resultSet.next()) {
                final String name = resultSet.getString(1);
                if (name.startsWith("#")) {
                    partitionNames.add(name);
                    m_typesList.add("");
                    break;
                } else {
                    if (!name.isEmpty()) {
                        m_namesList.add(name);
                        m_typesList.add(resultSet.getString(2));
                    }
                }
            }
            //Now search for the partition columns
            if (partitionNames.get(0).startsWith("# Partition Information")) {
                while (resultSet.next()) {
                    final String name = resultSet.getString(1);
                    if (name.startsWith("#")) {
                        partitionNames.add(name);
                        m_typesList.add("");
                        if (!name.startsWith("# col_name")) {
                            break;
                        }
                    } else {
                        if (!name.isEmpty()) {
                            //We have to remove the partitioning column from the list of normal columns names and types
                            final int index = m_namesList.lastIndexOf(name);
                            m_namesList.remove(index);
                            m_typesList.remove(index);
                            partitionNames.add(name);
                            m_typesList.add(resultSet.getString(2));
                        }
                    }
                }
            }

            m_namesList.addAll(partitionNames);
        }

        @Override
        public String getString(int columnIndex) throws SQLException {
            if (m_position < m_namesList.size()) {
                if (columnIndex == 1) {
                    return m_namesList.get(m_position);
                }
                if (columnIndex == 2) {
                    return m_typesList.get(m_position);
                }
                throw new ArrayIndexOutOfBoundsException();
            }
            return super.getString(columnIndex);
        }

        @Override
        public boolean next() throws SQLException {
            ++m_position;
            return m_position < m_namesList.size() || super.next();
        }
    }

    private class LocalHivePreparedStatementWrapper extends AbstractPreparedStatementWrapper
        implements LocalHiveWrapper {
        LocalHivePreparedStatementWrapper(final PreparedStatement statement) {
            super(LocalHiveWrappedConnection.this, statement);
        }

        @Override
        protected ResultSet wrap(final ResultSet resultSet) throws SQLException {
            return LocalHiveWrappedConnection.this.wrap(this, resultSet);
        }
    }

    private class LocalHiveResultSetWrapper extends AbstractResultSetWrapper implements LocalHiveWrapper {
        LocalHiveResultSetWrapper(final Statement statement, final ResultSet resultSet) throws SQLException {
            super(requireLocalHiveStatement(statement), resultSet);
        }

        @Override
        protected Array wrap(final Array array) throws SQLException {
            return LocalHiveWrappedConnection.this.wrap(array);
        }
    }

    private class LocalHiveStatementWrapper extends AbstractStatementWrapper implements LocalHiveWrapper {
        private final Statement m_statement;

        LocalHiveStatementWrapper(final Statement statement) {
            super(LocalHiveWrappedConnection.this, statement);
            m_statement = statement;
        }

        @Override
        public ResultSet executeQuery(final String sql) throws SQLException {
            if (isDescriptionQuery(sql)) {
                final ResultSet resultSet = m_statement.executeQuery(sql);
                try {
                    return LocalHiveWrappedConnection.this.wrapDescribing(this, resultSet);
                } catch (final Throwable throwable) {
                    try (ResultSet resultSetToClose = resultSet) {
                        throw throwable;
                    }
                }
            }
            return super.executeQuery(sql);
        }

        @Override
        protected ResultSet wrap(final ResultSet resultSet) throws SQLException {
            return LocalHiveWrappedConnection.this.wrap(this, resultSet);
        }
    }

    private class LocalHiveTableQueryResultSetWrapper extends LocalHiveResultSetWrapper
        implements LocalHiveTableQueryWrapper {
        LocalHiveTableQueryResultSetWrapper(final Statement statement, final ResultSet resultSet) throws SQLException {
            super(statement, resultSet);
        }

        @Override
        public String getString(String columnLabel) throws SQLException {
            return super.getString("TABLE_NAME".equals(columnLabel) ? "tablename" : columnLabel);
        }

        @Override
        public void close() throws SQLException {
            try (Statement statementToClose = getStatement()) {
                super.close();
            }
        }
    }

    private static boolean isDescriptionQuery(final String sql) {
        return sql != null && sql.startsWith("DESCRIBE FORMATTED");
    }

    private static boolean isLocalHiveDescriptionWrapper(final Object object) throws SQLException {
        return Wrappers.isWrapperFor(object, LocalHiveDescriptionWrapper.class);
    }

    private static boolean isLocalHiveTableQueryWrapper(final Object object) throws SQLException {
        return Wrappers.isWrapperFor(object, LocalHiveTableQueryWrapper.class);
    }

    private static boolean isLocalHiveWrapper(final Object object) throws SQLException {
        return Wrappers.isWrapperFor(object, LocalHiveWrapper.class);
    }

    private static Statement requireLocalHiveStatement(final Statement statement) throws SQLException {
        if (statement != null && !isLocalHiveWrapper(statement)) {
            throw new IllegalArgumentException("statement must be a local Hive statement.");
        }
        return statement;
    }

    LocalHiveWrappedConnection(final Connection connection) {
        super(connection);
    }

    @Override
    protected Array wrap(Array array) throws SQLException {
        return isLocalHiveWrapper(array) ? array : new LocalHiveArrayWrapper(array);
    }

    @Override
    protected CallableStatement wrap(CallableStatement statement) throws SQLException {
        return isLocalHiveWrapper(statement) ? statement : new LocalHiveCallableStatementWrapper(statement);
    }

    @Override
    protected DatabaseMetaData wrap(DatabaseMetaData metadata) throws SQLException {
        return isLocalHiveWrapper(metadata) ? metadata : new LocalHiveDatabaseMetaDataWrapper(metadata);
    }

    @Override
    protected PreparedStatement wrap(PreparedStatement statement) throws SQLException {
        return isLocalHiveWrapper(statement) ? statement : statement instanceof CallableStatement
            ? wrap((CallableStatement)statement) : new LocalHivePreparedStatementWrapper(statement);
    }

    @Override
    protected Statement wrap(final Statement statement) throws SQLException {
        return isLocalHiveWrapper(statement) ? statement : statement instanceof PreparedStatement
            ? wrap((PreparedStatement)statement) : new LocalHiveStatementWrapper(statement);
    }

    private ResultSet wrap(final Statement statement, final ResultSet resultSet) throws SQLException {
        return isLocalHiveWrapper(resultSet) ? resultSet
            : new LocalHiveResultSetWrapper(statement == null ? null : wrap(statement), resultSet);
    }

    private ResultSet wrapDescribing(final Statement statement, final ResultSet resultSet) throws SQLException {
        return isLocalHiveDescriptionWrapper(resultSet) ? resultSet
            : new LocalHiveDescriptionResultSetWrapper(statement == null ? null : wrap(statement), resultSet);
    }

    private ResultSet wrapTableQuerying(final Statement statement, final ResultSet resultSet) throws SQLException {
        return isLocalHiveTableQueryWrapper(resultSet) ? resultSet
            : new LocalHiveTableQueryResultSetWrapper(statement == null ? null : wrap(statement), resultSet);
    }
}
