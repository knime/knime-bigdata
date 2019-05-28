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
package org.knime.bigdata.database.hive;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.knime.core.node.NodeLogger;
import org.knime.database.connection.impl.AbstractConnectionWrapper;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class HiveWrappedConnection extends AbstractConnectionWrapper {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(HiveWrappedConnection.class);

    HiveWrappedConnection(final Connection connection) {
        super(connection);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Array wrap(final Array array) throws SQLException {
        return array;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DatabaseMetaData wrap(final DatabaseMetaData metadata) throws SQLException {
        return metadata;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected CallableStatement wrap(final CallableStatement statement) throws SQLException {
        return statement;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PreparedStatement wrap(final PreparedStatement statement) throws SQLException {
        return statement;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Statement wrap(final Statement statement) throws SQLException {
        return statement;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAutoCommit(final boolean autoCommit) throws SQLException {
        ensureOpenConnectionWrapper();
        LOGGER.debug("setAutoCommit is not supported by Hive");
    }

    @Override
    public void commit() throws SQLException {
        ensureOpenConnectionWrapper();
        LOGGER.debug("Commit is not supported by Hive");
    }

    @Override
    public void rollback() throws SQLException {
        ensureOpenConnectionWrapper();
        LOGGER.debug("Rollback is not supported by Hive");
    }
}