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
 *   Oct 31, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.database.databricks;

import java.sql.Array;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.core.node.NodeLogger;
import org.knime.database.connection.wrappers.AbstractConnectionWrapper;

/**
 * Databricks DB connection wrapper that avoids {@code close} on terminated clusters, otherwise cluster would be auto
 * started.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksUserDBConnectionWrapper extends AbstractConnectionWrapper {

    private static final NodeLogger LOG = NodeLogger.getLogger(DatabricksUserDBConnectionWrapper.class);

    private final DatabricksClusterStatusProvider m_clusterStatus;

    /**
     * Default constructor.
     *
     * @param connection connection to wrap
     * @param clusterStatus cluster status provider or {@code null} to ignore all {@link #close()} calls.
     */
    protected DatabricksUserDBConnectionWrapper(final Connection connection, final DatabricksClusterStatusProvider clusterStatus) {
        super(connection);
        m_clusterStatus = clusterStatus;
    }

    /**
     * {@inheritDoc}
     *
     * Calling {@code close} on terminated cluster is a no-op.
     */
    @Override
    public void close() throws SQLException {
        try {
            if (m_clusterStatus != null && m_clusterStatus.isClusterRunning()) {
                super.close();
            } else {
                LOG.info("Databricks cluster is not in running state, no JDBC connection close required.");
            }
        } catch (KNIMESparkException e) {
            LOG.warn("Unable to get Databricks cluster state, JDBC connection close failed");
        }
    }

    @Override
    protected Array wrap(final Array array) throws SQLException {
        return array;
    }

    @Override
    protected CallableStatement wrap(final CallableStatement statement) throws SQLException {
        return statement;
    }

    @Override
    protected DatabaseMetaData wrap(final DatabaseMetaData metadata) throws SQLException {
        return metadata;
    }

    @Override
    protected PreparedStatement wrap(final PreparedStatement statement) throws SQLException {
        return statement;
    }

    @Override
    protected Statement wrap(final Statement statement) throws SQLException {
        return statement;
    }
}
