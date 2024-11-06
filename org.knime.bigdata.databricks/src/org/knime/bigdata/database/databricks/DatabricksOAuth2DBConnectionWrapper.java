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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;

/**
 * Databricks DB connection wrapper that always uses a fresh access token, and avoids {@code close} on terminated
 * clusters, otherwise cluster would be auto started.
 *
 * This controller uses a {@link DatabricksAccessTokenCredential}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksOAuth2DBConnectionWrapper extends DatabricksUserDBConnectionWrapper {

    private final DatabricksAccessTokenCredential m_credential;

    private final Connection m_connection;

    private String m_lastAccessToken;

    /**
     * Default constructor.
     *
     * @param connection connection to wrap
     * @param clusterStatus cluster status provider or {@code null} to ignore all {@link #close()} calls.
     * @param credential the credential provider
     */
    DatabricksOAuth2DBConnectionWrapper(final Connection connection,
        final DatabricksClusterStatusProvider clusterStatus, final DatabricksAccessTokenCredential credential)
        throws SQLException {

        super(connection, clusterStatus);
        m_connection = connection;
        m_credential = credential;
        m_lastAccessToken = getAccessToken(credential);
    }

    @Override
    protected void ensureOpenConnectionWrapper() throws SQLException {
        updateAccessToken(getAccessToken(m_credential));
        super.ensureOpenConnectionWrapper();
    }

    private static String getAccessToken(final DatabricksAccessTokenCredential credential) throws SQLException {
        try {
            return credential.getAccessToken();
        } catch (final IOException e) {
            throw new SQLException("Unable to fetch current access token", e);
        }
    }

    private synchronized void updateAccessToken(final String newToken) throws SQLException {
        if (m_lastAccessToken != null && !m_lastAccessToken.equals(newToken)) {
            final Properties props = new Properties();
            props.put("Auth_AccessToken", newToken);
            m_connection.setClientInfo(props);
            m_lastAccessToken = newToken;
        }
    }

}
