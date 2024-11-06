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
 *   Oct 24, 2019 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.database.databricks;

import static org.knime.bigdata.database.databricks.DatabricksDBDriverLocator.isSimbaConnection;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.database.VariableContext;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.connection.UrlDBConnectionController;

/**
 * DB connection controller that injects additional parameter into JDBC URL to connect a Databricks cluster or SQL
 * Warehouse.
 *
 * This controller uses a {@link DatabricksAccessTokenCredential}.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksOAuth2DBConnectionController extends UrlDBConnectionController {

    private final DatabricksClusterStatusProvider m_clusterStatus;

    private final DatabricksAccessTokenCredential m_credential;

    private final String m_httpPath;

    private final String m_description;

    /**
     * Default constructor.
     *
     * @param jdbcUrl driver specific JDBC URL
     * @param httpPath HTTP path parameter
     * @param clusterStatus cluster status provider
     * @param description optional string used in description
     * @param credential the credential provider to use
     * @throws InvalidSettingsException on unknown JDBC URL schema
     */
    public DatabricksOAuth2DBConnectionController(final String jdbcUrl, final String httpPath,
        final DatabricksClusterStatusProvider clusterStatus, final String description,
        final DatabricksAccessTokenCredential credential) throws InvalidSettingsException {

        super(jdbcUrl);

        if (!isSimbaConnection(jdbcUrl)) {
            throw new InvalidSettingsException("Unknown JDBC schema (only databricks and spark supported)");
        }

        m_clusterStatus = clusterStatus;
        m_credential = credential;
        m_httpPath = httpPath;
        m_description = description;
    }

    @Override
    protected Connection createConnection(final AttributeValueRepository attributeValues, final Driver driver,
        final VariableContext variableContext, final ExecutionMonitor monitor)
        throws CanceledExecutionException, SQLException {

        return new DatabricksOAuth2DBConnectionWrapper(
            super.createConnection(attributeValues, driver, variableContext, monitor), m_clusterStatus, m_credential);
    }

    @Override
    protected Properties prepareJdbcProperties(final AttributeValueRepository attributeValues,
        final VariableContext variableContext, final ExecutionMonitor monitor)
        throws CanceledExecutionException, SQLException {

        final Properties props =  super.prepareJdbcProperties(attributeValues, variableContext, monitor);
        props.put("transportMode", "http");
        props.put("ssl", "1");
        props.put("httpPath", m_httpPath);

        if (!props.containsKey("AuthMech")) { // let the user overwrite this
            props.put("AuthMech", "11");
        }

        if (!props.containsKey("Auth_Flow")) { // let the user overwrite this
            props.put("Auth_Flow", "0"); // pass-though mode
        }

        try {
            props.put("Auth_AccessToken", m_credential.getAccessToken());
        } catch (final IOException e) {
            throw new SQLException("Unable to fetch current access token", e);
        }

        return props;
    }

    @Override
    public String getConnectionDescription() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.getConnectionDescription());
        if (!StringUtils.isBlank(m_description)) {
            sb.append(", ").append(m_description);
        }
        return sb.toString();
    }

}
