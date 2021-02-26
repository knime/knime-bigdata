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

import static org.knime.bigdata.database.databricks.DatabricksDBDriverLocator.isHiveConnection;
import static org.knime.bigdata.database.databricks.DatabricksDBDriverLocator.isSimbaConnection;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.database.VariableContext;
import org.knime.database.attribute.AttributeValueRepository;
import org.knime.database.connection.UserDBConnectionController;

/**
 * DB connection controller that injects additional parameter into JDBC URL to connect a Databricks cluster.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksDBConnectionController extends UserDBConnectionController {

    private static final String HTTP_PATH_FORMAT = "sql/protocolv1/o/%s/%s";

    private final DatabricksClusterStatusProvider m_clusterStatus;
    private final String m_clusterId;
    private final String m_workspaceId;
    private final String m_user;
    private final String m_password;

    /**
     * Default constructor.
     *
     * @param jdbcUrl driver specific JDBC URL
     * @param clusterStatus cluster status provider
     * @param clusterId unique cluster identifier
     * @param workspaceId workspace identifier for Azure or 0
     * @param user username to use (might by "token")
     * @param password password or token to use
     * @throws InvalidSettingsException on unknown JDBC URL schema
     */
    public DatabricksDBConnectionController(final String jdbcUrl, final DatabricksClusterStatusProvider clusterStatus,
        final String clusterId, final String workspaceId, final String user, final String password)
        throws InvalidSettingsException {

        super(jdbcUrl, AuthenticationType.USER_PWD, user, password, null, null);

        if (!isHiveConnection(jdbcUrl) && !isSimbaConnection(jdbcUrl)) {
            throw new InvalidSettingsException("Unknown JDBC schema (only hive2 and spark supported)");
        }

        m_clusterStatus = clusterStatus;
        m_clusterId = clusterId;
        m_workspaceId = workspaceId;
        m_user = user;
        m_password = password;
    }

    @Override
    protected Connection createConnection(final AttributeValueRepository attributeValues, final Driver driver,
        final VariableContext variableContext, final ExecutionMonitor monitor)
        throws CanceledExecutionException, SQLException {

        return new DatabricksDBConnectionWrapper(
            super.createConnection(attributeValues, driver, variableContext, monitor), m_clusterStatus);
    }

    @Override
    protected Properties prepareJdbcProperties(final AttributeValueRepository attributeValues,
        final VariableContext variableContext, final ExecutionMonitor monitor) throws CanceledExecutionException, SQLException {

        final Properties props =  super.prepareJdbcProperties(attributeValues, variableContext, monitor);
        final String jdbcUrl = getJdbcUrl();
        final String httpPath = getHttpPath(m_clusterId, m_workspaceId);

        if (isHiveConnection(jdbcUrl)) {
            props.put("transportMode", "http");
            props.put("ssl", "true");
            props.put("httpPath", httpPath);
            // user+password gets already set by getNonURLProperties
        } else if (isSimbaConnection(jdbcUrl)) {
            props.put("transportMode", "http");
            props.put("ssl", "1");
            props.put("httpPath", httpPath);
            props.put("UID", m_user);
            props.put("PWD", m_password);

            if (!props.containsKey("AuthMech")) { // let the user overwrite this
                props.put("AuthMech", "3");
            }
        }

        return props;
    }

    @Override
    public String getConnectionDescription() {
        final StringBuilder sb = new StringBuilder();
        sb.append(super.getConnectionDescription());
        sb.append(", cluster=\"").append(m_clusterId).append('"');
        if (!StringUtils.isBlank(m_workspaceId)) {
            sb.append(", workspace=\"").append(m_workspaceId).append('"');
        }
        return sb.toString();
    }

    /**
     * Format httpPath parameter for Databricks JDBC URL.
     * @param clusterId cluster ID or alias
     * @param workspaceId workspace ID on Azure, empty or {@code null} on AWS
     * @return httpPath parameter for JDBC URL
     */
    public static String getHttpPath(final String clusterId, final String workspaceId) {
        if (StringUtils.isBlank(workspaceId)) {
            return String.format(HTTP_PATH_FORMAT, 0, clusterId);
        } else {
            return String.format(HTTP_PATH_FORMAT, workspaceId, clusterId);
        }
    }
}
