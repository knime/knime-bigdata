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
 *   2024-11-05 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.spark.core.databricks.node.create;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.database.databricks.DatabricksOAuth2DBConnectionController;
import org.knime.bigdata.database.databricks.DatabricksUserDBConnectionController;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenType;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.core.node.InvalidSettingsException;
import org.knime.database.connection.DBConnectionController;

/**
 * Factory to create a Databricks Spark Cluster connection controller.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public final class ClusterDBControllerFactory {

    private static final String HTTP_PATH_FORMAT = "sql/protocolv1/o/%s/%s";

    private ClusterDBControllerFactory() {
    }

    /**
     * Create controller connected to a cluster.
     *
     * @param jdbcUrl driver specific JDBC URL
     * @param clusterStatus cluster status provider
     * @param clusterId unique cluster identifier
     * @param workspaceId workspace identifier for Azure or 0
     * @param user username to use, might by "token"
     * @param password password or token to use
     * @throws InvalidSettingsException on unknown JDBC URL schema
     * @return new controller instance
     */
    public static DBConnectionController create(final String jdbcUrl,
        final DatabricksClusterStatusProvider clusterStatus, final String clusterId, final String workspaceId,
        final String user, final String password) throws InvalidSettingsException {

        final String httpPath = getHttpPath(clusterId, workspaceId);
        final String description = createDescription(clusterId, workspaceId);

        return new DatabricksUserDBConnectionController(jdbcUrl, httpPath, clusterStatus, description, user,
            password);
    }

    /**
     * Create controller connected to a cluster.
     *
     * @param jdbcUrl driver specific JDBC URL
     * @param clusterStatus cluster status provider
     * @param clusterId unique cluster identifier
     * @param workspaceId workspace identifier for Azure or 0
     * @param credential the credential provider to use
     * @throws InvalidSettingsException on unknown JDBC URL schema
     * @return new controller instance
     */
    static DBConnectionController createController(final String jdbcUrl,
        final DatabricksClusterStatusProvider clusterStatus, final String clusterId, final String workspaceId,
        final DatabricksAccessTokenCredential credential) throws InvalidSettingsException {

        final String httpPath = getHttpPath(clusterId, workspaceId);
        final String description = createDescription(clusterId, workspaceId);

        if (credential.getDatabricksTokenType() == DatabricksAccessTokenType.PERSONAL_ACCESS_TOKEN) {
            final String user = "token";
            final String password = getPersonalAccessToken(credential);

            return new DatabricksUserDBConnectionController(jdbcUrl, httpPath, clusterStatus, description, user,
                password);
        }

        return new DatabricksOAuth2DBConnectionController(jdbcUrl, httpPath, clusterStatus, description, credential);
    }

    private static String getPersonalAccessToken(final DatabricksAccessTokenCredential credential)
        throws InvalidSettingsException {
        try {
            return credential.getAccessToken();
        } catch (final IOException e) {
            throw new InvalidSettingsException(
                "Unable to load personal access token from input connection. Restart predecessor nodes.", e);
        }
    }

    private static String createDescription(final String clusterId, final String workspaceId) {
        final StringBuilder sb = new StringBuilder();
        sb.append("cluster=\"").append(clusterId).append('"');
        if (!StringUtils.isBlank(workspaceId)) {
            sb.append(", workspace=\"").append(workspaceId).append('"');
        }
        return sb.toString();
    }

    /**
     * Format httpPath parameter for Databricks JDBC URL.
     *
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
