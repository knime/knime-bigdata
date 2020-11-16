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
 *   2020-11-13 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.spark.core.databricks.context;

import java.util.Optional;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSConnectionRegistry;

/**
 * Databricks Spark context configuration using a {@link ConnectionInformation} file system connection.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextFileSystemConfig extends DatabricksSparkContextConfig {

    private final String m_fileSystemId;

    /**
     * Constructor for token based authentication.
     *
     * @param sparkVersion Spark version of the cluster
     * @param databricksUrl Deployment URL of the cluster
     * @param clusterId ID if cluster
     * @param authToken authentication token
     * @param stagingAreaFolder Staging area in DBFS
     * @param terminateClusterOnDestroy terminate cluster on context destroy
     * @param connectionTimeoutSeconds Connection timeout
     * @param receiveTimeoutSeconds Receive timeout
     * @param jobCheckFrequencySeconds
     * @param sparkContextId
     * @param fileSystemId
     */
    public DatabricksSparkContextFileSystemConfig(final SparkVersion sparkVersion, final String databricksUrl,
        final String clusterId, final String authToken, final String stagingAreaFolder,
        final boolean terminateClusterOnDestroy, final int connectionTimeoutSeconds, final int receiveTimeoutSeconds,
        final int jobCheckFrequencySeconds, final SparkContextID sparkContextId,
        final String fileSystemId) {

        super(sparkVersion, databricksUrl, clusterId, authToken, stagingAreaFolder, terminateClusterOnDestroy,
            connectionTimeoutSeconds, receiveTimeoutSeconds, jobCheckFrequencySeconds, sparkContextId);
        m_fileSystemId = fileSystemId;
    }

    /**
     * Constructor for user and password authentication.
     *
     * @param sparkVersion Spark version of the cluster
     * @param databricksUrl Deployment URL of the cluster
     * @param clusterId ID if cluster
     * @param user user to use for authentication
     * @param password password to use for authentication
     * @param stagingAreaFolder Staging area in DBFS
     * @param terminateClusterOnDestroy terminate cluster on context destroy
     * @param connectionTimeoutSeconds Connection timeout
     * @param receiveTimeoutSeconds Receive timeout
     * @param jobCheckFrequencySeconds
     * @param sparkContextId
     * @param fileSystemId
     */
    public DatabricksSparkContextFileSystemConfig(final SparkVersion sparkVersion, final String databricksUrl,
        final String clusterId, final String user, final String password, final String stagingAreaFolder,
        final boolean terminateClusterOnDestroy, final int connectionTimeoutSeconds, final int receiveTimeoutSeconds,
        final int jobCheckFrequencySeconds, final SparkContextID sparkContextId,
        final String fileSystemId) {

        super(sparkVersion, databricksUrl, clusterId, user, password, stagingAreaFolder, terminateClusterOnDestroy,
            connectionTimeoutSeconds, receiveTimeoutSeconds, jobCheckFrequencySeconds, sparkContextId);
        m_fileSystemId = fileSystemId;
    }

    @Override
    @SuppressWarnings("resource")
    public RemoteFSController createRemoteFSController() throws KNIMESparkException {
        final Optional<FSConnection> fsConnection = FSConnectionRegistry.getInstance().retrieve(m_fileSystemId);

        if (fsConnection.isPresent()) {
            return new RemoteFSControllerNIO(fsConnection.get(), getStagingAreaFolder(), getClusterId());
        } else {
            throw new KNIMESparkException("File system connection unavailable. Restart connector node.");
        }
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((m_fileSystemId == null) ? 0 : m_fileSystemId.hashCode());
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DatabricksSparkContextFileSystemConfig other = (DatabricksSparkContextFileSystemConfig)obj;
        if (m_fileSystemId == null) {
            if (other.m_fileSystemId != null) {
                return false;
            }
        } else if (!m_fileSystemId.equals(other.m_fileSystemId)) {
            return false;
        }
        return true;
    }

}
