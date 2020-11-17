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
 */
package org.knime.bigdata.spark.core.databricks.context;

import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * {@link SparkContextConfig} implementation for a Spark context running on Databricks. This class holds all required
 * information to create and configure such a context.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public abstract class DatabricksSparkContextConfig implements SparkContextConfig {

    private final SparkVersion m_sparkVersion;

    private final String m_databricksUrl;

    private final String m_clusterId;

    private final boolean m_useToken;

    private final String m_authToken;

    private final String m_user;

    private final String m_password;

    private final String m_stagingAreaFolder;

    private final boolean m_terminateClusterOnDestroy;

    private final int m_connectionTimeoutSeconds;

    private final int m_receiveTimeoutSeconds;

    private final int m_jobCheckFrequencySeconds;

    private final SparkContextID m_sparkContextId;

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
     */
    public DatabricksSparkContextConfig(final SparkVersion sparkVersion, final String databricksUrl,
        final String clusterId, final String authToken, final String stagingAreaFolder,
        final boolean terminateClusterOnDestroy, final int connectionTimeoutSeconds,
        final int receiveTimeoutSeconds, final int jobCheckFrequencySeconds, final SparkContextID sparkContextId) {

        m_sparkVersion = sparkVersion;
        m_databricksUrl = databricksUrl;
        m_clusterId = clusterId;
        m_useToken = true;
        m_authToken = authToken;
        m_user = null;
        m_password = null;
        m_stagingAreaFolder = stagingAreaFolder;
        m_terminateClusterOnDestroy = terminateClusterOnDestroy;
        m_connectionTimeoutSeconds = connectionTimeoutSeconds;
        m_receiveTimeoutSeconds = receiveTimeoutSeconds;
        m_jobCheckFrequencySeconds = jobCheckFrequencySeconds;
        m_sparkContextId = sparkContextId;
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
     */
    public DatabricksSparkContextConfig(final SparkVersion sparkVersion, final String databricksUrl,
        final String clusterId, final String user, final String password, final String stagingAreaFolder,
        final boolean terminateClusterOnDestroy, final int connectionTimeoutSeconds,
        final int receiveTimeoutSeconds, final int jobCheckFrequencySeconds, final SparkContextID sparkContextId) {

        m_sparkVersion = sparkVersion;
        m_databricksUrl = databricksUrl;
        m_clusterId = clusterId;
        m_useToken = false;
        m_authToken = null;
        m_user = user;
        m_password = password;
        m_stagingAreaFolder = stagingAreaFolder;
        m_terminateClusterOnDestroy = terminateClusterOnDestroy;
        m_connectionTimeoutSeconds = connectionTimeoutSeconds;
        m_receiveTimeoutSeconds = receiveTimeoutSeconds;
        m_jobCheckFrequencySeconds = jobCheckFrequencySeconds;
        m_sparkContextId = sparkContextId;
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteObjectsOnDispose() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useCustomSparkSettings() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getCustomSparkSettings() {
        throw new RuntimeException("Databricks context does not support custom spark settings");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getSparkContextID() {
        return m_sparkContextId;
    }

    /**
     * @return {@link RemoteFSController} instance
     * @throws KNIMESparkException
     */
    public abstract RemoteFSController createRemoteFSController() throws KNIMESparkException;

    /**
     * @return the http(s) URL for Databricks
     */
    public String getDatabricksUrl() {
        return m_databricksUrl;
    }

    /**
     * @return the cluster ID in Databricks deployment
     */
    public String getClusterId() {
        return m_clusterId;
    }

    /**
     * @return <code>true</code> if token should be used, <code>false</code> if username and password should be used
     */
    public boolean useToken() {
        return m_useToken;
    }

    /**
     * @return authentication token
     */
    public String getAuthToken() {
        return m_authToken;
    }

    /**
     * @return authentication user
     */
    public String getUser() {
        return m_user;
    }

    /**
     * @return authentication password
     */
    public String getPassword() {
        return m_password;
    }

    /**
     * @return the staging area folder to use, or null if none was set.
     */
    public String getStagingAreaFolder() {
        return m_stagingAreaFolder;
    }

    /**
     * @return <code>true</code> if cluster should be terminated on context destroy
     */
    public boolean terminateClusterOnDestroy() {
        return m_terminateClusterOnDestroy;
    }

    /**
     * @return the TCP socket connection timeout when making connections to Databricks.
     */
    protected int getConnectionTimeoutSeconds() {
        return m_connectionTimeoutSeconds;
    }

    /**
     *
     * @return the TCP socket receive timeout in seconds when making connections to Databricks.
     */
    protected int getReceiveTimeoutSeconds() {
        return m_receiveTimeoutSeconds;
    }

    /**
     *
     * @return how often in seconds to poll the status of a Spark job running on Databricks.
     */
    protected int getJobCheckFrequencySeconds() {
        return m_jobCheckFrequencySeconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_authToken == null) ? 0 : m_authToken.hashCode());
        result = prime * result + ((m_clusterId == null) ? 0 : m_clusterId.hashCode());
        result = prime * result + m_connectionTimeoutSeconds;
        result = prime * result + ((m_databricksUrl == null) ? 0 : m_databricksUrl.hashCode());
        result = prime * result + m_jobCheckFrequencySeconds;
        result = prime * result + ((m_password == null) ? 0 : m_password.hashCode());
        result = prime * result + m_receiveTimeoutSeconds;
        result = prime * result + ((m_sparkContextId == null) ? 0 : m_sparkContextId.hashCode());
        result = prime * result + ((m_sparkVersion == null) ? 0 : m_sparkVersion.hashCode());
        result = prime * result + ((m_stagingAreaFolder == null) ? 0 : m_stagingAreaFolder.hashCode());
        result = prime * result + (m_terminateClusterOnDestroy ? 1231 : 1237);
        result = prime * result + (m_useToken ? 1231 : 1237);
        result = prime * result + ((m_user == null) ? 0 : m_user.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        DatabricksSparkContextConfig other = (DatabricksSparkContextConfig)obj;
        if (m_authToken == null) {
            if (other.m_authToken != null) {
                return false;
            }
        } else if (!m_authToken.equals(other.m_authToken)) {
            return false;
        }
        if (m_clusterId == null) {
            if (other.m_clusterId != null) {
                return false;
            }
        } else if (!m_clusterId.equals(other.m_clusterId)) {
            return false;
        }
        if (m_connectionTimeoutSeconds != other.m_connectionTimeoutSeconds) {
            return false;
        }
        if (m_databricksUrl == null) {
            if (other.m_databricksUrl != null) {
                return false;
            }
        } else if (!m_databricksUrl.equals(other.m_databricksUrl)) {
            return false;
        }
        if (m_jobCheckFrequencySeconds != other.m_jobCheckFrequencySeconds) {
            return false;
        }
        if (m_password == null) {
            if (other.m_password != null) {
                return false;
            }
        } else if (!m_password.equals(other.m_password)) {
            return false;
        }
        if (m_receiveTimeoutSeconds != other.m_receiveTimeoutSeconds) {
            return false;
        }
        if (m_sparkContextId == null) {
            if (other.m_sparkContextId != null) {
                return false;
            }
        } else if (!m_sparkContextId.equals(other.m_sparkContextId)) {
            return false;
        }
        if (m_sparkVersion == null) {
            if (other.m_sparkVersion != null) {
                return false;
            }
        } else if (!m_sparkVersion.equals(other.m_sparkVersion)) {
            return false;
        }
        if (m_stagingAreaFolder == null) {
            if (other.m_stagingAreaFolder != null) {
                return false;
            }
        } else if (!m_stagingAreaFolder.equals(other.m_stagingAreaFolder)) {
            return false;
        }
        if (m_terminateClusterOnDestroy != other.m_terminateClusterOnDestroy) {
            return false;
        }
        if (m_useToken != other.m_useToken) {
            return false;
        }
        if (m_user == null) {
            if (other.m_user != null) {
                return false;
            }
        } else if (!m_user.equals(other.m_user)) {
            return false;
        }
        return true;
    }

    @Override
    public String getContextName() {
        return "KNIME Databricks Spark context on cluster " + m_clusterId;
    }
}
