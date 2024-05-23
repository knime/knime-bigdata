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
 *   2021-06-03 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.databricks.unity.filehandling.fs;

import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.credential.DatabricksWorkspaceAccessor;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig;

/**
 * Databricks Unity File System connection configuration.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class UnityFSConnectionConfig extends BaseFSConnectionConfig {

    private DatabricksWorkspaceAccessor m_workspace;

    private Duration m_connectionTimeout;

    private Duration m_readTimeout;

    /**
     * Private constructor, use {@link #builder()} instead.
     */
    private UnityFSConnectionConfig(final String workingDirectory) {
        super(workingDirectory, true);
    }

    DatabricksWorkspaceAccessor getWorkspace() {
        return m_workspace;
    }

    String getHost() {
        return m_workspace.getDatabricksWorkspaceUrl().getHost();
    }

    Duration getConnectionTimeout() {
        return m_connectionTimeout;
    }

    Duration getReadTimeout() {
        return m_readTimeout;
    }

    /**
     * @return the {@link FSLocationSpec} for a Databricks file system.
     */
    public DefaultFSLocationSpec createFSLocationSpec() {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED,
            String.format("%s:%s", UnityFSDescriptorProvider.FS_TYPE.getTypeId(), getHost()));
    }

    /**
     * Builder to create a {@link UnityFSConnectionConfig}.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class Builder {

        private DatabricksWorkspaceAccessor m_workspace;

        private String m_workingDirectory;

        private Duration m_connectionTimeout;

        private Duration m_readTimeout;

        Builder() {
        }

        /**
         * Set the workspace accessor.
         *
         * @param workspace A {@link DatabricksWorkspaceAccessor} which provides both the Databricks workspace URL as
         *            well as credentials.
         *
         * @return current builder instance
         */
        public Builder withWorkspace(final DatabricksWorkspaceAccessor workspace) {
            m_workspace = workspace;
            return this;
        }

        /**
         * Set the working directory.
         *
         * @param workingDirectory the working directory to use
         * @return current builder instance
         */
        public Builder withWorkingDirectory(final String workingDirectory) {
            m_workingDirectory = workingDirectory;
            return this;
        }

        /**
         * Set the connection timeout.
         *
         * @param timeout connection timeout to use
         * @return current builder instance
         */
        public Builder withConnectionTimeout(final Duration timeout) {
            m_connectionTimeout = timeout;
            return this;
        }

        /**
         * Set the read timeout.
         *
         * @param timeout read timeout to use
         * @return current builder instance
         */
        public Builder withReadTimeout(final Duration timeout) {
            m_readTimeout = timeout;
            return this;
        }

        /**
         * Build the configuration.
         *
         * @return configuration instance
         */
        public UnityFSConnectionConfig build() {
            CheckUtils.checkArgumentNotNull(m_workspace, "Workspace accessor must not be blank");
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_workingDirectory), "Working directory must not be blank");
            CheckUtils.checkArgumentNotNull(m_connectionTimeout, "Connection timeout required.");
            CheckUtils.checkArgumentNotNull(m_readTimeout, "Read timeout required.");

            final UnityFSConnectionConfig config = new UnityFSConnectionConfig(m_workingDirectory);
            config.m_workspace = m_workspace;
            config.m_connectionTimeout = m_connectionTimeout;
            config.m_readTimeout = m_readTimeout;

            return config;
        }
    }

    /**
     * Create a new builder.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

}
