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
package org.knime.bigdata.hadoop.filehandling.knox.fs;

import java.net.URI;
import java.time.Duration;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig;

/**
 * WebHDFS via KNOX connection configuration.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class KnoxHdfsFSConnectionConfig extends BaseFSConnectionConfig {
    private String m_endpointUrl;
    private String m_username;

    private String m_password;

    private Duration m_connectionTimeout;

    private Duration m_receiveTimeout;

    /**
     * Private constructor, use {@link #builder()} instead.
     */
    private KnoxHdfsFSConnectionConfig(final String workingDirectory,
        final BrowserRelativizationBehavior relativizationBehavior) {
        super(workingDirectory, true, relativizationBehavior);
    }

    String getEndpointUrl() {
        return m_endpointUrl;
    }

    String getHost() {
        return URI.create(m_endpointUrl).getHost();
    }

    String getUsername() {
        return m_username;
    }

    String getPassword() {
        return m_password;
    }

    Duration getConnectionTimeout() {
        return m_connectionTimeout;
    }

    Duration getReceiveTimeout() {
        return m_receiveTimeout;
    }

    /**
     * @return the {@link FSLocationSpec} for a KNOX file system.
     */
    public DefaultFSLocationSpec createFSLocationSpec() {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED,
            String.format("%s:%s", KnoxHdfsFSDescriptorProvider.FS_TYPE.getTypeId(), getHost()));
    }

    /**
     * Builder to create a {@link KnoxHdfsFSConnectionConfig}.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class Builder {
        private String m_endpointUrl;

        private String m_workingDirectory;

        private String m_username;

        private String m_password;

        private Duration m_connectionTimeout;

        private Duration m_receiveTimeout;

        private BrowserRelativizationBehavior m_relativizationBehavior = BrowserRelativizationBehavior.ABSOLUTE;

        Builder() {
        }

        /**
         * Set the endpoint URL.
         *
         * @param url URL of endpoint
         * @return current builder instance
         */
        public Builder withEndpointUrl(final String url) {
            m_endpointUrl = url;
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
         * Use username and password authentication.
         *
         * @param username username to use
         * @param password password to use
         * @return current builder instance
         */
        public Builder withUserAndPassword(final String username, final String password) {
            m_username = username;
            m_password = password;
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
        public Builder withReceiveTimeout(final Duration timeout) {
            m_receiveTimeout = timeout;
            return this;
        }

        /**
         * @param relativizationBehavior the browser relativization behavior
         * @return current builder instance
         */
        public Builder withRelativizationBehavior(final BrowserRelativizationBehavior relativizationBehavior) {
            m_relativizationBehavior = relativizationBehavior;
            return this;
        }

        /**
         * Build the configuration.
         *
         * @return configuration instance
         */
        public KnoxHdfsFSConnectionConfig build() {
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_endpointUrl), "Deployment URL must not be blank");
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_workingDirectory), "Working directory must not be blank");
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_username), "Username must not be blank");
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_password), "Password must not be blank");
            CheckUtils.checkArgumentNotNull(m_connectionTimeout, "Connection timeout required.");
            CheckUtils.checkArgumentNotNull(m_receiveTimeout, "Read timeout required.");

            final KnoxHdfsFSConnectionConfig config =
                new KnoxHdfsFSConnectionConfig(m_workingDirectory, m_relativizationBehavior);
            config.m_endpointUrl = m_endpointUrl;
            config.m_username = m_username;
            config.m_password = m_password;
            config.m_connectionTimeout = m_connectionTimeout;
            config.m_receiveTimeout = m_receiveTimeout;

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
