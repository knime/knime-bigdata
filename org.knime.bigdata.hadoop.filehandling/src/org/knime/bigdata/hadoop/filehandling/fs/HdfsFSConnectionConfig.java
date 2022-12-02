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
package org.knime.bigdata.hadoop.filehandling.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig;


/**
 * HDFS connection configuration.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public final class HdfsFSConnectionConfig extends BaseFSConnectionConfig {
    private String m_hadoopScheme;

    private String m_host;

    private int m_port;

    /**
     * If {@code true}, use Kerberos or simple authentication otherwise.
     */
    private boolean m_useKerberos = true;

    private String m_username;

    /**
     * Private constructor, use {@link #builder()} instead.
     */
    private HdfsFSConnectionConfig(final String workingDirectory, final BrowserRelativizationBehavior relativizationBehavior) {
        super(workingDirectory, true, relativizationBehavior);
    }

    String getHadoopScheme() {
        return m_hadoopScheme;
    }

    String getHost() {
        return m_host;
    }

    int getPort() {
        return m_port;
    }

    /**
     * @return Hadoop compatible URI using scheme, host and port
     * @throws IOException
     */
    URI getHadopURI() throws IOException {
        try {
            return new URI(m_hadoopScheme, null, m_host, m_port, null, null, null);
        } catch (final URISyntaxException e) {
            throw new IOException("Failed to create Hadoop URI", e);
        }
    }

    /**
     * @return {@code true} if Kerberos or {@code false} if simple authentication should be used
     */
    boolean useKerberos() {
        return m_useKerberos;
    }

    String getUsername() {
        return m_username;
    }

    /**
     * @return the {@link FSLocationSpec} for a HDFS file system.
     */
    public DefaultFSLocationSpec createFSLocationSpec() {
        return new DefaultFSLocationSpec(FSCategory.CONNECTED,
            String.format("%s:%s", HdfsFSDescriptorProvider.FS_TYPE.getTypeId(), getHost()));
    }

    /**
     * Builder to create a {@link HdfsFSConnectionConfig}.
     *
     * @author Sascha Wolke, KNIME GmbH
     */
    public static class Builder {
        private String m_hadoopScheme;

        private String m_host;

        private int m_port = -1;

        private String m_workingDirectory;

        private boolean m_useKerberos = true;

        private String m_username;

        private BrowserRelativizationBehavior m_relativizationBehavior = BrowserRelativizationBehavior.ABSOLUTE;

        Builder() {
        }

        /**
         * Connection endpoint.
         *
         * @param hadoopScheme a Hadoop scheme like hdfs, webhdfs or swebhdfs
         * @param host endpoint host
         * @param port endpoint port
         * @return current builder instance
         */
        public Builder withEndpoint(final String hadoopScheme, final String host, final int port) {
            m_hadoopScheme = hadoopScheme;
            m_host = host;
            m_port = port;
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
         * Use Kerberos authentication.
         * @return current builder instance
         */
        public Builder withKerberosAuthentication() {
            m_useKerberos = true;
            return this;
        }

        /**
         * Use simple authentication with given username.
         * @param username username to use
         * @return current builder instance
         */
        public Builder withSimpleAuthentication(final String username) {
            m_useKerberos = false;
            m_username = username;
            return this;
        }

        /**
         * @param relativizationBehavior The browser relativization behavior.
         * @return current builder instance.
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
        public HdfsFSConnectionConfig build() {
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_hadoopScheme), "Hadoop scheme must not be blank");
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_host), "Host must not be blank");
            CheckUtils.checkArgument(m_port > 0, "Port must be a postive number");
            CheckUtils.checkArgument(StringUtils.isNotBlank(m_workingDirectory), "Working directory must not be blank");

            final HdfsFSConnectionConfig config = new HdfsFSConnectionConfig(m_workingDirectory, m_relativizationBehavior);
            config.m_hadoopScheme = m_hadoopScheme;
            config.m_host = m_host;
            config.m_port = m_port;
            config.m_useKerberos = m_useKerberos;

            if (!m_useKerberos) {
                CheckUtils.checkArgument(StringUtils.isNotBlank(m_username), "Username must not be blank in simple authentication mode");
                config.m_username = m_username;
            }

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
