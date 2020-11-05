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
 *   Nov 2, 2020 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.hadoop.filehandling.testing;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsConnection;
import org.knime.bigdata.hadoop.filehandling.fs.HdfsFileSystem;
import org.knime.bigdata.hadoop.filehandling.node.HdfsAuthenticationSettings.AuthType;
import org.knime.bigdata.hadoop.filehandling.node.HdfsConnectorNodeSettings;
import org.knime.bigdata.hadoop.filehandling.node.HdfsProtocol;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Provides factory methods to create {@link HdfsConnection} objects for testing purposes.
 *
 * @author Sascha Wolke, KNIME GmbH
 * @noreference This is testing code and its API is subject to change without notice.
 */
public class TestingHdfsConnectionFactory {

    private static final String WORKING_DIR = "/";

    /**
     * Creates a {@link FileSystemPortObjectSpec} with the given file system ID and settings from the given map of flow
     * variables.
     *
     * @param fsId file system connection identifier
     * @param flowVariables A map of flow variables that provide the connection settings.
     * @return a {@link FileSystemPortObjectSpec} instance
     */
    public static FileSystemPortObjectSpec createSpec(final String fsId,
        final Map<String, FlowVariable> flowVariables) {

        final URI uri = URI.create(TestflowVariable.getString(TestflowVariable.HDFS_URL, flowVariables));
        return new FileSystemPortObjectSpec(HdfsFileSystem.FS_TYPE, fsId, HdfsFileSystem.createFSLocationSpec(uri.getHost()));
    }

    /**
     * Creates a {@link FileSystemPortObjectSpec} with the given file system ID and settings from the given map of flow
     * variables.
     *
     * @param flowVariables A map of flow variables that provide the connection settings.
     * @return a {@link FileSystemPortObjectSpec} instance
     * @throws IOException
     */
    public static HdfsConnection createConnection(final Map<String, FlowVariable> flowVariables) throws IOException {
        final URI uri = URI.create(TestflowVariable.getString(TestflowVariable.HDFS_URL, flowVariables));
        final HdfsProtocol protocol = HdfsProtocol.valueOf(uri.getScheme().toUpperCase());
        final String authMethod = TestflowVariable.getString(TestflowVariable.HDFS_AUTH_METHOD, flowVariables);
        final HdfsConnectorNodeSettings settings;

        if (authMethod.equalsIgnoreCase("kerberos")) {
            settings = new HdfsConnectorNodeSettings(protocol, uri.getHost(), true, uri.getPort(),
                AuthType.KERBEROS, null, WORKING_DIR);
        } else if (authMethod.equalsIgnoreCase("password")) {
            final String user = TestflowVariable.getString(TestflowVariable.HDFS_USERNAME, flowVariables);
            settings = new HdfsConnectorNodeSettings(protocol, uri.getHost(), true, uri.getPort(),
                AuthType.SIMPLE, user, WORKING_DIR);
        } else if (authMethod.equalsIgnoreCase("none")) {
            settings = new HdfsConnectorNodeSettings(protocol, uri.getHost(), true, uri.getPort(),
                AuthType.SIMPLE, null, WORKING_DIR);
        } else {
            throw new IllegalArgumentException("Unsupported HDFS authentication method: " + authMethod);
        }

        return new HdfsConnection(settings);
    }
}
