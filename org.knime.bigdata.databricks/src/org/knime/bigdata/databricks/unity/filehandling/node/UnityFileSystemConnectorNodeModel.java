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
 *   2024-05-24 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.unity.filehandling.node;

import java.io.IOException;

import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnection;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnectionConfig;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSDescriptorProvider;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFileSystem;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.webui.node.impl.WebUINodeConfiguration;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Connector node for the Databricks Unity File System Connector.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
public class UnityFileSystemConnectorNodeModel extends WebUINodeModel<UnityFileSystemConnectorSettings> {

    private String m_fsId;

    private UnityFSConnection m_fsConnection;

    UnityFileSystemConnectorNodeModel(final WebUINodeConfiguration configuration) {
        super(configuration, UnityFileSystemConnectorSettings.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
        final UnityFileSystemConnectorSettings settings)
        throws InvalidSettingsException {

        m_fsId = FSConnectionRegistry.getInstance().getKey();

        PortObjectSpec[] toReturn = new PortObjectSpec[]{null};
        if (inSpecs[0] != null) {
            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inSpecs[0];
            if (spec.isPresent()) {
                try {
                    final DatabricksAccessTokenCredential credential =
                        spec.resolveCredential(DatabricksAccessTokenCredential.class);
                    final UnityFSConnectionConfig config = createFSConnectionConfig(credential, spec, settings);
                    toReturn = new PortObjectSpec[]{createSpec(config)};
                } catch (final NoSuchCredentialException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
            }
        }
        return toReturn;
    }

    private static UnityFSConnectionConfig createFSConnectionConfig(final DatabricksAccessTokenCredential credential,
        final DatabricksWorkspacePortObjectSpec spec, final UnityFileSystemConnectorSettings settings) {

        return UnityFSConnectionConfig.builder() //
            .withCredential(credential) //
            .withWorkingDirectory(settings.m_workingDirectory) //
            .withConnectionTimeout(spec.getConnectionTimeout()) //
            .withReadTimeout(spec.getConnectionTimeout()) //
            .build();
    }

    private FileSystemPortObjectSpec createSpec(final UnityFSConnectionConfig config) {
        return new FileSystemPortObjectSpec(UnityFSDescriptorProvider.FS_TYPE.getTypeId(), //
            m_fsId, //
            config.createFSLocationSpec());
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final UnityFileSystemConnectorSettings settings) throws Exception {

        final DatabricksWorkspacePortObjectSpec spec = ((DatabricksWorkspacePortObject)inObjects[0]).getSpec();
        final DatabricksAccessTokenCredential credential =
            spec.resolveCredential(DatabricksAccessTokenCredential.class);
        final UnityFSConnectionConfig config = createFSConnectionConfig(credential, spec, settings);
        m_fsConnection = new UnityFSConnection(config);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);

        testConnection(m_fsConnection);

        return new PortObject[]{new FileSystemPortObject(createSpec(config))};
    }

    @SuppressWarnings("resource")
    private static void testConnection(final UnityFSConnection connection) throws IOException {
        ((UnityFileSystem)connection.getFileSystem()).testConnection();
    }

    @Override
    protected void onDispose() {
        // close the file system also when the workflow is closed
        reset();
    }

    @Override
    protected void reset() {
        if (m_fsConnection != null) {
            m_fsConnection.closeInBackground();
            m_fsConnection = null;
        }
        m_fsId = null;
    }
}
