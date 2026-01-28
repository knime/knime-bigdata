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
 *   2020-10-14 (Alexander Bondaletov): created
 */
package org.knime.bigdata.dbfs.filehandling.node;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnection;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSConnectionConfig;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFSDescriptorProvider;
import org.knime.bigdata.dbfs.filehandling.fs.DbfsFileSystem;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.util.ThreadLocalHTTPAuthenticator;
import org.knime.core.webui.node.impl.WebUINodeModel;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Databricks DBFS Connector node.
 *
 * @author Alexander Bondaletov
 */
@SuppressWarnings({"deprecation", "restriction"})
class DbfsConnectorNodeModel extends WebUINodeModel<DbfsConnectorNodeParameters> {

    private String m_fsId;
    private DbfsFSConnection m_fsConnection;

    /**
     * Creates new instance.
     */
    DbfsConnectorNodeModel(final PortsConfiguration portsConfig, final boolean useWorkspaceConnection) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts(), DbfsConnectorNodeParameters.class);
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs, final DbfsConnectorNodeParameters params)
        throws InvalidSettingsException {

        m_fsId = FSConnectionRegistry.getInstance().getKey();

        final FileSystemPortObjectSpec fsPortSpec;
        if (inSpecs != null && inSpecs.length > 0 && inSpecs[0] != null) {
            if (!(inSpecs[0] instanceof DatabricksWorkspacePortObjectSpec)) {
                throw new InvalidSettingsException(
                    "Incompatible input connection. Connect the Databricks Workspace Connector output port.");
            }

            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inSpecs[0];
            if (spec.isPresent()) {
                try {
                    final DatabricksAccessTokenCredential credential =
                        spec.resolveCredential(DatabricksAccessTokenCredential.class);
                    fsPortSpec = createSpec(params.toFSConnectionConfig(spec, credential));
                } catch (final NoSuchCredentialException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
            } else {
                fsPortSpec = null;
            }
        } else {
            fsPortSpec = createSpec(params.toFSConnectionConfig(getCredentialsProvider()));
        }

        return new PortObjectSpec[]{fsPortSpec};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec,
        final DbfsConnectorNodeParameters params) throws Exception {

        final DbfsFSConnectionConfig config;
        if (inObjects != null && inObjects.length > 0 && inObjects[0] != null) {
            final DatabricksWorkspacePortObjectSpec spec = ((DatabricksWorkspacePortObject)inObjects[0]).getSpec();
            final DatabricksAccessTokenCredential credential =
                spec.resolveCredential(DatabricksAccessTokenCredential.class);
            config = params.toFSConnectionConfig(spec, credential);
        } else {
            config = params.toFSConnectionConfig(getCredentialsProvider());
        }

        m_fsConnection = new DbfsFSConnection(config);
        testConnection(m_fsConnection);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);
        return new PortObject[]{new FileSystemPortObject(createSpec(config))};
    }

    private FileSystemPortObjectSpec createSpec(final DbfsFSConnectionConfig config) {
        return new FileSystemPortObjectSpec(DbfsFSDescriptorProvider.FS_TYPE.getTypeId(), //
            m_fsId, //
            config.createFSLocationSpec());
    }

    @SuppressWarnings("resource")
    private static void testConnection(final DbfsFSConnection connection) throws IOException {
        try (final Closeable c = ThreadLocalHTTPAuthenticator.suppressAuthenticationPopups()) {
            ((DbfsFileSystem)connection.getFileSystem()).getClient().list(DbfsFileSystem.PATH_SEPARATOR);
        }
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        setWarningMessage("Connection no longer available. Please re-execute the node.");
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
            throws IOException, CanceledExecutionException {
        // nothing to save
    }

    @Override
    protected void onDispose() {
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
