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
package org.knime.bigdata.hadoop.filehandling.knox.node;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;

import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFSConnection;
import org.knime.bigdata.hadoop.filehandling.knox.fs.KnoxHdfsFileSystem;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * webHDFS via KNOX Connection node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KnoxHdfsConnectorNodeModel extends NodeModel {

    private static final String FILE_SYSTEM_NAME = KnoxHdfsFileSystem.FS_TYPE;

    private String m_fsId;

    private KnoxHdfsFSConnection m_connection;

    private KnoxHdfsConnectorNodeSettings m_settings = new KnoxHdfsConnectorNodeSettings();

    /**
     * Default constructor.
     */
    KnoxHdfsConnectorNodeModel() {
        super(new PortType[]{}, new PortType[]{FileSystemPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_fsId = FSConnectionRegistry.getInstance().getKey();
        return new PortObjectSpec[]{createSpec()};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        m_connection = new KnoxHdfsFSConnection(m_settings, getCredentialsProvider());
        FSConnectionRegistry.getInstance().register(m_fsId, m_connection);
        testConnection(m_connection);
        return new PortObject[]{new FileSystemPortObject(createSpec())};
    }

    @SuppressWarnings("resource")
    private static void testConnection(final KnoxHdfsFSConnection connection) throws IOException {
        try {
            Files.getLastModifiedTime(connection.getFileSystem().getPath("/"));
        } catch (NoSuchFileException e) {
            throw new IOException("Could not reach HDFS under the given URL. Please check the KNOX URL.", e);
        }
    }

    private FileSystemPortObjectSpec createSpec() {
        return new FileSystemPortObjectSpec(FILE_SYSTEM_NAME, m_fsId,
            KnoxHdfsFileSystem.createFSLocationSpec(m_settings.getHost()));
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        setWarningMessage("Connection no longer available. Please re-execute the node.");
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Not used
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO output) {
        m_settings.saveSettingsTo(output);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO input) throws InvalidSettingsException {
        m_settings.validateSettings(input);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO input) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(input);
    }

    @Override
    protected void onDispose() {
        // close the file system also when the workflow is closed
        reset();
    }

    @Override
    protected void reset() {
        if (m_connection != null) {
            m_connection.closeInBackground();
            m_connection = null;
        }
        m_fsId = null;
    }
}
