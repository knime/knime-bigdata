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
 *   Nov 6, 2020 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.bigdata.spark.local.node.create.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.local.context.LocalSparkContext;
import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings;
import org.knime.bigdata.spark.local.node.create.LocalSparkContextSettings.WorkingDirMode;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.WorkflowContext;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.connections.knimerelativeto.RelativeToUtil;
import org.knime.filehandling.core.connections.local.LocalFSConnection;
import org.knime.filehandling.core.connections.local.LocalFileSystem;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Utility to create {@link FileSystemPortObject} based file system output ports.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateFileSystemConnectionPortUtil implements CreateLocalBDEPortUtil {

    /**
     * Output test port type of this utility.
     */
    public static final PortType PORT_TYPE = FileSystemPortObject.TYPE;

    private final LocalSparkContextSettings m_settings;

    private String m_fsId;

    private FSConnection m_connection;

    /**
     * Default constructor.
     *
     * @param settings node settings
     */
    public CreateFileSystemConnectionPortUtil(final LocalSparkContextSettings settings) {
        m_settings = settings;
    }

    @Override
    public PortObjectSpec configure() throws InvalidSettingsException {
        m_fsId = FSConnectionRegistry.getInstance().getKey();

        if (m_settings.getWorkingDirectoryMode() == WorkingDirMode.USER_HOME
                && StringUtils.isBlank(System.getProperty("user.home"))) {
            throw new InvalidSettingsException("Unable to identify user home directory (system property 'user.home' not set or empty)");
        }

        return createSpec(m_fsId);
    }

    @Override
    public PortObject execute(final LocalSparkContext sparkContext, final ExecutionContext exec) throws Exception {
        final String workingDir = getWorkingDirectory();
        m_connection = new LocalFSConnection(workingDir, LocalFileSystem.CONNECTED_FS_LOCATION_SPEC);
        FSConnectionRegistry.getInstance().register(m_fsId, m_connection);
        testConnection(m_connection);
        return new FileSystemPortObject(createSpec(m_fsId));
    }

    @SuppressWarnings("resource")
    private static void testConnection(final FSConnection connection) throws IOException {
        Files.getLastModifiedTime(connection.getFileSystem().getWorkingDirectory());
    }

    private static FileSystemPortObjectSpec createSpec(final String fsId) {
        return new FileSystemPortObjectSpec(LocalFileSystem.FS_TYPE.getName(), fsId,
            LocalFileSystem.CONNECTED_FS_LOCATION_SPEC);
    }

    private String getWorkingDirectory() throws InvalidSettingsException, IOException {
        switch(m_settings.getWorkingDirectoryMode()) {
            case MANUAL:
                return m_settings.getManualWorkingDirectory();
            case USER_HOME:
                return System.getProperty("user.home");
            case WORKFLOW_DATA_AREA:
                final WorkflowContext workflowContext = RelativeToUtil.getWorkflowContext();
                final Path workflowLocation = workflowContext.getCurrentLocation().toPath().toAbsolutePath().normalize();
                final Path workflowDataDir = workflowLocation.resolve("data");
                Files.createDirectories(workflowDataDir);
                return workflowDataDir.toString();
            default:
                throw new InvalidSettingsException(
                    "Unable to identify workflow directory, unsupported mode: " + m_settings.getWorkingDirectoryMode());
        }
    }

    @Override
    public void onDispose() {
        // close the file system also when the workflow is closed
        reset();
    }

    @Override
    public void reset() {
        if (m_connection != null) {
            m_connection.closeInBackground();
            m_connection = null;
        }
        m_fsId = null;
    }

}
