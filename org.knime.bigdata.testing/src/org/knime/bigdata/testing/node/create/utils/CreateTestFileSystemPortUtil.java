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
package org.knime.bigdata.testing.node.create.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.eclipse.core.runtime.Platform;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.dbfs.filehandling.testing.TestingDbfsFSConnectionFactory;
import org.knime.bigdata.hadoop.filehandling.testing.TestingHdfsConnectionFactory;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.FlowVariable;
import org.knime.core.node.workflow.VariableType.StringType;
import org.knime.filehandling.core.connections.DefaultFSConnectionFactory;
import org.knime.filehandling.core.connections.DefaultFSLocationSpec;
import org.knime.filehandling.core.connections.FSCategory;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSLocationSpec;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.connections.meta.FSType;
import org.knime.filehandling.core.connections.meta.base.BaseFSConnectionConfig.BrowserRelativizationBehavior;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Utility to create {@link FileSystemPortObject} based file system output ports.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class CreateTestFileSystemPortUtil implements CreateTestPortUtil {

    /**
     * Output test port type of this utility.
     */
    public static final PortType PORT_TYPE = FileSystemPortObject.TYPE;

    private String m_fsId;

    private FSConnection m_connection;

    @Override
    public FileSystemPortObjectSpec configure(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars)
        throws InvalidSettingsException {

        m_fsId = FSConnectionRegistry.getInstance().getKey();
        return createSpec(m_fsId, sparkScheme, flowVars);
    }

    private static FileSystemPortObjectSpec createSpec(final String fsId, final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) throws InvalidSettingsException {

        switch (sparkScheme) {
            case SPARK_LOCAL:
                final FSLocationSpec locationSpec =
                    new DefaultFSLocationSpec(FSCategory.CONNECTED, FSType.LOCAL_FS.getTypeId());
                return new FileSystemPortObjectSpec(FSType.LOCAL_FS.getTypeId(), fsId, locationSpec);
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                return TestingHdfsConnectionFactory.createSpec(fsId, flowVars);
            case SPARK_DATABRICKS:
                return TestingDbfsFSConnectionFactory.createFileSystemPortSpec(fsId, flowVars);
            default:
                throw new InvalidSettingsException(
                    "Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    @Override
    public FileSystemPortObject execute(final SparkContextIDScheme sparkScheme, final Map<String, FlowVariable> flowVars,
        final ExecutionContext exec, final CredentialsProvider credentialsProvider) throws Exception {

        m_connection = createConnection(sparkScheme, flowVars);
        FSConnectionRegistry.getInstance().register(m_fsId, m_connection);
        testConnection(m_connection);
        return new FileSystemPortObject(createSpec(m_fsId, sparkScheme, flowVars));
    }

    @Override
    @SuppressWarnings("resource")
    public List<FlowVariable> createLocalTempDirAndVariables(final Map<String, FlowVariable> flowVars)
        throws Exception {

        try {
            final FSPath remoteTempDirParent = m_connection.getFileSystem()
                .getPath(TestflowVariable.getString(TestflowVariable.TMP_REMOTE_PARENT, flowVars));
            final FSPath remoteTempDir = FSFiles.createTempDirectory(remoteTempDirParent, "bde-tests-", "-remote");

            final var newVars = new ArrayList<FlowVariable>();
            newVars.add(new FlowVariable("tmp.remote", StringType.INSTANCE, remoteTempDir.toString()));

            final var legacyPath = LegacyHadoopPathUtil.convertToLegacyPath(remoteTempDir.toUri().getPath());
            newVars.add(new FlowVariable("tmp.remote.hadoop", StringType.INSTANCE, legacyPath));

            final var fsLocation = remoteTempDir.toFSLocation();
            newVars.add(new FlowVariable("tmp.remote.path", FSLocationVariableType.INSTANCE, fsLocation));

            return newVars;
        } catch (final IOException e) {
            throw new IOException("Failed to create remote temporary directory: " + e.getMessage(), e);
        }
    }

    /**
     * Create a {@link FSConnection} based on a {@link SparkContextIDScheme} using flow variables.
     *
     * @param flowVars test workflow variables
     * @param sparkScheme spark scheme to match
     * @return file system connection information
     * @throws InvalidSettingsException
     */
    private static FSConnection createConnection(final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) throws IOException, InvalidSettingsException {

        switch (sparkScheme) {
            case SPARK_LOCAL:
                final String workingDir = Platform.getLocation().toString();
                return DefaultFSConnectionFactory.createLocalFSConnection(workingDir, BrowserRelativizationBehavior.ABSOLUTE);
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                return TestingHdfsConnectionFactory.createConnection(flowVars);
            case SPARK_DATABRICKS:
                return TestingDbfsFSConnectionFactory.createFSConnection(flowVars);
            default:
                throw new InvalidSettingsException(
                    "Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    @SuppressWarnings("resource")
    private static void testConnection(final FSConnection connection) throws IOException {
        Files.getLastModifiedTime(connection.getFileSystem().getPath("/"));
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

    /**
     * @return file system ID of connection
     */
    public String getFileSystemID() {
        return m_fsId;
    }

    /**
     * @return {@code true} if file system is connected
     */
    public boolean isConnected() {
        return m_connection != null;
    }

}
