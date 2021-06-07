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
package org.knime.bigdata.spark.core.databricks.node.create;

import java.io.IOException;
import java.nio.file.Files;

import org.knime.bigdata.database.databricks.DatabricksDBConnectionController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.databricks.context.DatabricksClusterStatusProvider;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContext;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.database.connection.DBConnectionController;
import org.knime.database.port.DBSessionPortObject;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Node model of the "Create Spark Context (Databricks)" node using a NIO file system.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContextCreatorNodeModel2
    extends AbstractDatabricksSparkContextCreatorNodeModel<DatabricksSparkContextCreatorNodeSettings2> {

    private String m_fsId;

    private FSConnection m_fsConnection;

    /**
     * Constructor.
     */
    DatabricksSparkContextCreatorNodeModel2() {
        super(new PortType[0],
            new PortType[]{DBSessionPortObject.TYPE, FileSystemPortObject.TYPE, SparkContextPortObject.TYPE},
            new DatabricksSparkContextCreatorNodeSettings2());
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_settings.validateDeeper();

        initializeTypeMapping();
        m_settings.validateDriverRegistered();
        m_settings.validate(m_variableContext);

        m_fsId = FSConnectionRegistry.getInstance().getKey();
        final FileSystemPortObjectSpec fsPortSpec = m_settings.createFileSystemSpec(m_fsId, getCredentialsProvider());
        configureSparkContext(m_sparkContextId, m_fsId, m_settings, getCredentialsProvider());

        final PortObjectSpec sparkPortSpec;
        if (m_settings.isCreateSparkContextSet()) {
            sparkPortSpec = new SparkContextPortObjectSpec(m_sparkContextId);
        } else {
            sparkPortSpec = InactiveBranchPortObjectSpec.INSTANCE;
        }

        return new PortObjectSpec[]{null, fsPortSpec, sparkPortSpec};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setProgress(0.1, "Connecting to Databricks File System");
        m_fsConnection = m_settings.createDatabricksFSConnection(getCredentialsProvider());
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);
        testFileSystemConnection(m_fsConnection);
        final FileSystemPortObject fsPortObject =
            new FileSystemPortObject(m_settings.createFileSystemSpec(m_fsId, getCredentialsProvider()));

        // configure context
        exec.setProgress(0.2, "Configuring Databricks Spark context");
        configureSparkContext(m_sparkContextId, m_fsId, m_settings, getCredentialsProvider());

        // start cluster
        exec.setProgress(0.3, "Starting cluster on Databricks");
        final DatabricksSparkContext sparkContext = (DatabricksSparkContext)SparkContextManager
            .<DatabricksSparkContextConfig> getOrCreateSparkContext(m_sparkContextId);
        sparkContext.startCluster(exec);

        // create spark context
        final PortObject sparkPortObject;
        if (m_settings.isCreateSparkContextSet()) {
            exec.setProgress(0.8, "Creating context");
            sparkContext.ensureOpened(true, exec.createSubProgress(0.1));
            sparkPortObject = new SparkContextPortObject(m_sparkContextId);
        } else {
            sparkPortObject = InactiveBranchPortObject.INSTANCE;
        }

        // open the JDBC connection AFTER starting the cluster, otherwise Databricks returns 503... (cluster starting)
        exec.setProgress(0.9, "Configuring Databricks DB connection");
        final PortObject dbPortObject = createDBPort(exec, sparkContext.getClusterStatusHandler());

        return new PortObject[]{dbPortObject, fsPortObject, sparkPortObject};
    }

    @SuppressWarnings("resource")
    private static void testFileSystemConnection(final FSConnection connection) throws IOException {
        Files.getLastModifiedTime(connection.getFileSystem().getWorkingDirectory());
    }

    /**
     * Internal method to ensure that the given Spark context is configured.
     *
     * @param sparkContextId Identifies the Spark context to configure.
     * @param connInfo
     * @param settings The settings from which to configure the context.
     * @param credentialsProvider credentials provider to use
     */
    private static void configureSparkContext(final SparkContextID sparkContextId,
        final String fileSystemId, final DatabricksSparkContextCreatorNodeSettings2 settings,
        final CredentialsProvider credentialsProvider) {

        try {
            final SparkContext<DatabricksSparkContextConfig> sparkContext =
                SparkContextManager.getOrCreateSparkContext(sparkContextId);
            final DatabricksSparkContextConfig config =
                settings.createContextConfig(sparkContextId, fileSystemId, credentialsProvider);

            final boolean configApplied = sparkContext.ensureConfigured(config, true);
            if (!configApplied) {
                // this should never ever happen
                throw new RuntimeException("Failed to apply Spark context settings.");
            }
        } catch (final InvalidSettingsException e) {
            // this should never ever happen
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    protected void createDummyContext(final String previousContextID) {
        final String dummyFileSystemId = "dummy-file-system-id";
        configureSparkContext(new SparkContextID(previousContextID), dummyFileSystemId, m_settings,
            getCredentialsProvider());
    }

    @Override
    protected DBConnectionController createConnectionController(final DatabricksClusterStatusProvider clusterStatus)
        throws InvalidSettingsException {

        final CredentialsProvider cp = getCredentialsProvider();
        final String username;
        final String password;
        if (m_settings.getAuthenticationSettings().useTokenAuth()) {
            username = "token";
            password = m_settings.getAuthenticationSettings().getToken(cp);
        } else {
            username = m_settings.getAuthenticationSettings().getUser(cp);
            password = m_settings.getAuthenticationSettings().getPassword(cp);
        }

        return new DatabricksDBConnectionController(m_settings.getDBUrl(), clusterStatus, m_settings.getClusterId(),
            m_settings.getWorkspaceId(), username, password);
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsForModel(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsForModel(settings);
    }

    @Override
    public void onDisposeInternal() {
        super.onDisposeInternal();

        // close the file system also when the workflow is closed
        resetFileSystemConnection();
    }

    @Override
    public void resetInternal() {
        super.resetInternal();
        resetFileSystemConnection();
    }

    private void resetFileSystemConnection() {
        if (m_fsConnection != null) {
            m_fsConnection.closeInBackground();
            m_fsConnection = null;
        }
        m_fsId = null;
    }
}
