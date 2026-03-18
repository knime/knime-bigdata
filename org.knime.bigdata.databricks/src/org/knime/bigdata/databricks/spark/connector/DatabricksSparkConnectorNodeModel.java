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
 *   2026-03-10 (Sascha Wolke, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.bigdata.databricks.spark.connector;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.UUID;

import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnection;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnectionConfig;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSDescriptorProvider;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObject;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.databricks.DatabricksSparkContextProvider;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContext;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextFileSystemConfig;
import org.knime.bigdata.spark.core.node.SparkWebUINodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.core.util.BackgroundTasks;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.util.context.DestroyAndDisposeSparkContextTask;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.credentials.base.CredentialPortObject;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSConnectionRegistry;
import org.knime.filehandling.core.port.FileSystemPortObject;
import org.knime.filehandling.core.port.FileSystemPortObjectSpec;

/**
 * Node implementation for the Databricks Spark Connector node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
public class DatabricksSparkConnectorNodeModel extends SparkWebUINodeModel<DatabricksSparkConnectorNodeParameters> {

    private SparkContextID m_sparkContextId;

    private String m_fsId;

    private UnityFSConnection m_fsConnection;

    DatabricksSparkConnectorNodeModel() {
        super(new PortType[]{CredentialPortObject.TYPE}, // input
            new PortType[]{SparkContextPortObject.TYPE, FileSystemPortObject.TYPE}, // output
            DatabricksSparkConnectorNodeParameters.class);
        resetContextID();
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs,
        final DatabricksSparkConnectorNodeParameters settings) throws InvalidSettingsException {
        settings.validateOnConfigure();

        m_fsId = FSConnectionRegistry.getInstance().getKey();

        FileSystemPortObjectSpec fsPortSpec = null;

        if (inSpecs[0] != null) {
            if (!(inSpecs[0] instanceof DatabricksWorkspacePortObjectSpec)) {
                throw new InvalidSettingsException(
                    "Incompatible input connection. Connect the Databricks Workspace Connector output port.");
            }
            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inSpecs[0];
            if (spec.isPresent()) {
                try {
                    final DatabricksAccessTokenCredential credential =
                        spec.resolveCredential(DatabricksAccessTokenCredential.class);
                    final UnityFSConnectionConfig config = settings.createStagingFSConnectionConfig(credential, spec);
                    fsPortSpec = createSpec(config);
                } catch (final NoSuchCredentialException ex) {
                    throw new InvalidSettingsException(ex.getMessage(), ex);
                }
            }
        }

        final PortObjectSpec sparkPortSpec = new SparkContextPortObjectSpec(m_sparkContextId);
        return new PortObjectSpec[]{sparkPortSpec, fsPortSpec};
    }

    private FileSystemPortObjectSpec createSpec(final UnityFSConnectionConfig config) {
        return new FileSystemPortObjectSpec(UnityFSDescriptorProvider.FS_TYPE.getTypeId(), //
            m_fsId, //
            config.createFSLocationSpec());
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inObjects, final ExecutionContext exec,
        final DatabricksSparkConnectorNodeParameters settings) throws Exception {

        final DatabricksWorkspacePortObjectSpec spec = ((DatabricksWorkspacePortObject)inObjects[0]).getSpec();
        final DatabricksAccessTokenCredential credential =
            spec.resolveCredential(DatabricksAccessTokenCredential.class);

        // connect file system
        exec.setProgress(0.1, "Connecting Unity File System");
        final UnityFSConnectionConfig config = settings.createStagingFSConnectionConfig(credential, spec);
        m_fsConnection = new UnityFSConnection(config);
        FSConnectionRegistry.getInstance().register(m_fsId, m_fsConnection);
        testFileSystemConnection(m_fsConnection);
        final FileSystemPortObject fsPortObject = new FileSystemPortObject(createSpec(config));

        // configure context
        exec.setProgress(0.2, "Configuring Databricks Spark context");
        configureSparkContext(m_sparkContextId, //
            settings.createContextConfig(m_sparkContextId, m_fsId, spec, credential));

        // start cluster
        exec.setProgress(0.3, "Starting cluster on Databricks");
        final DatabricksSparkContext sparkContext = (DatabricksSparkContext)SparkContextManager
            .<DatabricksSparkContextConfig> getOrCreateSparkContext(m_sparkContextId);
        sparkContext.startCluster(exec);

        // create spark context
        exec.setProgress(0.8, "Creating context");
        sparkContext.ensureOpened(true, exec.createSubProgress(0.1));
        final PortObject sparkPortObject = new SparkContextPortObject(m_sparkContextId);

        return new PortObject[]{sparkPortObject, fsPortObject};
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
        final DatabricksSparkContextConfig config) {

        final SparkContext<DatabricksSparkContextConfig> sparkContext =
            SparkContextManager.getOrCreateSparkContext(sparkContextId);

        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied) {
            // this should never ever happen
            throw new RuntimeException("Failed to apply Spark context settings."); // NOSONAR
        }
    }

    @Override
    protected void loadAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // this is only to avoid errors about an unconfigured spark context. the spark context configured here is
        // has and never will be opened because m_sparkContextId has a new and unique value.
        final String previousContextID = Files
            .readAllLines(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"), StandardCharsets.UTF_8).get(0);
        createDummyContext(previousContextID);

        // Do not restore the DB session here to avoid a cluster start
        setWarningMessage("State restored from disk. Reset this node to start a cluster and Spark execution context.");
    }

    private void createDummyContext(final String previousContextID) {
        final SparkVersion sparkVersion = getSettings().map(s -> s.getSparkVersion())
            .orElse(DatabricksSparkContextProvider.HIGHEST_SUPPORTED_SPARK_VERSION);
        final String url = "https://dummy-instance";
        final String clusterId = getSettings().map(s -> s.m_clusterId).orElse("dummy-cluster-id");
        final String token = "dummy-token";
        final String stagingArea = "";
        final boolean terminateClusterOnDestroy = true;
        final int connectionTimeout = 30;
        final int receiveTimeout = 60;
        final int jobCheckFrequency = 5;
        final SparkContextID contextId = new SparkContextID(previousContextID);
        final String fileSystemId = "dummy-file-system-id";

        final DatabricksSparkContextConfig config = new DatabricksSparkContextFileSystemConfig(sparkVersion, url,
            clusterId, token, stagingArea, terminateClusterOnDestroy, connectionTimeout, receiveTimeout,
            jobCheckFrequency, contextId, fileSystemId);

        configureSparkContext(contextId, config);
    }

    @Override
    protected void saveAdditionalInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {

        // see loadAdditionalInternals() for why we are doing this
        Files.write(Paths.get(nodeInternDir.getAbsolutePath(), "contextID"),
            m_sparkContextId.toString().getBytes(StandardCharsets.UTF_8));
    }

    @Override
    protected void onDisposeInternal() {
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));

        // close the file system also when the workflow is closed
        resetFileSystemConnection();
    }

    @Override
    protected void resetInternal() {
        BackgroundTasks.run(new DestroyAndDisposeSparkContextTask(m_sparkContextId));
        resetContextID();
        resetFileSystemConnection();
    }

    private void resetContextID() {
        // An ID that is unique to this node model instance, i.e. no two instances of this node model
        // have the same value here. Additionally, it's value changes during reset.
        final String uniqueContextId = UUID.randomUUID().toString();
        m_sparkContextId = DatabricksSparkContextProvider.createSparkContextID(uniqueContextId);
    }

    private void resetFileSystemConnection() {
        if (m_fsConnection != null) {
            m_fsConnection.closeInBackground();
            m_fsConnection = null;
        }
        m_fsId = null;
    }

}
