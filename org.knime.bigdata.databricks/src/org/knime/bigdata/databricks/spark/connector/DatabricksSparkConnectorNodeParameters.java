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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.databricks.credential.DatabricksAccessTokenCredential;
import org.knime.bigdata.databricks.node.ClusterChoiceProvider;
import org.knime.bigdata.databricks.rest.DatabricksRESTClient;
import org.knime.bigdata.databricks.rest.clusters.ClusterAPI;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnection;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFSConnectionConfig;
import org.knime.bigdata.databricks.unity.filehandling.fs.UnityFileSystem;
import org.knime.bigdata.databricks.workspace.port.DatabricksWorkspacePortObjectSpec;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.databricks.DatabricksSparkContextProvider;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextConfig;
import org.knime.bigdata.spark.core.databricks.context.DatabricksSparkContextFileSystemConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FSConnectionProvider;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.FileSelectionWidget;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.SingleFileSelectionMode;
import org.knime.core.webui.node.dialog.defaultdialog.internal.file.WithCustomFileSystem;
import org.knime.core.webui.node.dialog.defaultdialog.util.updates.StateComputationFailureException;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.CustomValidation;
import org.knime.core.webui.node.dialog.defaultdialog.widget.validation.custom.SimpleValidation;
import org.knime.credentials.base.NoSuchCredentialException;
import org.knime.node.parameters.Advanced;
import org.knime.node.parameters.NodeParameters;
import org.knime.node.parameters.NodeParametersInput;
import org.knime.node.parameters.Widget;
import org.knime.node.parameters.layout.After;
import org.knime.node.parameters.layout.Layout;
import org.knime.node.parameters.layout.Section;
import org.knime.node.parameters.updates.ParameterReference;
import org.knime.node.parameters.updates.StateProvider;
import org.knime.node.parameters.updates.ValueProvider;
import org.knime.node.parameters.updates.ValueReference;
import org.knime.node.parameters.widget.choices.ChoicesProvider;
import org.knime.node.parameters.widget.choices.StringChoice;
import org.knime.node.parameters.widget.choices.StringChoicesProvider;
import org.knime.node.parameters.widget.number.NumberInputWidget;
import org.knime.node.parameters.widget.number.NumberInputWidgetValidation.MinValidation.IsNonNegativeValidation;
import org.knime.node.parameters.widget.text.TextInputWidget;
import org.knime.node.parameters.widget.text.TextInputWidgetValidation.PatternValidation.IsNotBlankValidation;

/**
 * Node parameters for the Databricks Spark Connector node.
 *
 * @author Sascha Wolke, KNIME GmbH, Berlin, Germany
 */
@SuppressWarnings("restriction")
class DatabricksSparkConnectorNodeParameters implements NodeParameters {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DatabricksSparkConnectorNodeParameters.class);

    // ===================== Section definitions =====================

    @Section(title = "Cluster")
    interface ClusterSection {
    }

    @Section(title = "Timeouts")
    @After(ClusterSection.class)
    @Advanced
    interface TimeoutsSection {
    }

    // ===================== References =====================

    interface ClusterIdRef extends ParameterReference<String> {
    }

    interface SparkVersionRef extends ParameterReference<String> {
    }

    interface StagingDirectoryRef extends ParameterReference<String> {
    }

    // ===================== Fields =====================

    @Layout(ClusterSection.class)
    @Widget(title = "Cluster ID", description = "Unique identifier of a cluster in the databricks workspace.")
    @ChoicesProvider(ClusterChoiceProvider.class)
    @TextInputWidget(patternValidation = IsNotBlankValidation.class)
    @ValueReference(ClusterIdRef.class)
    String m_clusterId = "";

    @Layout(ClusterSection.class)
    @Widget(title = "Spark Version", description = "Version of Spark running on the cluster.")
    @ChoicesProvider(SparkVersionChoicesProvider.class)
    @ValueProvider(SparkVersionValueProvider.class)
    @TextInputWidget(patternValidation = IsNotBlankValidation.class)
    @ValueReference(SparkVersionRef.class)
    String m_sparkVersion = DatabricksSparkContextProvider.HIGHEST_SUPPORTED_SPARK_VERSION.toString();

    @Layout(ClusterSection.class)
    @Widget(title = "Staging area for Spark jobs",
        description = "Specify a directory in the Unity File System,"
            + " that will be used to transfer temporary files between KNIME and the Spark context.")
    @FileSelectionWidget(SingleFileSelectionMode.FOLDER)
    @WithCustomFileSystem(connectionProvider = FileSystemConnectionProvider.class)
    @ValueReference(StagingDirectoryRef.class)
    @CustomValidation(StagingDirectoryValidator.class)
    String m_stagingArea = "";

    @Layout(ClusterSection.class)
    @Widget(title = "Terminate cluster on context destroy",
        description = "If selected, the cluster will be destroyed when the node will be reset,"
            + " the <i>Destroy Spark Context</i> node executed on the context, the workflow or"
            + " KNIME is closed. This way, resources are released, but all data cached inside the cluster"
            + " are lost, unless they have been saved to persistent storage such as DBFS.")
    boolean m_terminateClusterOnDestroy;

    @Layout(TimeoutsSection.class)
    @Widget(title = "Job status polling interval (seconds)",
        description = "The frequency with which KNIME polls the status of a job in seconds.")
    @NumberInputWidget(minValidation = IsNonNegativeValidation.class)
    int m_jobCheckFrequency = 1;

    // ===================== File System Provider =====================

    /**
     * Provides a {@link FSConnectionProvider} based on the connection settings and staging directory.
     */
    static final class FileSystemConnectionProvider implements StateProvider<FSConnectionProvider> {

        private Supplier<String> m_stagingDirSupplier;

        private static final String ERROR_MSG =
            "Connection not available. Please re-execute the preceding connector node and make sure it is connected.";

        @Override
        public void init(final StateProvider.StateProviderInitializer initializer) {
            m_stagingDirSupplier = initializer.computeFromValueSupplier(StagingDirectoryRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public FSConnectionProvider computeState(final NodeParametersInput parametersInput) {
            final PortObjectSpec[] inputSpecs = parametersInput.getInPortSpecs();

            return () -> createConnection(inputSpecs);
        }

        private UnityFSConnection createConnection(final PortObjectSpec[] inputSpecs)
            throws InvalidSettingsException, IOException {

            final DatabricksWorkspacePortObjectSpec spec = getWorkspaceSpec(inputSpecs);
            final DatabricksAccessTokenCredential credential;
            try {
                credential = spec.resolveCredential(DatabricksAccessTokenCredential.class);
            } catch (final NoSuchCredentialException e) {
                throw new InvalidSettingsException(ERROR_MSG, e);
            }

            final String currentDir = normalizeWorkingDirectory(m_stagingDirSupplier.get());
            final UnityFSConnectionConfig config =
                createFSConnectionConfig(credential, spec, currentDir);

            final UnityFSConnection connection = new UnityFSConnection(config); // NOSONAR Java version has no var
            try {
                testConnection(connection);
            } catch (final IOException e) {
                connection.closeInBackground();
                throw e;
            }
            return connection;
        }

        private static DatabricksWorkspacePortObjectSpec getWorkspaceSpec(final PortObjectSpec[] inputSpecs)
            throws InvalidSettingsException {

            final boolean hasWorkspaceConnection = inputSpecs != null && inputSpecs.length > 0
                && inputSpecs[0] instanceof DatabricksWorkspacePortObjectSpec;
            CheckUtils.checkSetting(hasWorkspaceConnection, ERROR_MSG);

            final DatabricksWorkspacePortObjectSpec spec = (DatabricksWorkspacePortObjectSpec)inputSpecs[0];
            CheckUtils.checkSetting(spec.isPresent(), ERROR_MSG);
            return spec;
        }

        private static String normalizeWorkingDirectory(final String workingDirectory) {
            if (StringUtils.isBlank(workingDirectory) //
                || !workingDirectory.startsWith(UnityFileSystem.PATH_SEPARATOR)) {
                return UnityFileSystem.PATH_SEPARATOR;
            }
            return workingDirectory;
        }

        @SuppressWarnings("resource")
        private static void testConnection(final UnityFSConnection connection) throws IOException {
            ((UnityFileSystem)connection.getFileSystem()).testConnection();
        }

    }

    // ===================== Choices Provider =====================

    private static final class SparkVersionChoicesProvider implements StringChoicesProvider {

        @Override
        public void init(final StateProviderInitializer initializer) {
            initializer.computeAfterOpenDialog();
        }

        @Override
        public List<StringChoice> computeState(final NodeParametersInput context) {
            return new DatabricksSparkContextProvider().getSupportedSparkVersions().stream() //
                .sorted(Collections.reverseOrder()) //
                .map(v -> new StringChoice(v.toString(), v.getLabel())) //
                .toList();
        }

    }

    // ===================== Value Provider =====================

    private static final class SparkVersionValueProvider implements StateProvider<String> {

        private Supplier<String> m_clusterIdSupplier;

        private Supplier<String> m_sparkVersionSupplier;

        @Override
        public void init(final StateProviderInitializer initializer) {
            m_clusterIdSupplier = initializer.computeFromValueSupplier(ClusterIdRef.class);
            m_sparkVersionSupplier = initializer.getValueSupplier(SparkVersionRef.class);
            initializer.computeAfterOpenDialog();
        }

        @Override
        public String computeState(final NodeParametersInput context) throws StateComputationFailureException {
            final String clusterId = m_clusterIdSupplier.get();

            try {
                final PortObjectSpec[] inputSpecs = context.getInPortSpecs();

                // try to detect spark version from cluster info if input port connected
                if (inputSpecs.length > 0 && inputSpecs[0] instanceof DatabricksWorkspacePortObjectSpec) {
                    final ClusterAPI client =
                        DatabricksRESTClient.fromSingleWorkspaceInputPort(ClusterAPI.class, inputSpecs);
                    final Optional<String> detectedVersion =
                        ClusterSparkVersionDetector.getSparkVersion(client, clusterId);
                    if (detectedVersion.isPresent()) {
                        return detectedVersion.get();
                    }
                }

            } catch (final Exception e) { // NOSONAR catch all exceptions here
                LOGGER.info("Unable to fetch Spark version for cluster.", e);
            }

            // fallback to current selection
            return m_sparkVersionSupplier.get();
        }

    }

    // ===================== Validation =====================

    void validateOnConfigure() throws InvalidSettingsException {
        // cluster-id
        CheckUtils.checkSetting(StringUtils.isNotBlank(m_clusterId), "Please specify a cluster ID.");

        // spark version
        CheckUtils.checkSetting(StringUtils.isNotBlank(m_sparkVersion), "Please specify a Spark version.");
        CheckUtils.checkSetting(getSparkVersion() != null, "Please specify a valid Spark version.");

        // staging directory
        validateStagingDirectory(m_stagingArea);

        // job check frequency
        CheckUtils.checkSetting(m_jobCheckFrequency > 0, "Job status polling interval must be greater than zero.");
    }

    private static void validateStagingDirectory(final String stagingDirectory) throws InvalidSettingsException {
        if (StringUtils.isBlank(stagingDirectory)) {
            throw new InvalidSettingsException("Please specify a staging directory.");
        }
        if (!stagingDirectory.startsWith(UnityFileSystem.PATH_SEPARATOR)) {
            throw new InvalidSettingsException("Staging directory must be set to an absolute path.");
        }
    }

    // ===================== Custom Validations =====================

    static class StagingDirectoryValidator extends SimpleValidation<String> {
        @Override
        public void validate(final String stagingDirectory) throws InvalidSettingsException {
            validateStagingDirectory(stagingDirectory);
        }
    }

    // ===================== Config Generators =====================

    SparkVersion getSparkVersion() {
        return SparkVersion.fromString(m_sparkVersion);
    }

    /**
     * Create context config using the given workspace connection.
     *
     * @param contextId The ID of the Spark context for which to create the config object.
     * @param fileSystemId Identifies the staging area file system connection.
     * @param spec port spec with timeout settings
     * @param credential credential to authenticate with
     * @return a new {@link DatabricksSparkContextConfig} derived from the current settings.
     */
    public DatabricksSparkContextConfig createContextConfig(final SparkContextID contextId, final String fileSystemId,
        final DatabricksWorkspacePortObjectSpec spec, final DatabricksAccessTokenCredential credential) {

        final int connectionTimeout = Math.toIntExact(spec.getConnectionTimeout().toSeconds());
        final int readTimeout = Math.toIntExact(spec.getReadTimeout().toSeconds());

        return new DatabricksSparkContextFileSystemConfig(getSparkVersion(), m_clusterId, credential, m_stagingArea,
            m_terminateClusterOnDestroy, connectionTimeout, readTimeout, m_jobCheckFrequency, contextId, fileSystemId);
    }

    UnityFSConnectionConfig createStagingFSConnectionConfig(final DatabricksAccessTokenCredential credential,
        final DatabricksWorkspacePortObjectSpec spec) {
        return createFSConnectionConfig(credential, spec, m_stagingArea);
    }

    private static UnityFSConnectionConfig createFSConnectionConfig(final DatabricksAccessTokenCredential credential,
        final DatabricksWorkspacePortObjectSpec spec, final String workingDirectory) {

        return UnityFSConnectionConfig.builder() //
            .withCredential(credential) //
            .withWorkingDirectory(workingDirectory) //
            .withConnectionTimeout(spec.getConnectionTimeout()) //
            .withReadTimeout(spec.getReadTimeout()) //
            .build();
    }

}
