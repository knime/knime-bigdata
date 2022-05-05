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
 */
package org.knime.bigdata.spark.core.databricks.context;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksPrepareContextJobInput;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksPrepareContextJobOutput;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksStagingAreaTester;
import org.knime.bigdata.spark.core.exception.InvalidJobJarException;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.TextTemplateUtil;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

/**
 * Spark context implementation for Databricks.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DatabricksSparkContext extends SparkContext<DatabricksSparkContextConfig> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(DatabricksSparkContext.class);

    private static final String CLUSTER_NAME_CONF_KEY = "spark.databricks.clusterUsageTags.clusterName";

    /**
     * Filename of the job jar in the remote staging area.
     */
    public static final String JOB_JAR_FILENAME = "knime-spark-jobs-jar";

    private DatabricksJobController m_jobController;

    private JobBasedNamedObjectsController m_namedObjectsController;

    private DatabricksClient m_databricksClient;

    private RemoteFSController m_remoteFSController;

    private ContextAttributes m_contextAttributes;

    private class ContextAttributes {
        String sparkWebUI;

        Map<String, String> sparkConf;

        boolean adaptiveExecutionEnabled;
    }

    /**
     * Creates a new Spark context that pushes jobs to Databricks.
     *
     * @param contextID The identfier for this context.
     */
    public DatabricksSparkContext(final SparkContextID contextID) {
        super(contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setStatus(final SparkContextStatus newStatus) throws KNIMESparkException {
        super.setStatus(newStatus);
        switch (newStatus) {
            case NEW:
            case CONFIGURED:
                m_databricksClient = null;
                if (m_remoteFSController != null) {
                    m_remoteFSController.ensureClosed();
                    m_remoteFSController = null;
                }
                m_contextAttributes = null;
                m_namedObjectsController = null;
                m_jobController = null;
                break;
            default: // OPEN
                ensureRemoteFSConnection();
                ensureNamedObjectsController();
                ensureJobController();
                break;
        }
    }

    private void ensureRemoteFSConnection() throws KNIMESparkException {
        final RemoteFSController tmpController = getConfiguration().createRemoteFSController();
        tmpController.createStagingArea();
        m_remoteFSController = tmpController;
    }

    private void ensureNamedObjectsController() {
        if (m_namedObjectsController == null) {
            m_namedObjectsController = new JobBasedNamedObjectsController(getID());
        }
    }

    private void ensureJobController() throws KNIMESparkException {
        if (m_jobController == null) {
            final Class<?> jobBindingClass = getJobJar().getDescriptor()
                .getWrapperJobClasses().get(SparkContextIDScheme.SPARK_DATABRICKS);
            if (jobBindingClass == null) {
                throw new KNIMESparkException("Missing Spark job binding class for Databricks.");
            }

            m_jobController = new DatabricksJobController(m_databricksClient, m_remoteFSController, jobBindingClass,
                m_namedObjectsController);
        }
    }

    private void ensureDatabricksClient() throws KNIMESparkException {
        if (m_databricksClient == null) {
            try {
                final DatabricksSparkContextConfig config = getConfiguration();
                LOGGER.debug("Creating new Databricks client for context: " + config.getContextName());
                m_databricksClient = new DatabricksClient(config);
            } catch (final Exception e) {
                throw new KNIMESparkException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JobController getJobController() {
        return m_jobController;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NamedObjectsController getNamedObjectsController() {
        return m_namedObjectsController;
    }

    /**
     * Start a cluster on databricks if required and wait until cluster is in running state.
     *
     * @param exec
     * @throws KNIMESparkException
     * @throws CanceledExecutionException
     */
    public void startCluster(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        ensureDatabricksClient();
        m_databricksClient.startOrConnectCluster(exec);
    }

    @Override
    protected boolean open(final boolean createRemoteContext, final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        boolean contextWasCreated = false;
        try {
            if (!createRemoteContext) {
                throw new SparkContextNotFoundException(getID());
            }

            ensureDatabricksClient();
            setStatus(SparkContextStatus.OPEN);

            exec.setProgress(0.1, "Uploading Spark jobs jar");
            uploadJobJar(exec);

            exec.setProgress(0.5, "Opening remote Spark context on Databricks");
            m_jobController.createContext(exec);
            contextWasCreated = true;

            exec.setProgress(0.9, "Validating and preparing context on Databricks");
            validateAndPrepareContext(exec);
            exec.setProgress(1);
        } catch (final Exception e) {
            // make sure we don't leave a broken context behind
            if (contextWasCreated) {
                try {
                    destroy();
                } catch (final KNIMESparkException toIgnore) {
                    // ignore
                }
            }

            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        }

        return contextWasCreated;
    }

    private void validateAndPrepareContext(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        exec.setProgress(0.7, "Testing file upload on file system connection");
        final String stagingTestfileName = uploadStagingTestfile();

        exec.setProgress(0.8, "Running job to prepare Spark context");
        final DatabricksPrepareContextJobInput prepInput = new DatabricksPrepareContextJobInput(getJobJar().getDescriptor().getHash(),
            getSparkVersion().toString(), getJobJar().getDescriptor().getPluginVersion(),
            IntermediateToSparkConverterRegistry.getConverters(getSparkVersion()),
            stagingTestfileName);

        try {
            final DatabricksPrepareContextJobOutput output =
                SparkContextUtil.<DatabricksPrepareContextJobInput, DatabricksPrepareContextJobOutput> getJobRunFactory(getID(),
                    DatabricksPrepareContextJobInput.DATABRICKS_PREPARE_CONTEXT_JOB_ID).createRun(prepInput).run(getID());
            m_contextAttributes = new ContextAttributes();
            m_contextAttributes.sparkWebUI = output.getSparkWebUI();
            m_contextAttributes.sparkConf = output.getSparkConf();
            m_contextAttributes.adaptiveExecutionEnabled = output.adaptiveExecutionEnabled();

            exec.setProgress(0.9, "Testing file download on file system connection");
            downloadStagingTestfile(output.getTestfileName());

        } catch (final InvalidJobJarException e) {
            m_databricksClient.removeJobJarFromCluster(exec);
            throw new KNIMESparkException(
                "Invalid job jar on cluster detected. Please restart or terminate the cluster and run this node again.",
                e);
        }
    }

    private void downloadStagingTestfile(final String testfileName) throws KNIMESparkException {
        try {
            DatabricksStagingAreaTester.validateTestfileContent(m_remoteFSController, testfileName);
        } catch (final Exception e) {
            throw new KNIMESparkException("Remote file system download test failed: " + e.getMessage(), e);
        } finally {
            m_remoteFSController.deleteSafely(testfileName);
        }

    }

    private String uploadStagingTestfile() throws KNIMESparkException {
        try {
            return DatabricksStagingAreaTester.writeTestfileContent(m_remoteFSController);
        } catch (final Exception e) {
            throw new KNIMESparkException("Remote file system upload test failed: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy() throws KNIMESparkException {
        LOGGER.info("Destroying Databricks Spark context.");
        try {
            if (getStatus() == SparkContextStatus.OPEN) {
                if (getConfiguration().terminateClusterOnDestroy()) {
                    LOGGER.info("Terminating cluster " + getConfiguration().getClusterId() + " on Databricks.");
                    m_jobController.reset();
                    m_databricksClient.terminateCluster();
                } else {
                    LOGGER.info("Destroying context on cluster " + getConfiguration().getClusterId() + " on Databricks.");
                    m_jobController.destroyContext();
                }
            }
        } finally {
            setStatus(SparkContextStatus.CONFIGURED);
        }
    }

    private void uploadJobJar(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        final File jobJarFile = getJobJar().getJarFile();
        LOGGER.debug(String.format("Uploading job jar: %s", jobJarFile.getAbsolutePath()));

        try {
            final String remoteJobJar = m_remoteFSController.uploadAdditionalFile(jobJarFile, JOB_JAR_FILENAME);
            if (m_remoteFSController.getStagingAreaReturnsPath()) {
                m_databricksClient.installJobJarOnCluster(exec, "dbfs://" + remoteJobJar);
            } else {
                m_databricksClient.installJobJarOnCluster(exec, remoteJobJar);
            }

        } catch (final IOException e) {
            throw new KNIMESparkException("Unable to upload job jar", e);
        }
    }

    /**
     * Databricks client generally catches all exceptions and wraps them as RuntimeException. This method extracts cause and
     * message and wraps them properly inside a {@link KNIMESparkException}.
     *
     * @param e An exception thrown by the programmatic Databricks API
     * @throws Exception Rewrapped {@link KNIMESparkException} or the original exception.
     */
    static void handleDatabricksException(final Exception e) throws KNIMESparkException {
        // databricks client catches all exceptions and wraps them as RuntimeException
        // here we extract
        if (e instanceof RuntimeException && e.getCause() != null) {
            final Throwable cause = e.getCause();
            if (cause.getMessage() != null && !cause.getMessage().isEmpty()) {
                throw new KNIMESparkException(cause.getMessage(), cause);
            } else {
                throw new KNIMESparkException(cause);
            }
        } else {
            throw new KNIMESparkException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription() {
        if (getStatus() == SparkContextStatus.NEW) {
            return "<strong>Spark context is currently unconfigured.</strong>";
        }

        final DatabricksSparkContextConfig config = getConfiguration();
        final Map<String, String> reps = new HashMap<>();

        reps.put("spark_version", config.getSparkVersion().toString());
        reps.put("url", config.getDatabricksUrl());
        reps.put("cluster_id", config.getClusterId());
        reps.put("cluster_name", getClusterName());
        reps.put("authentication", createAuthenticationInfoString());
        reps.put("job_check_frequency", Integer.toString(config.getJobCheckFrequencySeconds()));
        reps.put("terminate_on_dispose", config.terminateClusterOnDestroy() ? "yes" : "no");
        reps.put("context_state", getStatus().toString());
        reps.put("spark_web_ui", m_contextAttributes != null ? m_contextAttributes.sparkWebUI : "unavailable");
        reps.put("spark_properties", mkSparkPropertiesHTMLRows());
        reps.put("adaptiveExecutionEnabled",
            m_contextAttributes != null ? Boolean.toString(m_contextAttributes.adaptiveExecutionEnabled) : null);

        try (InputStream r = getClass().getResourceAsStream("context_html_description.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }

    private String getClusterName() {
        if (m_contextAttributes != null
                && !StringUtils.isBlank(m_contextAttributes.sparkConf.get(CLUSTER_NAME_CONF_KEY))) {
            return m_contextAttributes.sparkConf.get(CLUSTER_NAME_CONF_KEY);
        } else {
            return "unavailable";
        }
    }

    private String mkSparkPropertiesHTMLRows() {
        if (m_contextAttributes == null) {
            return "<tr><td>unavailable</td><td></td></tr>";
        }

        final ArrayList<String> sortedProperties = new ArrayList<>(m_contextAttributes.sparkConf.keySet());
        Collections.sort(sortedProperties);
        final StringBuilder buf = new StringBuilder();
        for (final String property : sortedProperties) {
            String value = m_contextAttributes.sparkConf.get(property);
            if (value.length() > 1000) {
                value = value.substring(0, 1000) + "... (value truncated)";
            }
            buf.append(String.format("<tr><td>%s</td><td>%s</td></tr>%n", property, value));
        }
        return buf.toString();
    }

    private String createAuthenticationInfoString() {
        final DatabricksSparkContextConfig config = getConfiguration();
        if (config.useToken()) {
            return "Token";
        } else {
            return String.format("User and password (user: %s)", config.getUser());
        }
    }

    /**
     * @return cluster status provider of cluster running the spark context
     */
    public DatabricksClusterStatusProvider getClusterStatusHandler() {
        return new DatabricksClusterStatusProvider(m_databricksClient);
    }

    @Override
    public synchronized KNIMEToIntermediateConverterParameter getConverterPrameter() {
        return KNIMEToIntermediateConverterParameter.DEFAULT;
    }

    @Override
    public boolean adaptiveExecutionEnabled() {
        return m_contextAttributes != null && m_contextAttributes.adaptiveExecutionEnabled;
    }
}
