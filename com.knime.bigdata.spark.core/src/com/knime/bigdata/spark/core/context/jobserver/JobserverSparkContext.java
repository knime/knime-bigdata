/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Mar 2, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context.jobserver;

import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Set;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.context.JobController;
import com.knime.bigdata.spark.core.context.SparkContext;
import com.knime.bigdata.spark.core.context.SparkContextConstants;
import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUnavailableException;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.context.jobserver.request.CreateContextRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.DestroyContextRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.GetContextsRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.GetJarsRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.UploadFileRequest;
import com.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import com.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import com.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.jar.JobJar;
import com.knime.bigdata.spark.core.jar.SparkJarRegistry;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.SimpleJobRun;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;
import com.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import com.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import com.knime.bigdata.spark.core.util.PrepareContextJobInput;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * TODO: move away from KNIMESparkContext
 *
 * @author Bjoern Lohrmann, KNIME.COM
 */
public class JobserverSparkContext extends SparkContext {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(JobserverSparkContext.class);

    private final SparkContextID m_contextID;

    private SparkContextConfig m_config;

    private RestClient m_restClient;

    private SparkContextStatus m_status;

    private JobJar m_jobJar;

    private JobserverJobController m_jobController;

    private NamedObjectsController m_namedObjectsController;

    private static interface Task {
        public void run() throws Exception;
    }

    /**
     * Creates a new Spark context that pushes jobs to the Spark jobserver.
     *
     * @param contextID The identfier for this context.
     */
    public JobserverSparkContext(final SparkContextID contextID) {
        this.m_contextID = contextID;
        this.m_status = SparkContextStatus.NEW;

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized SparkContextStatus getStatus() {
        return this.m_status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean canReconfigure(final SparkContextConfig config) {
        // we can never change the id of an existing context
        if (!SparkContextID.fromConnectionDetails(config.getJobManagerUrl(), config.getContextName())
            .equals(m_contextID)) {
            return false;
        }

        switch (getStatus()) {
            case NEW:
            case CONFIGURED:
                return true;
            default:
                return m_config.getSparkVersion().equals(config.getSparkVersion())
                    && m_config.overrideSparkSettings() == config.overrideSparkSettings()
                    && (m_config.overrideSparkSettings()) ? Objects.equals(m_config.getCustomSparkSettings(), config.getCustomSparkSettings()) : true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void configure(final SparkContextConfig config) {

        switch (getStatus()) {
            case NEW:
            case CONFIGURED:
                applyConfig(config);
                this.m_status = SparkContextStatus.CONFIGURED;
                break;
            default:
                if (canReconfigure(config)) {
                    applyConfig(config);
                } else {
                    throw new RuntimeException(String.format(
                        "Trying to configure Spark context which is in status: %s. This is a bug.", getStatus()));
                }
        }
    }

    /**
     * @param config
     */
    private void applyConfig(final SparkContextConfig config) {
        m_config = config;
        m_jobJar = null;
        m_restClient = null;
        m_jobController = null;
        m_namedObjectsController = null;
    }

    private void ensureRestClient() throws KNIMESparkException {
        if (this.m_restClient == null) {
            try {
                this.m_restClient = new RestClient(m_config);
            } catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException e) {
                throw new KNIMESparkException(e);
            }
        }
    }

    private synchronized void resetToConfigured() {
        m_status = SparkContextStatus.CONFIGURED;
        m_restClient = null;
        m_jobController = null;
        m_namedObjectsController = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open() throws KNIMESparkException {
        if (getStatus() != SparkContextStatus.CONFIGURED) {
            throw new RuntimeException(
                String.format("Trying to open Spark context which is in status: %s. This is a bug.", getStatus()));
        }

        runWithResetOnFailure(new Task() {
            @Override
            public void run() throws Exception {
                ensureRestClient();
                ensureJobJar();

                if (!remoteSparkContextExists()) {
                    createRemoteSparkContext();
                } else if (m_config.overrideSparkSettings()){
                    LOGGER.warn("Remote Spark context already exists, cannot apply custom Spark context settings.");
                }

                // FIXME: make sure not to re-upload job jars if they are already on the server
                if (!isJobJarUploaded()) {
                    uploadJobJar();
                }

                m_status = SparkContextStatus.OPEN;

                validateAndPrepareContext();
            }

        });

    }

    private void ensureJobJar() throws KNIMESparkException {
        if (m_jobJar == null) {
            m_jobJar = SparkJarRegistry.getJobJar(m_config.getSparkVersion());
            if (m_jobJar == null) {
                throw new KNIMESparkException(String.format("No Spark jobs for your Spark version %s found.", m_config.getSparkVersion().getLabel()));
            }
        }
    }


    private void validateAndPrepareContext() throws KNIMESparkException {
        SparkVersion sparkVersion = m_config.getSparkVersion();

        PrepareContextJobInput prepInput = PrepareContextJobInput.create(m_jobJar.getDescriptor().getHash(),
            sparkVersion.getLabel(), m_jobJar.getDescriptor().getPluginVersion(),
            IntermediateToSparkConverterRegistry.getConverters(sparkVersion));

        SparkContextUtil.getSimpleRunFactory(m_contextID, SparkContextConstants.PREPARE_CONTEXT_JOB_ID)
            .createRun(prepInput).run(m_contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void destroy() throws KNIMESparkException {
        if (getStatus() != SparkContextStatus.OPEN) {
            throw new RuntimeException(
                String.format("Trying to destroy Spark context which is in status: %s. This is a bug.", getStatus()));
        }

        runWithResetOnFailure(new Task() {
            @Override
            public void run() throws Exception {
                ensureRestClient();
                LOGGER.info("Destroying context " + m_config.getContextName());
                new DestroyContextRequest(m_contextID, m_config, m_restClient).send();
                m_status = SparkContextStatus.CONFIGURED;
            }
        });
    }

    private void runWithResetOnFailure(final Task task) throws KNIMESparkException {
        try {
            task.run();
        } catch (Exception e) {
            resetToConfigured();

            if (e instanceof KNIMESparkException) {
                throw (KNIMESparkException)e;
            } else {
                throw new KNIMESparkException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextConfig getConfiguration() {
        return m_config;
    }

    private synchronized JobController getJobController() throws KNIMESparkException {
        ensureOpened();

        if (m_jobController == null) {
            ensureRestClient();
            ensureJobJar();
            m_jobController = new JobserverJobController(m_contextID, m_config, m_restClient,
                m_jobJar.getDescriptor().getJobserverJobClass());
        }

        return m_jobController;
    }

    private synchronized NamedObjectsController getNamedObjectController() throws KNIMESparkException {
        ensureOpened();

        if (m_namedObjectsController == null) {
            m_namedObjectsController = new JobBasedNamedObjectsController(m_contextID);
        }

        return m_namedObjectsController;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getID() {
        return m_contextID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkVersion getSparkVersion() {
        if (m_config == null) {
            throw new RuntimeException(String.format(
                "Trying to get Spark version of Spark context which is in status: %s. This is a bug.", getStatus()));
        }
        return m_config.getSparkVersion();
    }

    /**
     * @return <code>true</code> if the context exists
     * @throws KNIMESparkException
     */
    private boolean remoteSparkContextExists() throws KNIMESparkException {
        LOGGER.debug("Checking if remote context exists. Name: " + m_config.getContextName());
        final JsonArray contexts = new GetContextsRequest(m_contextID, m_config, m_restClient).send();

        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Available remote contexts: " + contexts);
        }

        boolean toReturn = false;

        for (int i = 0; i < contexts.size(); i++) {
            if (m_config.getContextName().equals(contexts.getString(i))) {
                LOGGER.debug("Remote context exists. Name: " + m_config.getContextName());
                toReturn = true;
                break;
            }
        }

        LOGGER.debug("Remote context does not exist. Name: " + m_config.getContextName());
        return toReturn;
    }

    /**
     * @param context the {@link SparkContextConfig} to use for checking job jar existence
     * @return <code>true</code> if the jar is uploaded, false otherwise
     * @throws KNIMESparkException
     */
    private boolean isJobJarUploaded() throws KNIMESparkException {

        LOGGER.debug("Checking if job jar is uploaded.");
        final JsonObject jars = new GetJarsRequest(m_contextID, m_config, m_restClient).send();

        if (jars.containsKey(JobserverConstants.APP_NAME)) {
            LOGGER.debug("Job jar is uploaded");
            return true;
        } else {
            LOGGER.debug("Job jar is not uploaded");
            return false;
        }
    }

    private void uploadJobJar() throws KNIMESparkException {
        LOGGER.debug(String.format("Uploading job jar: %s", m_jobJar.getJarFile().getAbsolutePath()));
        // uploads or overwrites any existing jar file uploaded from this workspace
        new UploadFileRequest(m_contextID, m_config, m_restClient, m_jobJar.getJarFile(),
            JobserverConstants.buildJarPath()).send();
    }

    private void createRemoteSparkContext() throws KNIMESparkException {
        LOGGER.debug("Creating new remote Spark context. Name: " + m_config.getContextName());
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Context settings: " + m_config);
        }

        new CreateContextRequest(m_contextID, m_config, m_restClient).send();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription() {
        if (getStatus() == SparkContextStatus.NEW) {
            return "<strong>Spark context is currently unconfigured.</strong>";
        }

        StringBuilder buf = new StringBuilder();
        buf.append("<strong>Connection settings</strong><hr/>");
        buf.append("<strong>Url:</strong>&nbsp;&nbsp;<tt>" + m_config.getJobManagerUrl() + "</tt><br/>");
        buf.append(
            "<strong>Use authentication:</strong>&nbsp;&nbsp;<tt>" + m_config.useAuthentication() + "</tt><br/>");
        if (m_config.useAuthentication()) {
            buf.append("<strong>User:</strong>&nbsp;&nbsp;<tt>" + m_config.getUser() + "</tt><br/>");
            buf.append("<strong>Password:</strong>&nbsp;&nbsp;<tt>" + (m_config.getPassword() != null) + "</tt><br/>");
        }
        buf.append("<strong>Job timeout:</strong>&nbsp;&nbsp;<tt>" + m_config.getJobTimeout() + " seconds</tt><br/>");
        buf.append("<strong>Job check frequency:</strong>&nbsp;&nbsp;<tt>" + m_config.getJobCheckFrequency()
            + " seconds</tt><br/>");

        buf.append("<br/>");
        buf.append("<strong>Context settings</strong><hr/>");
        buf.append("<strong>Spark version:</strong>&nbsp;&nbsp;<tt>" + m_config.getSparkVersion() + "</tt><br>");
        buf.append("<strong>Context name:</strong>&nbsp;&nbsp;<tt>" + m_config.getContextName() + "</tt><br>");
        buf.append("<strong>Delete objects on dispose:</strong>&nbsp;&nbsp;<tt>" + m_config.deleteObjectsOnDispose()
            + "</tt><br>");
        buf.append("<strong>Override spark settings:</strong>&nbsp;&nbsp;<tt>" + m_config.overrideSparkSettings()
            + "</tt><br>");
        if (m_config.overrideSparkSettings()) {
            buf.append(
                "<strong>Custom settings:</strong>&nbsp;&nbsp;<tt>" + m_config.getCustomSparkSettings() + "</tt><br>");
        }

        return buf.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("KNIMESparkContext [url=");
        builder.append(m_config.getJobManagerUrl());
        builder.append(", auth=");
        builder.append(m_config.useAuthentication());
        builder.append(", user=");
        builder.append(m_config.getUser());
        builder.append(", password set=");
        builder.append(m_config.getPassword() != null);
        builder.append(", jobCheckFrequency=");
        builder.append(m_config.getJobCheckFrequency());
        builder.append(", jobTimeout=");
        builder.append(m_config.getJobTimeout());
        builder.append(", sparkVersion=");
        builder.append(m_config.getSparkVersion());
        builder.append(", contextName=");
        builder.append(m_config.getContextName());
        builder.append(", deleteObjectsOnDispose=");
        builder.append(m_config.deleteObjectsOnDispose());
        builder.append(", sparkJobLogLevel=");
        builder.append(m_config.getSparkJobLogLevel());
        builder.append(", overrideSparkSettings=");
        builder.append(m_config.overrideSparkSettings());
        builder.append(", customSparkSettings=");
        builder.append(m_config.getCustomSparkSettings());
        builder.append("]");
        return builder.toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <O extends JobOutput> O startJobAndWaitForResult(final JobWithFilesRun<?, O> fileJob,
        final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {

        try {
            return getJobController().startJobAndWaitForResult(fileJob, exec);
        } catch (SparkContextUnavailableException e) {
            resetToConfigured();
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <O extends JobOutput> O startJobAndWaitForResult(final JobRun<?, O> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        try {
            return getJobController().startJobAndWaitForResult(job, exec);
        } catch (SparkContextUnavailableException e) {
            resetToConfigured();
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startJobAndWaitForResult(final SimpleJobRun<?> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {
        try {
            getJobController().startJobAndWaitForResult(job, exec);
        } catch (SparkContextUnavailableException e) {
            resetToConfigured();
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getNamedObjects() throws KNIMESparkException {
        try {
            return getNamedObjectController().getNamedObjects();
        } catch (SparkContextUnavailableException e) {
            resetToConfigured();
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteNamedObjects(final Set<String> namedObjects) throws KNIMESparkException {

        try {
            getNamedObjectController().deleteNamedObjects(namedObjects);
        } catch (SparkContextUnavailableException e) {
            resetToConfigured();
            throw e;
        }
    }
}
