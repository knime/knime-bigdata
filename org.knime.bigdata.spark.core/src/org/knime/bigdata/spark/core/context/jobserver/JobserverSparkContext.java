/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
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
package org.knime.bigdata.spark.core.context.jobserver;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;
import java.util.Set;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextConstants;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.jobserver.request.CreateContextRequest;
import org.knime.bigdata.spark.core.context.jobserver.request.DestroyContextRequest;
import org.knime.bigdata.spark.core.context.jobserver.request.GetContextsRequest;
import org.knime.bigdata.spark.core.context.jobserver.request.GetJarsRequest;
import org.knime.bigdata.spark.core.context.jobserver.request.UploadFileRequest;
import org.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.jar.JobJar;
import org.knime.bigdata.spark.core.jar.SparkJarRegistry;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;

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

    private String m_jobserverAppName;

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
        m_contextID = contextID;
        m_status = SparkContextStatus.NEW;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized SparkContextStatus getStatus() {
        return m_status;
    }

    private boolean canReconfigureWithoutDestroy(final SparkContextConfig newConfig) {
        if (m_config == null) {
            return true;
        }
        return m_config.getSparkVersion().equals(newConfig.getSparkVersion())
            && (m_config.overrideSparkSettings() == newConfig.overrideSparkSettings())
            && ((m_config.overrideSparkSettings())
                ? Objects.equals(m_config.getCustomSparkSettings(), newConfig.getCustomSparkSettings()) : true);
    }

    /**
     * Updates the status to the given status
     *
     * @param newStatus the new status of the context
     */
    protected void setStatus(final SparkContextStatus newStatus) {
        LOGGER.info(String.format("Spark context %s changed status from %s to %s", getID().toString(),
            m_status.toString(), newStatus.toString()));
        m_status = newStatus;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean ensureConfigured(final SparkContextConfig newConfig,
        final boolean overwriteExistingConfig, final boolean destroyIfNecessary) throws KNIMESparkException {

        // we can never change the id of an existing context
        if (!SparkContextID.fromConnectionDetails(newConfig.getJobServerUrl(), newConfig.getContextName())
            .equals(m_contextID)) {
            return false;
        }

        boolean toReturn = false;
        boolean doApply = false;

        switch (getStatus()) {
            case NEW:
                doApply = toReturn = true;
                break;
            case CONFIGURED:
                doApply = toReturn = newConfig.equals(m_config) || overwriteExistingConfig;
                break;
            default: // OPEN
                final boolean canReconfigureWithoutDestroy = canReconfigureWithoutDestroy(newConfig);

                // make sure we only apply the config (and switch to CONFIGURED) if the new config is different from
                // the existing one
                doApply = !newConfig.equals(m_config) && overwriteExistingConfig
                    && (canReconfigureWithoutDestroy || destroyIfNecessary);
                toReturn = doApply || newConfig.equals(m_config);

                if (doApply && !canReconfigureWithoutDestroy) {
                    ensureDestroyed();
                }
                break;
        }

        if (doApply) {
            applyConfig(newConfig);
            setStatus(SparkContextStatus.CONFIGURED);
        }

        return toReturn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized boolean ensureConfigured(final SparkContextConfig newConfig,
        final boolean overwriteExistingConfig) {
        try {
            return ensureConfigured(newConfig, overwriteExistingConfig, false);
        } catch (KNIMESparkException e) {
            // should never happen because we are not actually destroying a pre-existing remote context
            return false;
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
        if (m_restClient == null) {
            try {
                m_restClient = new RestClient(m_config);
            } catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException
                    | UnsupportedEncodingException e) {
                throw new KNIMESparkException(e);
            }
        }
    }

    private synchronized void resetToConfigured() {
        setStatus(SparkContextStatus.CONFIGURED);
        m_restClient = null;
        m_jobController = null;
        m_namedObjectsController = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void open(final boolean createRemoteContext) throws KNIMESparkException {
        if (getStatus() != SparkContextStatus.CONFIGURED) {
            throw new RuntimeException(
                String.format("Trying to open Spark context which is in status: %s. This is a bug.", getStatus()));
        }

        runWithResetOnFailure(new Task() {
            @Override
            public void run() throws Exception {

                boolean contextWasCreated = false;

                try {
                    ensureRestClient();
                    ensureJobJar();

                    if (!remoteSparkContextExists()) {
                        if (createRemoteContext) {
                            contextWasCreated = createRemoteSparkContext();
                        } else {
                            throw new SparkContextNotFoundException(m_contextID);
                        }
                    }

                    // regardless of of contextWasCreated is true or not we can assume that the context exists now
                    // (somebody else may have created it before us)
                    if (!isJobJarUploaded()) {
                        uploadJobJar();
                    }

                    setStatus(SparkContextStatus.OPEN);
                    validateAndPrepareContext();
                } catch (KNIMESparkException e) {
                    if (contextWasCreated) {
                        try {
                            doDestroy();
                        } catch (KNIMESparkException toIgnore) {
                            // ignore
                        }
                    }
                    throw e;
                }
            }
        });
    }

    private void ensureJobJar() throws KNIMESparkException {
        if (m_jobJar == null || !m_jobJar.getJarFile().exists()) {
            m_jobJar = SparkJarRegistry.getJobJar(m_config.getSparkVersion());
            if (m_jobJar == null) {
                throw new KNIMESparkException(
                    String.format("No Spark jobs for Spark version %s found.", m_config.getSparkVersion().getLabel()));
            }

            m_jobserverAppName = createJobserverAppname(m_config.getSparkVersion(), m_jobJar.getDescriptor().getHash());
        }
    }

    /**
     * @return the "app name" for the jobserver, which is an identifier for the uploaded job jar, that has to be
     *         specified with each job.
     */
    private String createJobserverAppname(final SparkVersion sparkVersion, final String jobJarHash) {
        final String knimeInstanceID = KNIMEConstants.getKNIMEInstanceID();
        return String.format("knimeJobs_%s_%s_spark-%s", knimeInstanceID.substring(knimeInstanceID.indexOf('-') + 1),
            jobJarHash, m_config.getSparkVersion().toString());
    }

    private void validateAndPrepareContext() throws KNIMESparkException {
        SparkVersion sparkVersion = m_config.getSparkVersion();

        PrepareContextJobInput prepInput = PrepareContextJobInput.create(m_jobJar.getDescriptor().getHash(),
            sparkVersion.toString(), m_jobJar.getDescriptor().getPluginVersion(),
            IntermediateToSparkConverterRegistry.getConverters(sparkVersion));

        SparkContextUtil.getSimpleRunFactory(m_contextID, SparkContextConstants.PREPARE_CONTEXT_JOB_ID)
            .createRun(prepInput).run(m_contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected synchronized void destroy() throws KNIMESparkException {
        if (getStatus() == SparkContextStatus.NEW) {
            throw new RuntimeException(
                String.format("Cannot destroy unconfigured Spark context. This is a bug.", getStatus()));
        }

        doDestroy();
    }

    private void doDestroy() throws KNIMESparkException {

        runWithResetOnFailure(new Task() {
            @Override
            public void run() throws Exception {
                ensureRestClient();
                LOGGER.info("Destroying context " + m_config.getContextName());
                new DestroyContextRequest(m_contextID, m_config, m_restClient).send();

                // wait up to 10s until context is removed from running list
                for (int i = 0; i < 50 && remoteSparkContextExists(); i++) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                    }
                }
                setStatus(SparkContextStatus.CONFIGURED);
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
        ensureOpened(false);

        if (m_jobController == null) {
            ensureRestClient();
            ensureJobJar();
            m_jobController = new JobserverJobController(m_contextID, m_config, m_jobserverAppName, m_restClient,
                m_jobJar.getDescriptor().getJobserverJobClass());
        }

        return m_jobController;
    }

    private synchronized NamedObjectsController getNamedObjectController() throws KNIMESparkException {
        ensureOpened(false);

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

        if (!toReturn) {
            LOGGER.debug("Remote context does not exist. Name: " + m_config.getContextName());
        }

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

        if (jars.containsKey(m_jobserverAppName)) {
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
            JobserverConstants.buildJarPath(m_jobserverAppName)).send();
    }

    private boolean createRemoteSparkContext() throws KNIMESparkException {
        LOGGER.debug("Creating new remote Spark context. Name: " + m_config.getContextName());
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Context settings: " + m_config);
        }

        return new CreateContextRequest(m_contextID, m_config, m_restClient).send();
    }

    private void replace(final StringBuilder buf, final String pattern, final String value) {
        final String realPattern = String.format("${%s}", pattern);
        int start = buf.indexOf(realPattern);

        if (start == -1) {
            throw new IllegalArgumentException(String.format("Pattern %s does not appear in template", realPattern));
        }

        buf.replace(start, start + realPattern.length(), value);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription() {
        if (getStatus() == SparkContextStatus.NEW) {
            return "<strong>Spark context is currently unconfigured.</strong>";
        }

        final String template;
        try (InputStream r = getClass().getResourceAsStream("context_html_description.template")) {
            final byte[] bytes = new byte[r.available()];
            r.read(bytes);
            template = new String(bytes, Charset.forName("UTF8"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }


        StringBuilder buf = new StringBuilder(template);
        replace(buf, "url", m_config.getJobServerUrl());
        replace(buf, "use_authentication", m_config.useAuthentication()
            ? String.format("true (user: %s)", m_config.getUser())
            : "false");
        replace(buf, "receive_timeout", (m_config.getReceiveTimeout().getSeconds() == 0)
            ? "infinite"
            : Long.toString(m_config.getReceiveTimeout().getSeconds()));
        replace(buf, "job_check_frequency", Integer.toString(m_config.getJobCheckFrequency()));
        replace(buf, "spark_version", m_config.getSparkVersion().toString());
        replace(buf, "context_name", m_config.getContextName());
        replace(buf, "delete_data_on_dispose", Boolean.toString(m_config.deleteObjectsOnDispose()));
        replace(buf, "override_settings", Boolean.toString(m_config.overrideSparkSettings()));
        replace(buf, "custom_settings",m_config.overrideSparkSettings()
            ? m_config.getCustomSparkSettings()
            : "(not applicable)");
        replace(buf, "context_state", getStatus().toString());
        return buf.toString();
//
//
//        buf.append("<strong>Connection settings</strong><hr/>");
//        buf.append("<strong>Url:</strong>&nbsp;&nbsp;<tt>" + m_config.getJobServerUrl() + "</tt><br/>");
//        buf.append(
//            "<strong>Use authentication:</strong>&nbsp;&nbsp;<tt>" + m_config.useAuthentication() + "</tt><br/>");
//        if (m_config.useAuthentication()) {
//            buf.append("<strong>User:</strong>&nbsp;&nbsp;<tt>" + m_config.getUser() + "</tt><br/>");
//            buf.append("<strong>Password:</strong>&nbsp;&nbsp;<tt>" + (m_config.getPassword() != null) + "</tt><br/>");
//        }
//        long receiveTimeout = m_config.getReceiveTimeout().getSeconds();
//        buf.append("<strong>Receive timeout:</strong>&nbsp;&nbsp;<tt>");
//        if (receiveTimeout == 0) { buf.append("infinite"); }
//        else { buf.append(receiveTimeout + " seconds"); }
//        buf.append("</tt><br/>");
//        buf.append("<strong>Job check frequency:</strong>&nbsp;&nbsp;<tt>" + m_config.getJobCheckFrequency()
//            + " seconds</tt><br/>");
//
//        buf.append("<br/>");
//        buf.append("<strong>Context settings</strong><hr/>");
//        buf.append("<strong>Spark version:</strong>&nbsp;&nbsp;<tt>" + m_config.getSparkVersion() + "</tt><br>");
//        buf.append("<strong>Context name:</strong>&nbsp;&nbsp;<tt>" + m_config.getContextName() + "</tt><br>");
//        buf.append("<strong>Delete objects on dispose:</strong>&nbsp;&nbsp;<tt>" + m_config.deleteObjectsOnDispose()
//            + "</tt><br>");
//        buf.append("<strong>Override spark settings:</strong>&nbsp;&nbsp;<tt>" + m_config.overrideSparkSettings()
//            + "</tt><br>");
//        if (m_config.overrideSparkSettings()) {
//            buf.append(
//                "<strong>Custom settings:</strong>&nbsp;&nbsp;<tt>" + m_config.getCustomSparkSettings() + "</tt><br>");
//        }
//
//        System.out.println(buf.toString());

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("KNIMESparkContext [url=");
        builder.append(m_config.getJobServerUrl());
        builder.append(", auth=");
        builder.append(m_config.useAuthentication());
        builder.append(", user=");
        builder.append(m_config.getUser());
        builder.append(", password set=");
        builder.append(m_config.getPassword() != null);
        builder.append(", jobCheckFrequency=");
        builder.append(m_config.getJobCheckFrequency());
        builder.append(", receiveTimeout=");
        builder.append(m_config.getReceiveTimeout().getSeconds());
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
        } catch (SparkContextNotFoundException e) {
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
        } catch (SparkContextNotFoundException e) {
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
        } catch (SparkContextNotFoundException e) {
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
        } catch (SparkContextNotFoundException e) {
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
        } catch (SparkContextNotFoundException e) {
            resetToConfigured();
            throw e;
        }
    }
}
