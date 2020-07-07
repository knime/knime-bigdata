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
package org.knime.bigdata.spark.core.sparkjobserver.context;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextConstants;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.sparkjobserver.request.CreateContextRequest;
import org.knime.bigdata.spark.core.sparkjobserver.request.DestroyContextRequest;
import org.knime.bigdata.spark.core.sparkjobserver.request.GetContextsRequest;
import org.knime.bigdata.spark.core.sparkjobserver.request.GetJarsRequest;
import org.knime.bigdata.spark.core.sparkjobserver.request.UploadFileRequest;
import org.knime.bigdata.spark.core.sparkjobserver.rest.RestClient;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.TextTemplateUtil;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.KNIMEConstants;
import org.knime.core.node.NodeLogger;

/**
 * Spark context implementation for Spark Jobserver.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class JobserverSparkContext extends SparkContext<JobServerSparkContextConfig> {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(JobserverSparkContext.class);

    /**
     * A REST client to talk to Spark Jobserver.
     */
    private RestClient m_restClient;

    /**
     * The "app name" for Spark Jobserver, which is an identifier for the uploaded job jar, that has to be specified
     * with each job. See {@link #createJobserverAppname(SparkVersion, String)}.
     */
    private String m_jobserverAppName;


    private JobserverJobController m_jobController;

    private JobBasedNamedObjectsController m_namedObjectsController;


    /**
     * Creates a new Spark context that pushes jobs to the Spark jobserver.
     *
     * @param contextID The identfier for this context.
     */
    public JobserverSparkContext(final SparkContextID contextID) {
        super(contextID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setStatus(final SparkContextStatus newStatus) throws KNIMESparkException {
        super.setStatus(newStatus);

        switch(newStatus) {
            case NEW:
            case CONFIGURED:
                m_restClient = null;
                m_jobserverAppName = null;
                m_namedObjectsController = null;
                m_jobController = null;
                break;
            default: // OPEN
                ensureRestClient();
                ensureJobserverAppname();
                ensureNamedObjectsController();
                ensureJobController();
                break;
        }
    }

    private void ensureNamedObjectsController() {
        if (m_namedObjectsController == null) {
            m_namedObjectsController = new JobBasedNamedObjectsController(getID());
        }
    }

    private void ensureJobController() throws KNIMESparkException {
        if (m_jobController == null) {
        	final Class<?> jobBindingClass = getJobJar().getDescriptor().getWrapperJobClasses().get(SparkContextIDScheme.SPARK_JOBSERVER);
        	if (jobBindingClass == null) {
        		throw new KNIMESparkException("Missing Spark job binding class for Spark Jobserver.");
        	}
        	
            m_jobController = new JobserverJobController(getID(),
                getConfiguration(),
                m_jobserverAppName,
                m_restClient,
                jobBindingClass.getName(),
                m_namedObjectsController);
        }
    }

    private void ensureRestClient() throws KNIMESparkException {
        if (m_restClient == null) {
            try {
                m_restClient = new RestClient(getConfiguration());
            } catch (KeyManagementException | NoSuchAlgorithmException | URISyntaxException
                    | UnsupportedEncodingException e) {
                throw new KNIMESparkException(e);
            }
        }
    }

    private void ensureJobserverAppname() throws KNIMESparkException {
        m_jobserverAppName = createJobserverAppname(getJobJar().getDescriptor().getHash());
    }

    /**
     * @return the "app name" for the jobserver, which is an identifier for the uploaded job jar, that has to be
     *         specified with each job.
     */
    private String createJobserverAppname(final String jobJarHash) {
        final String knimeInstanceID = KNIMEConstants.getKNIMEInstanceID();
        return String.format("knimeJobs_%s_%s_spark-%s", knimeInstanceID.substring(knimeInstanceID.indexOf('-') + 1),
            jobJarHash, getSparkVersion().toString());
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
     * {@inheritDoc}
     */
    @Override
    protected boolean open(final boolean createRemoteContext, final ExecutionMonitor exec) throws KNIMESparkException {
        boolean contextWasCreated = false;

        try {
        	exec.setProgress(0, "Opening remote context on Spark Jobserver");
        	
            setStatus(SparkContextStatus.OPEN);

            if (!remoteSparkContextExists()) {
                if (createRemoteContext) {
                    contextWasCreated = createRemoteSparkContext();
                } else {
                    throw new SparkContextNotFoundException(getID());
                }
            }
            
            exec.setProgress(0.7, "Uploading Spark jobs");
            // regardless of contextWasCreated is true or not we can assume that the context exists now
            // (somebody else may have created it before us)
            if (!isJobJarUploaded()) {
                uploadJobJar();
            }
            
            exec.setProgress(0.9, "Running job to prepare context");
            validateAndPrepareContext();
            exec.setProgress(1);
        } catch (KNIMESparkException e) {

            // make sure we don't leave a broken context behind
            if (contextWasCreated) {
                try {
                    destroy();
                } catch (KNIMESparkException toIgnore) {
                    // ignore
                }
            }

            setStatus(SparkContextStatus.CONFIGURED);
            throw e;
        }

        return contextWasCreated;
    }

    private void validateAndPrepareContext() throws KNIMESparkException {
        PrepareContextJobInput prepInput = new PrepareContextJobInput(getJobJar().getDescriptor().getHash(),
            getSparkVersion().toString(),
            getJobJar().getDescriptor().getPluginVersion(),
            IntermediateToSparkConverterRegistry.getConverters(getSparkVersion()));

        SparkContextUtil.getSimpleRunFactory(getID(), SparkContextConstants.PREPARE_CONTEXT_JOB_ID)
            .createRun(prepInput).run(getID());
    }



    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy() throws KNIMESparkException {
        try {
            LOGGER.info("Destroying context " + getConfiguration().getContextName());
            ensureRestClient();

            new DestroyContextRequest(getID(), getConfiguration(), m_restClient).send();

            // wait up to 10s until context is removed from running list
            for (int i = 0; i < 50 && remoteSparkContextExists(); i++) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                }
            }
        } catch (SparkContextNotFoundException e) {
            // do nothing, this is alright
        } finally {
            setStatus(SparkContextStatus.CONFIGURED);
        }
    }


    /**
     * @return <code>true</code> if the context exists
     * @throws KNIMESparkException
     */
    private boolean remoteSparkContextExists() throws KNIMESparkException {
        final JobServerSparkContextConfig config = getConfiguration();

        LOGGER.debug("Checking if remote context exists. Name: " + config.getContextName());
        final JsonArray contexts = new GetContextsRequest(getID(), config, m_restClient).send();

        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Available remote contexts: " + contexts);
        }

        boolean toReturn = false;

        for (int i = 0; i < contexts.size(); i++) {
            if (config.getContextName().equals(contexts.getString(i))) {
                LOGGER.debug("Remote context exists. Name: " + config.getContextName());
                toReturn = true;
                break;
            }
        }

        if (!toReturn) {
            LOGGER.debug("Remote context does not exist. Name: " + config.getContextName());
        }

        return toReturn;
    }

    /**
     * @param context the {@link JobServerSparkContextConfig} to use for checking job jar existence
     * @return <code>true</code> if the jar is uploaded, false otherwise
     * @throws KNIMESparkException
     */
    private boolean isJobJarUploaded() throws KNIMESparkException {

        LOGGER.debug("Checking if job jar is uploaded.");
        final JsonObject jars = new GetJarsRequest(getID(), getConfiguration(), m_restClient).send();

        if (jars.containsKey(m_jobserverAppName)) {
            LOGGER.debug("Job jar is uploaded");
            return true;
        } else {
            LOGGER.debug("Job jar is not uploaded");
            return false;
        }
    }

    private void uploadJobJar() throws KNIMESparkException {
        LOGGER.debug(String.format("Uploading job jar: %s", getJobJar().getJarFile().getAbsolutePath()));
        // uploads or overwrites any existing jar file uploaded from this workspace
        new UploadFileRequest(getID(), getConfiguration(), m_restClient, getJobJar().getJarFile().toPath(),
            JobserverConstants.buildJarPath(m_jobserverAppName)).send();
    }

    private boolean createRemoteSparkContext() throws KNIMESparkException {
        LOGGER.debug("Creating new remote Spark context. Name: " + getConfiguration().getContextName());
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Context settings: " + getConfiguration());
        }

        return new CreateContextRequest(getID(), getConfiguration(), m_restClient).send();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHTMLDescription() {
        if (getStatus() == SparkContextStatus.NEW) {
            return "<strong>Spark context is currently unconfigured.</strong>";
        }

        final JobServerSparkContextConfig config = getConfiguration();
        final Map<String, String> reps = new HashMap<>();

        reps.put("url", config.getJobServerUrl());
        reps.put("use_authentication", config.useAuthentication()
            ? String.format("true (user: %s)", config.getUser())
            : "false");
        reps.put("receive_timeout", (config.getReceiveTimeout().getSeconds() == 0)
            ? "infinite"
            : Long.toString(config.getReceiveTimeout().getSeconds()));
        reps.put("job_check_frequency", Integer.toString(config.getJobCheckFrequency()));
        reps.put("spark_version", config.getSparkVersion().toString());
        reps.put("context_name", config.getContextName());
        reps.put("delete_data_on_dispose", Boolean.toString(config.deleteObjectsOnDispose()));
        reps.put("override_settings", Boolean.toString(config.useCustomSparkSettings()));
        reps.put("custom_settings", config.useCustomSparkSettings()
            ? renderCustomSparkSettings(config.getCustomSparkSettings())
            : "(not applicable)");
        reps.put("context_state", getStatus().toString());
        
        try (InputStream r = getClass().getResourceAsStream("context_html_description.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }

    private static String renderCustomSparkSettings(final Map<String, String> customSparkSettings) {
        final StringBuffer buf= new StringBuffer();

        for (String key : customSparkSettings.keySet()) {
            buf.append(key);
            buf.append(": ");
            buf.append(customSparkSettings.get(key));
            buf.append("\n");
        }
        return buf.toString();
    }

    @Override
    public synchronized KNIMEToIntermediateConverterParameter getConverterPrameter() {
        return KNIMEToIntermediateConverterParameter.DEFAULT;
    }
}
