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
 */
package org.knime.bigdata.spark.core.livy.context;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.livy.Job;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.knime.bigdata.commons.hadoop.ConfigurationFactory;
import org.knime.bigdata.commons.hadoop.UserGroupUtil;
import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyPrepareContextJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyPrepareContextJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.StagingAreaUtil;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.TextTemplateUtil;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.util.FileUtil;

/**
 * Spark context implementation for Apache Livy.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContext extends SparkContext<LivySparkContextConfig> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(LivySparkContext.class);

    private LivyJobController m_jobController;

    private NamedObjectsController m_namedObjectsController;

    private LivyClient m_livyClient;

    private RemoteFSController m_remoteFSController;

    private ContextAttributes m_contextAttributes;

    private class ContextAttributes {
        String sparkWebUI;

        Map<String, String> sparkConf;
    }

    /**
     * Creates a new Spark context that pushes jobs to Apache Livy.
     *
     * @param contextID The identfier for this context.
     */
    public LivySparkContext(final SparkContextID contextID) {
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
                m_livyClient = null;
                if (m_remoteFSController != null) {
                    m_remoteFSController.ensureClosed();
                    m_remoteFSController = null;
                }
                m_contextAttributes = null;
                m_jobController = null;
                m_namedObjectsController = null;
                break;
            default: // OPEN
                ensureRemoteFSConnection();
                ensureJobController();
                ensureNamedObjectsController();
                break;
        }
    }

    private void ensureRemoteFSConnection() throws KNIMESparkException {
        RemoteFSController tmpController = new RemoteFSController(getConfiguration().getRemoteFsConnectionInfo(),
            getConfiguration().getStagingAreaFolder());
        tmpController.createStagingArea();
        m_remoteFSController = tmpController;
    }

    private void ensureNamedObjectsController() {
        if (m_namedObjectsController == null) {
            m_namedObjectsController = new JobBasedNamedObjectsController(getID());
        }
    }

    @SuppressWarnings("unchecked")
    private void ensureJobController() throws KNIMESparkException {
        if (m_jobController == null) {
            final Class<Job<LivyJobOutput>> jobBindingClass = (Class<Job<LivyJobOutput>>)getJobJar().getDescriptor()
                .getJobBindingClasses().get(SparkContextIDScheme.SPARK_LIVY);

            m_jobController = new LivyJobController(m_livyClient, m_remoteFSController, jobBindingClass);
        }
    }

    private void ensureLivyClient() throws KNIMESparkException {
        if (m_livyClient == null) {
            final LivySparkContextConfig config = getConfiguration();
            final Properties livyHttpConf = createLivyHttpConf(config);

            LOGGER.debug("Creating new remote Spark context. Name: " + config.getContextName());
            try {
                if (config.getAuthenticationType() == AuthenticationType.KERBEROS) {

                    final Configuration baseHadoopConf = ConfigurationFactory.createBaseConfigurationWithKerberosAuth();

                    m_livyClient = UserGroupUtil.getKerberosTGTUser(baseHadoopConf)
                        .doAs(new PrivilegedExceptionAction<LivyClient>() {
                            @Override
                            public LivyClient run() throws Exception {
                                return buildLivyClient(livyHttpConf, config.getLivyUrl());
                            }
                        });
                } else {
                    m_livyClient = buildLivyClient(livyHttpConf, config.getLivyUrl());
                }
            } catch (PrivilegedActionException e) {
                // just rethrow the original exception thrown in the run() method
                throw new KNIMESparkException(e.getException());
            } catch (Exception e) {
                throw new KNIMESparkException(e);
            }
        }
    }

    private static Properties createLivyHttpConf(final LivySparkContextConfig config) {
        final Properties livyHttpConf = new Properties();

        // timeout until a connection is established. zero means infinite timeout. 
        livyHttpConf.setProperty("livy.client.http.connection.timeout",
            String.format("%ds", config.getConnectTimeoutSeconds()));

        // socket timeout (SO_TIMEOUT), which is the maximum period of inactivity between two consecutive
        // data packets). zero means infinite timeout. We use this to implement the response timeout.
        livyHttpConf.setProperty("livy.client.http.connection.socket.timeout",
            String.format("%ds", config.getResponseTimeoutSeconds()));

        // idle HTTP connections will be closed after this timeout
        livyHttpConf.setProperty("livy.client.http.connection.idle.timeout", "15s");

        // whether the target server is requested to compress content. 
        livyHttpConf.setProperty("livy.client.http.content.compress.enable", "true");

        // job status polling interval
        livyHttpConf.setProperty("livy.client.http.job.initial-poll-interval", "10ms");
        livyHttpConf.setProperty("livy.client.http.job.max-poll-interval",
            String.format("%ds", config.getJobCheckFrequencySeconds()));

        if (config.getAuthenticationType() == AuthenticationType.KERBEROS) {
            livyHttpConf.setProperty("livy.client.http.spnego.enable", "true");
            livyHttpConf.setProperty("livy.client.http.spnego.useSubjectCredentials", "true");
        } else {
            livyHttpConf.setProperty("livy.client.http.spnego.enable", "false");
        }

        // transfer all custom Spark settings
        for (Entry<String, String> customSparkSetting : config.getCustomSparkSettings().entrySet()) {
            livyHttpConf.setProperty(customSparkSetting.getKey(), customSparkSetting.getValue());
        }

        return livyHttpConf;
    }

    private static LivyClient buildLivyClient(final Properties livyHttpConf, final String livyUrl)
        throws IOException, URISyntaxException {
        final LivyClientBuilder builder = new LivyClientBuilder(false).setAll(livyHttpConf).setURI(new URI(livyUrl));

        final ClassLoader origCtxClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(LivySparkContext.class.getClassLoader());
            return builder.build();
        } finally {
            Thread.currentThread().setContextClassLoader(origCtxClassLoader);
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
     * {@inheritDoc}
     */
    @Override
    protected boolean open(final boolean createRemoteContext, final ExecutionMonitor exec) throws KNIMESparkException {
        boolean contextWasCreated = false;
        try {
            exec.setProgress(0, "Opening remote Spark context on Apache Livy");

            ensureLivyClient();
            setStatus(SparkContextStatus.OPEN);

            if (createRemoteContext) {
                contextWasCreated = createRemoteSparkContext();
            } else {
                throw new SparkContextNotFoundException(getID());
            }

            exec.setProgress(0.7, "Uploading Spark jobs");
            uploadJobJar();

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
        final String stagingTestfileName = uploadStagingTestfile();

        LivyPrepareContextJobInput prepInput = new LivyPrepareContextJobInput(getJobJar().getDescriptor().getHash(),
            getSparkVersion().toString(), getJobJar().getDescriptor().getPluginVersion(),
            IntermediateToSparkConverterRegistry.getConverters(getSparkVersion()),
            m_remoteFSController.getStagingArea(), m_remoteFSController.getStagingAreaReturnsPath(),
            stagingTestfileName);

        final LivyPrepareContextJobOutput output =
            SparkContextUtil.<LivyPrepareContextJobInput, LivyPrepareContextJobOutput> getJobRunFactory(getID(),
                LivyPrepareContextJobInput.LIVY_PREPARE_CONTEXT_JOB_ID).createRun(prepInput).run(getID());

        m_contextAttributes = new ContextAttributes();
        m_contextAttributes.sparkWebUI = output.getSparkWebUI();
        m_contextAttributes.sparkConf = output.getSparkConf();

        downloadStagingTestfile(output.getTestfileName());

    }

    private void downloadStagingTestfile(String testfileName) throws KNIMESparkException {
        try {
            try (final InputStream in = m_remoteFSController.download(testfileName)) {
                StagingAreaUtil.validateTestfileContent(in);
            } finally {
                m_remoteFSController.delete(testfileName);
            }
        } catch (Exception e) {
            throw new KNIMESparkException("Remote file system download test failed: " + e.getMessage(), e);
        }
    }

    private String uploadStagingTestfile() throws KNIMESparkException {
        File tmpFile = null;
        try {
            tmpFile = FileUtil.createTempFile("uploadtest", "livy");
            try (final OutputStream out = new FileOutputStream(tmpFile)) {
                StagingAreaUtil.writeTestfileContent(out);
            }
            return m_remoteFSController.upload(tmpFile);
        } catch (Exception e) {
            throw new KNIMESparkException("Remote file system upload test failed: " + e.getMessage(), e);
        } finally {
            if (tmpFile != null) {
                tmpFile.delete();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy() throws KNIMESparkException {
        LOGGER.info("Destroying Livy Spark context ");
        try {
            ensureLivyClient();
            m_livyClient.stop(true);
        } finally {
            m_livyClient = null;
            setStatus(SparkContextStatus.CONFIGURED);
        }
    }

    private void uploadJobJar() throws KNIMESparkException {
        try {
            final File jobJarFile = getJobJar().getJarFile();
            LOGGER.debug(String.format("Uploading job jar: %s", jobJarFile.getAbsolutePath()));
            Future<?> uploadFuture = m_livyClient.uploadJar(jobJarFile);
            waitForFuture(uploadFuture, null);
        } catch (Exception e) {
            handleLivyException(e);
        }
    }

    private boolean createRemoteSparkContext() throws KNIMESparkException {
        try {
            m_livyClient.startOrConnectSession();
        } catch (Exception e) {
            handleLivyException(e);
        }

        return true;

    }

    static <O> O waitForFuture(final Future<O> future, final ExecutionMonitor exec)
        throws CanceledExecutionException, KNIMESparkException {

        while (true) {
            try {
                return future.get(500, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                if (exec != null) {
                    try {
                        exec.checkCanceled();
                    } catch (CanceledExecutionException canceledInKNIME) {
                        future.cancel(true);
                        throw canceledInKNIME;
                    }
                }
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof KNIMESparkException) {
                    throw (KNIMESparkException)cause;
                } else {
                    throw new KNIMESparkException(e);
                }
            } catch (InterruptedException e) {
                throw new KNIMESparkException("Execution was interrupted");
            } catch (Exception e) {
                handleLivyException(e);
            }
        }
    }

    /**
     * Livy client generally catches all exceptions and wraps them as RuntimeException. This method extracts cause and
     * message and wraps them properly inside a {@link KNIMESparkException}.
     *
     * @param e An exception thrown by the programmatic Livy API
     * @throws Exception Rewrapped {@link KNIMESparkException} or the original exception.
     */
    static void handleLivyException(final Exception e) throws KNIMESparkException {
        // livy client catches all exceptions and wraps them as RuntimeException
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

        final LivySparkContextConfig config = getConfiguration();
        final Map<String, String> reps = new HashMap<>();

        reps.put("spark_version", config.getSparkVersion().toString());
        reps.put("url", config.getLivyUrl());
        reps.put("authentication", createAuthenticationInfoString());
        reps.put("context_state", getStatus().toString());
        reps.put("spark_web_ui", (m_contextAttributes != null) ? m_contextAttributes.sparkWebUI : "unavailable");
        reps.put("spark_properties", mkSparkPropertiesHTMLRows());

        try (InputStream r = getClass().getResourceAsStream("context_html_description.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }

    private String mkSparkPropertiesHTMLRows() {
        if (m_contextAttributes == null) {
            return "<tr><td>unavailable</td><td></td></tr>";
        }
        
        ArrayList<String> sortedProperties = new ArrayList<>(m_contextAttributes.sparkConf.keySet());
        Collections.sort(sortedProperties);
        final StringBuilder buf = new StringBuilder();
        for (String property : sortedProperties) {
            buf.append(String.format("<tr><td>%s</td><td>%s</td></tr>\n", property,
                m_contextAttributes.sparkConf.get(property)));
        }
        return buf.toString();
    }

    private String createAuthenticationInfoString() {
        final LivySparkContextConfig config = getConfiguration();
        if (config.getAuthenticationType() == AuthenticationType.KERBEROS) {
            try {
                return String.format("Kerberos (authenticated as: %s)", UserGroupUtil
                    .getKerberosTGTUser(ConfigurationFactory.createBaseConfigurationWithKerberosAuth()).getUserName());
            } catch (Exception e) {
                return "Kerberos";
            }
        } else {
            return "None";
        }
    }

    private static String renderCustomSparkSettings(final Map<String, String> customSparkSettings) {
        final StringBuilder buf = new StringBuilder();

        for (String key : customSparkSettings.keySet()) {
            buf.append(key);
            buf.append(": ");
            buf.append(customSparkSettings.get(key));
            buf.append("\n");
        }
        return buf.toString();
    }
}
