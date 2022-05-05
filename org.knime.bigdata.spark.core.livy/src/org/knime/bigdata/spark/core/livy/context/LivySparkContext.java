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
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.livy.CreateSessionHandle;
import org.apache.livy.Handle;
import org.apache.livy.Job;
import org.apache.livy.JobHandle;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.knime.bigdata.commons.config.CommonConfigContainer;
import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyPrepareContextJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyPrepareContextJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.StagingAreaTester;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.TextTemplateUtil;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.kerberos.api.KerberosProvider;
import org.knime.kerberos.api.KerberosState;

/**
 * Spark context implementation for Apache Livy.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContext extends SparkContext<LivySparkContextConfig> {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(LivySparkContext.class);

    private LivyJobController m_jobController;

    private JobBasedNamedObjectsController m_namedObjectsController;

    private LivyClient m_livyClient;

    private RemoteFSController m_remoteFSController;

    private ContextAttributes m_contextAttributes;

    private class ContextAttributes {
        String sparkWebUI;

        Map<String, String> sparkConf;

        Map<String, String> systemProperties;

        boolean adaptiveExecutionEnabled;
    }

    private KNIMEToIntermediateConverterParameter m_converterParameter;

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

    @SuppressWarnings("unchecked")
    private void ensureJobController() throws KNIMESparkException {
        if (m_jobController == null) {
            final Class<Job<WrapperJobOutput>> jobBindingClass = (Class<Job<WrapperJobOutput>>)getJobJar().getDescriptor()
                .getWrapperJobClasses().get(SparkContextIDScheme.SPARK_LIVY);

            m_jobController = new LivyJobController(m_livyClient, m_remoteFSController, jobBindingClass, m_namedObjectsController);
        }
    }

    private void ensureLivyClient(final ExecutionMonitor exec) throws KNIMESparkException {
        if (m_livyClient == null) {
            final LivySparkContextConfig config = getConfiguration();
            final Properties livyHttpConf = createLivyHttpConf(config);
            final String livyUrl = config.getLivyUrlWithAuthentication();
            final String livyUrlLog = config.getLivyUrlWithoutAuthentication();

            LOGGER.debug(String.format("Creating new remote Spark context %s at %s with authentication %s.",
                config.getSparkContextID(), livyUrlLog, config.getAuthenticationType()));

            try {
                if (config.getAuthenticationType() == AuthenticationType.KERBEROS) {
                    m_livyClient = KerberosProvider
                        .doWithKerberosAuthBlocking(() -> buildLivyClient(livyHttpConf, livyUrl), exec);
                } else {
                    m_livyClient = buildLivyClient(livyHttpConf, livyUrl);
                }
            } catch (final Exception e) {
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

        setProxyUserIfNecessary(config, livyHttpConf);

        // transfer kryo serializer version if set
        if (config.getCustomSparkSettings().containsKey("spark.knime.kryo.serializer")) {
            livyHttpConf.setProperty("livy.client.http.kryo.serializer",
                config.getCustomSparkSettings().get("spark.knime.kryo.serializer"));
        }

        // transfer all custom Spark settings
        for (final Entry<String, String> customSparkSetting : config.getCustomSparkSettings().entrySet()) {
            final String settingName = customSparkSetting.getKey();
            final String settingsValue = customSparkSetting.getValue();
            if (!settingName.startsWith("livy.client.http.") && settingsValue == null) {
                livyHttpConf.setProperty(settingName, "");
            } else if (!settingName.startsWith("livy.client.http.")) {
                livyHttpConf.setProperty(settingName, settingsValue);
            } else {
                LOGGER.warn(String.format(
                    "Ignoring custom Spark setting %s=%s. Custom Spark settings must not start with \"livy.client.http\"",
                    settingName, customSparkSetting.getValue()));
            }
        }

        return livyHttpConf;
    }

    private static void setProxyUserIfNecessary(final LivySparkContextConfig config, final Properties livyHttpConf) {
        final String proxyUser;

        switch (config.getAuthenticationType()) {
            case KERBEROS:
                proxyUser = CommonConfigContainer.getInstance().getUserToImpersonate().orElse(null);
                break;
            case USER:
                proxyUser = extractUserFromLivyUrl(config);
                break;
            default:
                proxyUser = null;
                break;
        }

        if (proxyUser != null) {
            LOGGER.info(String.format("Opening Spark context with proxyUser=%s", proxyUser));
            livyHttpConf.setProperty("livy.client.http.proxyUser", proxyUser);
        }
    }

    private static String extractUserFromLivyUrl(final LivySparkContextConfig config) {
        try {
            final URL livyUrl = new URL(config.getLivyUrlWithAuthentication());
            final String encodedUser = livyUrl.getUserInfo().split(":", 2)[0];
            return URLDecoder.decode(encodedUser, StandardCharsets.UTF_8.name());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Invalid URL: " + config.getLivyUrlWithAuthentication(), e);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("Invalid encoded username in URL: " + config.getLivyUrlWithAuthentication(), e);
        }
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
    protected boolean open(final boolean createRemoteContext, final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        boolean contextWasCreated = false;
        try {
            if (!createRemoteContext) {
                throw new SparkContextNotFoundException(getID());
            }

            exec.setProgress(0, "Opening remote Spark context on Apache Livy");

            ensureLivyClient(exec);
            setStatus(SparkContextStatus.OPEN);

            createRemoteSparkContext(exec);
            contextWasCreated = true;

            if (m_livyClient.requiresKryoSerializerDetector()) {
                exec.setProgress(0.5, "Uploading and running job to detect kryo serializer version.");
                detectKryoVersion(exec);
            }

            exec.setProgress(0.6, "Uploading Spark jobs");
            uploadJobJar(exec);
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

    private void detectKryoVersion(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        LOGGER.debug("Uploading Kryo version detector job jar.");
        uploadKryoVersionDetectorJob(exec);

        LOGGER.debug("Running Kryo version detector job jar.");
        final String kryoVersion = runKryoVersionDetectorJob(exec);

        LOGGER.debug("Using Kryo serializer version: " + kryoVersion);
        m_livyClient.setKryoSerializerVersion(kryoVersion);
    }

    private void validateAndPrepareContext(final ExecutionMonitor exec) throws KNIMESparkException {
        exec.setProgress(0.7, "Testing file upload on file system connection");
        final String stagingTestfileName = uploadStagingTestfile();

        exec.setProgress(0.8, "Running job to prepare Spark context");
        final LivyPrepareContextJobInput prepInput = new LivyPrepareContextJobInput(getJobJar().getDescriptor().getHash(),
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
        m_contextAttributes.systemProperties = output.getSystemProperties();
        m_contextAttributes.adaptiveExecutionEnabled = output.adaptiveExecutionEnabled();

        m_converterParameter = getConfiguration().getConverterParameter(m_contextAttributes.sparkConf,
            m_contextAttributes.systemProperties);

        if (getConfiguration().failOnDifferentClusterTimeZone()) {
            validateClusterTimeZone(getConfiguration(), m_contextAttributes.systemProperties);
        }

        exec.setProgress(0.9, "Testing file download on file system connection");
        downloadStagingTestfile(output.getTestfileName());
    }

    /**
     * Validate that cluster uses the same time zone set in configuration and throw an exception otherwise.
     */
    private static void validateClusterTimeZone(final LivySparkContextConfig config,
        final Map<String, String> systemProperties) throws KNIMESparkException {

        final ZoneId clusterTimeZone;
        final ZoneId expectedTimeZone = config.getTimeShiftZone();

        if (systemProperties.containsKey("user.timezone")) {
            clusterTimeZone = ZoneId.of(systemProperties.get("user.timezone"));
        } else {
            throw new KNIMESparkException("Unable to identifiy cluster default time zone.");
        }

        if (!expectedTimeZone.equals(clusterTimeZone)) {
            throw new KNIMESparkException(
                "Found different time zone " + clusterTimeZone + " on cluster than expected " + expectedTimeZone + ".");
        }
    }

    private void downloadStagingTestfile(final String testfileName) throws KNIMESparkException {
        try {
            StagingAreaTester.validateTestfileContent(m_remoteFSController, testfileName);
        } catch (final Exception e) {
            throw new KNIMESparkException("Remote file system download test failed: " + e.getMessage(), e);
        } finally {
            m_remoteFSController.deleteSafely(testfileName);
        }

    }

    private String uploadStagingTestfile() throws KNIMESparkException {
        try {
            return StagingAreaTester.writeTestfileContent(m_remoteFSController);
        } catch (final Exception e) {
            throw new KNIMESparkException("Remote file system upload test failed: " + e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy() throws KNIMESparkException {
        LOGGER.info("Destroying Livy Spark context ");
        try {
            if (getStatus() == SparkContextStatus.OPEN) {
                m_livyClient.stop(true);
            }
        } finally {
            setStatus(SparkContextStatus.CONFIGURED);
        }
    }

    private void uploadKryoVersionDetectorJob(final ExecutionMonitor exec) throws KNIMESparkException {
        try {
            final Future<?> uploadFuture = m_livyClient.uploadKryoSerializerDetector();
            waitForFuture(uploadFuture, exec);
        } catch (final Exception e) {
            throw new KNIMESparkException("Failed to upload Kryo version detector job: " + e.getMessage(), e);
        }
    }

    private String runKryoVersionDetectorJob(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        final JobHandle<String> handle = m_livyClient.runKryoSerializerDetector();
        final String kryoVersion = waitForFuture(handle, exec);
        handleOperationResult(handle);
        return kryoVersion;
    }

    private void uploadJobJar(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        final File jobJarFile = getJobJar().getJarFile();
        LOGGER.debug(String.format("Uploading job jar: %s", jobJarFile.getAbsolutePath()));
        final Future<?> uploadFuture = m_livyClient.uploadJar(jobJarFile);
        waitForFuture(uploadFuture, exec);
    }

    private void createRemoteSparkContext(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        final CreateSessionHandle handle = m_livyClient.startOrConnectSession();
        waitForFuture(handle, exec);
        handleOperationResult(handle);
    }

    static void handleOperationResult(final Handle<?> handle) throws KNIMESparkException, CanceledExecutionException {
        switch(handle.getHandleState()) {
            case DONE_CANCELLED:
                throw new CanceledExecutionException();
            case DONE_ERROR:
                throw new KNIMESparkException(handle.getError());
            case DONE_SUCCESS:
                break;
            default:
                // should never happen
                throw new RuntimeException("Unexpected state: " + handle.getHandleState());
        }
    }

    static <O> O waitForFuture(final Future<O> future, final ExecutionMonitor exec)
        throws CanceledExecutionException, KNIMESparkException {

        while (true) {
            try {
                return future.get(500, TimeUnit.MILLISECONDS);
            } catch (final TimeoutException | InterruptedException e) {
                checkForCancelation(future, exec);
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof KNIMESparkException) {
                    throw (KNIMESparkException)cause;
                } else {
                    throw new KNIMESparkException(e);
                }
            } catch (final Exception e) {
                handleLivyException(e);
            }
        }
    }

    private static void checkForCancelation(final Future<?> future, final ExecutionMonitor exec) throws CanceledExecutionException {
        if (exec != null) {
            try {
                exec.checkCanceled();
            } catch (final CanceledExecutionException canceledInKNIME) {
                future.cancel(true);
                throw canceledInKNIME;
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
        reps.put("url", config.getLivyUrlWithoutAuthentication());
        reps.put("authentication", createAuthenticationInfoString());
        reps.put("context_state", getStatus().toString());
        reps.put("time_shift",
            m_converterParameter != null ? m_converterParameter.getTimeShiftDescription() : "unavailable");
        reps.put("spark_web_ui", m_contextAttributes != null && m_contextAttributes.sparkWebUI != null ?
            m_contextAttributes.sparkWebUI : "unavailable");
        reps.put("spark_properties",
            mkPropertiesHTMLRows(m_contextAttributes != null ? m_contextAttributes.sparkConf : null));
        reps.put("sys_properties",
            mkPropertiesHTMLRows(m_contextAttributes != null ? m_contextAttributes.systemProperties : null));
        reps.put("adaptiveExecutionEnabled",
            m_contextAttributes != null ? Boolean.toString(m_contextAttributes.adaptiveExecutionEnabled) : null);

        try (InputStream r = getClass().getResourceAsStream("context_html_description.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (final IOException e) {
            throw new RuntimeException("Failed to read context description template");
        }
    }

    private static String mkPropertiesHTMLRows(final Map<String, String> props) {
        if (props == null) {
            return "<tr><td>unavailable</td><td></td></tr>";
        }

        final ArrayList<String> sortedProperties = new ArrayList<>(props.keySet());
        Collections.sort(sortedProperties);
        final StringBuilder buf = new StringBuilder();
        for (final String property : sortedProperties) {
            buf.append(String.format("<tr><td>%s</td><td>%s</td></tr>\n", property,
                props.get(property)));
        }
        return buf.toString();
    }

    private String createAuthenticationInfoString() {
        final LivySparkContextConfig config = getConfiguration();
        if (config.getAuthenticationType() == AuthenticationType.KERBEROS) {
            final KerberosState krbState = KerberosProvider.getKerberosState();
            if (krbState.isAuthenticated()) {
                return String.format("Kerberos (authenticated as: %s)", krbState.getPrincipal());
            } else {
                return "Kerberos (currently not logged in)";
            }
        } else if (config.getAuthenticationType() == AuthenticationType.CREDENTIALS
            || config.getAuthenticationType() == AuthenticationType.USER
            || config.getAuthenticationType() == AuthenticationType.USER_PWD) {

            final String[] userInfo = URI.create(config.getLivyUrlWithAuthentication()).getUserInfo().split(":");
            try {
                return String.format("Basic (user: %s)", URLDecoder.decode(userInfo[0], StandardCharsets.UTF_8.name()));
            } catch (UnsupportedEncodingException e) {
                return "Basic";
            }
        } else {
            return "None";
        }
    }

    private static String renderCustomSparkSettings(final Map<String, String> customSparkSettings) {
        final StringBuilder buf = new StringBuilder();

        for (final String key : customSparkSettings.keySet()) {
            buf.append(key);
            buf.append(": ");
            buf.append(customSparkSettings.get(key));
            buf.append("\n");
        }
        return buf.toString();
    }

    /**
     * Fetches the Spark driver logs.
     *
     * @param rows The number of rows to fetch.
     * @param exec Execution monitor to check for cancelation.
     * @return a list of the last lines from the Spark driver log.
     * @throws KNIMESparkException if context was not OPEN or something went wrong while fetching the logs.
     * @throws CanceledExecutionException if given {@link ExecutionMonitor} was canceled.
     */
    public synchronized List<String> getSparkDriverLogs(final int rows, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        switch (getStatus()) {
            case NEW:
            case CONFIGURED:
                throw new KNIMESparkException("Spark context does not exist (anymore).");
            default: // this is actually OPEN
                return waitForFuture(m_livyClient.getDriverLog(rows), exec);
        }
    }

    @Override
    public synchronized KNIMEToIntermediateConverterParameter getConverterPrameter() {
        return m_converterParameter;
    }

    @Override
    public boolean adaptiveExecutionEnabled() {
        return m_contextAttributes != null && m_contextAttributes.adaptiveExecutionEnabled;
    }
}
