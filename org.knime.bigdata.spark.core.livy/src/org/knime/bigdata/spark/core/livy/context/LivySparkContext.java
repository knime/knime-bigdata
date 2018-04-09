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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
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
import org.knime.bigdata.spark.core.livy.LivyPlugin;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

/**
 * Spark context implementation for Apache Livy.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class LivySparkContext extends SparkContext<LivySparkContextConfig> {

	private final static NodeLogger LOGGER = NodeLogger.getLogger(LivySparkContext.class);

	private LivyJobController m_jobController;

	private NamedObjectsController m_namedObjectsController;

	private LivyClient m_livyClient;

	/**
	 * Creates a new Spark context that pushes jobs to Apache Livy.
	 *
	 * @param contextID
	 *            The identfier for this context.
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
			m_jobController = null;
			m_namedObjectsController = null;
			break;
		default: // OPEN
			ensureJobController();
			ensureNamedObjectsController();
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
			final Class<?> jobBindingClass = getJobJar().getDescriptor().getJobBindingClasses()
					.get(SparkContextIDScheme.SPARK_LIVY);

			m_jobController = new LivyJobController(getConfiguration(), m_livyClient, jobBindingClass.getName());
		}
	}

	private void ensureLivyClient() throws KNIMESparkException {
		if (m_livyClient == null) {
			try {
				final Properties livyClientConf = new Properties();
				livyClientConf.load(getClass().getResource("/livy-client.conf").openStream());

				// this is temporary until we fix how SPNEGO/Kerberos is configured for the Livy client API
				final String loginConf = String.join(File.separator, LivyPlugin.getDefault().getPluginRootPath(), "conf", "spnegoLogin.conf");
				livyClientConf.setProperty("livy.client.http.auth.login.config", loginConf);

				final LivySparkContextConfig config = getConfiguration();

				LOGGER.debug("Creating new remote Spark context. Name: " + config.getContextName());

				final LivyClientBuilder builder = new LivyClientBuilder(false).setAll(livyClientConf).setURI(new URI(config.getLivyUrl()));
				
				final ClassLoader origCtxClassLoader = Thread.currentThread().getContextClassLoader();
				try {
					Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
					m_livyClient = builder.build();
				} finally {
					Thread.currentThread().setContextClassLoader(origCtxClassLoader);
				}

			} catch (Exception e) {
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
	 * {@inheritDoc}
	 */
	@Override
	protected boolean open(final boolean createRemoteContext, final ExecutionMonitor exec) throws KNIMESparkException {
		boolean contextWasCreated = false;
		try {
			exec.setProgress(0, "Opening remote Spark context on Apache Livy");

			ensureLivyClient();
			setStatus(SparkContextStatus.OPEN);

			if (!remoteSparkContextExists()) {
				if (createRemoteContext) {
					contextWasCreated = createRemoteSparkContext();
				} else {
					throw new SparkContextNotFoundException(getID());
				}
			}

			exec.setProgress(0.7, "Uploading Spark jobs");
			// regardless of contextWasCreated is true or not we can assume that
			// the context exists now
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
		PrepareContextJobInput prepInput = PrepareContextJobInput.create(getJobJar().getDescriptor().getHash(),
				getSparkVersion().toString(), getJobJar().getDescriptor().getPluginVersion(),
				IntermediateToSparkConverterRegistry.getConverters(getSparkVersion()));

		SparkContextUtil.getSimpleRunFactory(getID(), SparkContextConstants.PREPARE_CONTEXT_JOB_ID).createRun(prepInput)
				.run(getID());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void destroy() throws KNIMESparkException {
        LOGGER.info("Destroying context " + getConfiguration().getContextName());
        try {
        	ensureLivyClient();
            m_livyClient.stop(true);
        } finally {
            m_livyClient = null;
            setStatus(SparkContextStatus.CONFIGURED);
        }
	}

	/**
	 * @return <code>true</code> if the context exists
	 * @throws KNIMESparkException
	 */
	private boolean remoteSparkContextExists() throws KNIMESparkException {
		// FIXME right new we don't have a way to determine whether a remote
		// context with the name already exists
		return false;
	}

	/**
	 * @param context
	 *            the {@link JobServerSparkContextConfig} to use for checking
	 *            job jar existence
	 * @return <code>true</code> if the jar is uploaded, false otherwise
	 * @throws KNIMESparkException
	 */
	private boolean isJobJarUploaded() throws KNIMESparkException {
		// FIXME this is currently not possible with the Livy REST API
		return false;
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
					throw (KNIMESparkException) cause;
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
	 * Livy client generally catches all exceptions and wraps them as
	 * RuntimeException. This method extracts cause and message and wraps them
	 * properly inside a {@link KNIMESparkException}.
	 *
	 * @param e
	 *            An exception thrown by the programmatic Livy API
	 * @throws Exception
	 *             Rewrapped {@link KNIMESparkException} or the original
	 *             exception.
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

		 reps.put("url", config.getLivyUrl());
//		 reps.put("use_authentication", config.useAuthentication()
//		 ? String.format("true (user: %s)", config.getUser())
//		 : "false");
//		 reps.put("receive_timeout", (config.getReceiveTimeout().getSeconds()
//		 == 0)
//		 ? "infinite"
//		 : Long.toString(config.getReceiveTimeout().getSeconds()));
//		 reps.put("job_check_frequency",
//		 Integer.toString(config.getJobCheckFrequency()));
		 reps.put("spark_version", config.getSparkVersion().toString());
		 reps.put("context_name", config.getContextName());
		 reps.put("delete_data_on_dispose",
		 Boolean.toString(config.deleteObjectsOnDispose()));
		 reps.put("override_settings",
		 Boolean.toString(config.useCustomSparkSettings()));
		 reps.put("custom_settings", config.useCustomSparkSettings()
		 ? renderCustomSparkSettings(config.getCustomSparkSettings())
		 : "(not applicable)");
		 reps.put("context_state", getStatus().toString());

		return generateHTMLDescription("context_html_description.template", reps);
	}

	private String renderCustomSparkSettings(final Map<String, String> customSparkSettings) {
		final StringBuffer buf = new StringBuffer();

		for (String key : customSparkSettings.keySet()) {
			buf.append(key);
			buf.append(": ");
			buf.append(customSparkSettings.get(key));
			buf.append("\n");
		}
		return buf.toString();
	}
}
