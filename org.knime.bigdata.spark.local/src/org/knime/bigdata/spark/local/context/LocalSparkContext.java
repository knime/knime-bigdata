/**
 *
 */
package org.knime.bigdata.spark.local.context;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.context.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.TextTemplateUtil;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapper;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapperFactory;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

/**
 * Implementation of {@link SparkContext} for local Spark. This class uses the
 * {@link LocalSparkWrapper} interface to actually create a local Spark context
 * and run jobs in it.
 *
 * @author Oleg Yasnev, KNIME GmbH
 */
public class LocalSparkContext extends SparkContext<LocalSparkContextConfig> {

	private static final String SPARK_EXECUTOR_EXTRA_CLASS_PATH = "spark.executor.extraClassPath";

	private static final String SPARK_DRIVER_EXTRA_CLASS_PATH = "spark.driver.extraClassPath";

	private static final String SPARK_JARS = "spark.jars";

	private final static NodeLogger LOGGER = NodeLogger.getLogger(LocalSparkContext.class);

	/**
	 * Job ID for the Spark context preparation job (Spark 2.2)
	 */
	public final static String PREPARE_LOCAL_SPARK_CONTEXT_JOB = "prepareLocalSpark22Context";

	private LocalSparkWrapper m_wrapper;

	private LocalSparkJobController m_jobController;

	private LocalSparkNamedObjectsController m_namedObjectsController;


	/**
	 * Creates a new local Spark context with the given ID.
	 *
	 * @param contextID
	 *            The ID of the new local Spark context.
	 */
	public LocalSparkContext(final SparkContextID contextID) {
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
            	m_wrapper = null;
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
            m_namedObjectsController = new LocalSparkNamedObjectsController(m_wrapper);
        }
    }

    private void ensureJobController() throws KNIMESparkException {
        if (m_jobController == null) {
            m_jobController = new LocalSparkJobController(m_wrapper);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean canReconfigureWithoutDestroy(final LocalSparkContextConfig newConfig) {
    	final LocalSparkContextConfig config = getConfiguration();
    	
    	return super.canReconfigureWithoutDestroy(newConfig)
    			&& config.getNumberOfThreads() == newConfig.getNumberOfThreads()
    			&& config.enableHiveSupport() == newConfig.enableHiveSupport()
    			&& config.startThriftserver() == newConfig.startThriftserver()
    			&& ((config.enableHiveSupport() && config.useHiveDataFolder())
    				? newConfig.useHiveDataFolder() && newConfig.getHiveDataFolder().equals(config.getHiveDataFolder())
    				: true);
    }

    /**
     * {@inheritDoc}
     */
	@Override
	protected boolean open(final boolean createRemoteContext, final ExecutionMonitor exec)
			throws KNIMESparkException, SparkContextNotFoundException {
		
		if (m_wrapper == null && !createRemoteContext) {
			throw new SparkContextNotFoundException(getID());
		}

		boolean contextWasCreated = false;
		try {
			exec.setProgress(0, "Opening local Spark context");
			final LocalSparkContextConfig config = getConfiguration();
			final Map<String, String> sparkConf = new HashMap<>();

			if (config.useCustomSparkSettings()) {
				sparkConf.putAll(config.getCustomSparkSettings());
			}

			final File[] extraJars = collectExtraJars(config);

			m_wrapper = LocalSparkWrapperFactory.createWrapper(getJobJar().getJarFile(), extraJars);
			m_wrapper.openSparkContext(config.getContextName(),
					config.getNumberOfThreads(),
					sparkConf,
					config.enableHiveSupport(),
					config.startThriftserver(),
					config.getThriftserverPort(),
					config.useHiveDataFolder()
							? config.getHiveDataFolder()
							: null);

			contextWasCreated = true;
			setStatus(SparkContextStatus.OPEN);

			// run prepare context job to initialize type converters
			exec.setProgress(0.9, "Running job to prepare context");
			validateAndPrepareContext();
			exec.setProgress(1);
		} catch (final KNIMESparkException e) {
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

	private static File[] collectExtraJars(final LocalSparkContextConfig config) {
		if (!config.useCustomSparkSettings()) {
			return new File[0];
		}
		final Set<String> extraJars = new HashSet<>();
		final Map<String, String> customSparkSettings = config.getCustomSparkSettings();
		if (customSparkSettings.containsKey(SPARK_JARS)) {
			extraJars.addAll(Arrays.asList(customSparkSettings.get(SPARK_JARS).split(",")));
		}

		if (customSparkSettings.containsKey(SPARK_DRIVER_EXTRA_CLASS_PATH)) {
			extraJars.addAll(Arrays.asList(customSparkSettings.get(SPARK_DRIVER_EXTRA_CLASS_PATH).split(",")));
		}

		if (customSparkSettings.containsKey(SPARK_EXECUTOR_EXTRA_CLASS_PATH)) {
			extraJars.addAll(Arrays.asList(customSparkSettings.get(SPARK_EXECUTOR_EXTRA_CLASS_PATH).split(",")));
		}

		return extraJars.stream()
				.map(File::new)
				.filter((file) -> {
					if (!file.exists()) {
						LOGGER.warn(String.format("Extra jar %s does not exist. Ignoring it.", file.getPath()));
					}
					if (!file.isFile()) {
						LOGGER.warn(String.format("Extra jar %s ist not a file. Ignoring it.", file.getPath()));
					}
					if (!file.canRead()) {
						LOGGER.warn(String.format("Extra jar %s is not readable. Ignoring it.", file.getPath()));
					}

					return file.isFile() && file.canRead();
				})
				.collect(Collectors.toList())
				.toArray(new File[0]);
	}


	private void validateAndPrepareContext() throws KNIMESparkException {
		final SparkVersion sparkVersion = getSparkVersion();

		final PrepareContextJobInput prepInput = new PrepareContextJobInput(getJobJar().getDescriptor().getHash(),
				sparkVersion.toString(),
				getJobJar().getDescriptor().getPluginVersion(),
				IntermediateToSparkConverterRegistry.getConverters(sparkVersion));

		SparkContextUtil.getSimpleRunFactory(getID(), PREPARE_LOCAL_SPARK_CONTEXT_JOB).createRun(prepInput)
				.run(getID());
	}

    /**
     * {@inheritDoc}
     */
	@Override
	protected void destroy() throws KNIMESparkException {
		try {
			if (m_wrapper != null) {
				LOGGER.info("Destroying local Spark context " + getID());
				m_wrapper.destroy();
				m_wrapper = null;
			}
		} catch (final KNIMESparkException e) {
			// do nothing, this is alright
		} finally {
			setStatus(SparkContextStatus.CONFIGURED);
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

        final LocalSparkContextConfig config = getConfiguration();
        final Map<String, String> reps = new HashMap<>();

        reps.put("spark_version", config.getSparkVersion().toString());

        if (getStatus() == SparkContextStatus.OPEN) {
			reps.put("spark_webui_url",
					String.format("<a href=\"%s\" style=\"text-decoration: underline;\">Click here to open</a>",
							m_wrapper.getSparkWebUIUrl()));
        	reps.put("hiveserver_port", config.startThriftserver()
        			? Integer.toString(m_wrapper.getHiveserverPort())
        			: "unavailable");
        } else {
        	reps.put("spark_webui_url", "unavailable");
        	reps.put("hiveserver_port", "unavailable");
        }

        reps.put("context_name", config.getContextName());
        reps.put("delete_data_on_dispose", Boolean.toString(config.deleteObjectsOnDispose()));
        reps.put("override_settings", Boolean.toString(config.useCustomSparkSettings()));
        reps.put("custom_settings", config.useCustomSparkSettings()
            ? config.getCustomSparkSettings()
            		.entrySet()
            		.stream()
            		.map((e) -> String.format("%s: %s\n", e.getKey(), e.getValue()))
            		.collect(Collectors.joining("\n"))
            : "(not applicable)");
        reps.put("context_state", getStatus().toString());
        
        try (InputStream r = getClass().getResourceAsStream("context_html_description.template")) {
            return TextTemplateUtil.fillOutTemplate(r, reps);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read context description template");
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
     * 
     * @return the TCP port that Hiveserver2 (Spark Thriftserver actually) is listening on, or -1 if Hiveserver2 has not
     *         been started.
     */
	public synchronized int getHiveserverPort() {
		if (getStatus() != SparkContextStatus.OPEN) {
			throw new IllegalStateException("Local Spark context is not open.");
		}

		return m_wrapper.getHiveserverPort();
	}
}
