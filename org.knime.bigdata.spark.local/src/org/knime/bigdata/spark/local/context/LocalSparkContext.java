/**
 *
 */
package org.knime.bigdata.spark.local.context;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectsController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.exception.SparkContextNotFoundException;
import org.knime.bigdata.spark.core.types.converter.spark.IntermediateToSparkConverterRegistry;
import org.knime.bigdata.spark.core.util.PrepareContextJobInput;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapper;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapperFactory;
import org.knime.core.node.NodeLogger;

/**
 * Implementation of {@link SparkContext} for local Spark. This class uses the
 * {@link LocalSparkWrapper} interface to actually create a local Spark context
 * and run jobs in it.
 * 
 * @author Oleg Yasnev, KNIME GmbH
 */
public class LocalSparkContext extends SparkContext<LocalSparkContextConfig> {

	private final static NodeLogger LOGGER = NodeLogger.getLogger(LocalSparkContext.class);

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
	public LocalSparkContext(SparkContextID contextID) {
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
	protected boolean open(boolean createRemoteContext) throws KNIMESparkException, SparkContextNotFoundException {
		if (m_wrapper == null && !createRemoteContext) {
			throw new SparkContextNotFoundException(getID());
		}

		boolean contextWasCreated = false;
		try {
			final LocalSparkContextConfig config = getConfiguration();
			final Map<String, String> sparkConf = new HashMap<>();

			if (config.useCustomSparkSettings()) {
				sparkConf.putAll(config.getCustomSparkSettings());
			}
			
			m_wrapper = LocalSparkWrapperFactory.createWrapper(getJobJar().getJarFile());
			m_wrapper.openSparkContext(config.getContextName(),
					config.getNumberOfThreads(),
					sparkConf,
					config.enableHiveSupport(),
					config.startThriftserver(),
					(config.useHiveDataFolder())
							? config.getHiveDataFolder()
							: null);
			
			contextWasCreated = true;
			setStatus(SparkContextStatus.OPEN);
			
			// run prepare context job to initialize type converters
			validateAndPrepareContext();
		} catch (KNIMESparkException e) {
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
		SparkVersion sparkVersion = getSparkVersion();

		PrepareContextJobInput prepInput = PrepareContextJobInput.create(getJobJar().getDescriptor().getHash(),
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
		} catch (KNIMESparkException e) {
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
        	reps.put("spark_webui_url", String.format("<a href=\"%s\">Click here to open</a>", m_wrapper.getSparkWebUIUrl()));
        	reps.put("hiveserver_port", (config.startThriftserver())
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

        return generateHTMLDescription("context_html_description.template", reps);
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
	
	public synchronized int getHiveserverPort() {
		if (getStatus() != SparkContextStatus.OPEN) {
			throw new IllegalStateException("Local Spark context is not open.");
		}
		
		return m_wrapper.getHiveserverPort();
	}
}
