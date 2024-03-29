/**
 * 
 */
package org.knime.bigdata.spark.local.context;

import java.time.ZoneId;
import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterParameter;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.local.LocalSparkVersion;
import org.knime.bigdata.spark.node.util.context.create.time.TimeSettings.TimeShiftStrategy;

/**
 * Implementation of {@link SparkContextConfig} interface for local Spark.
 * 
 * @author Oleg Yasnev, KNIME GmbH
 */
public class LocalSparkContextConfig implements SparkContextConfig {
    
    private final SparkVersion m_sparkVersion = LocalSparkVersion.SUPPORTED_SPARK_VERSION;

    private final SparkContextID m_contextID;

    private final String m_contextName;

    private final int m_numberOfThreads;

    private final boolean m_deleteObjectsOnDispose;

    private final boolean m_useCustomSparkSettings;

    private final Map<String, String> m_customSparkSettings;

    private final boolean m_enableHiveSupport;

    private final boolean m_startThriftserver;
    
    private final int m_thriftserverPort;

    private final boolean m_useHiveDataFolder;

    private final String m_hiveDataFolder;

    private final TimeShiftStrategy m_timeShiftStrategy;

    private ZoneId m_timeShiftZoneId;

    private boolean m_failOnDifferentClusterTimeZone;

    /**
     * Constructor.
     * 
     * @param contextID The ID of the Spark context.
     * @param contextName Name of the local Spark context.
     * @param numberOfThreads Number of worker threads in Spark.
     * @param deleteObjectsOnDispose Whether to delete named objects on dispose.
     * @param useCustomSparkSettings Whether to inject the given custom Spark settings into SparkConf.
     * @param customSparkSettings Key-value pairs of custom settings to inject into SparkConf.
     * @param timeShiftStrategy Time shift strategy to use in converter between KNIME and Spark
     * @param timeShiftZoneId optional time shift zone ID, might by {@code null}
     * @param failOnDifferentClusterTimeZone if context creation should fail on different time zone
     * @param enableHiveSupport Whether to provide HiveQL (or just SparkSQL).
     * @param startThriftserver Whether to start Spark Thriftserver or not.
     * @param thriftserverPort The TCP port on which Spark Thriftserver shall listen for JDBC connections. May be -1,
     *            which results in the port being chosen randomly if necessary.
     * @param useHiveDataFolder Whether to store Hive's Metastore and warehouse in the given Hive data folder.
     * @param hiveDataFolder A folder where to store Hive's Metastore and warehouse.
     */
    public LocalSparkContextConfig(
        final SparkContextID contextID,
        final String contextName, 
        final int numberOfThreads,
        final boolean deleteObjectsOnDispose, 
        final boolean useCustomSparkSettings, 
        final Map<String, String> customSparkSettings,
        final TimeShiftStrategy timeShiftStrategy,
        final ZoneId timeShiftZoneId,
        boolean failOnDifferentClusterTimeZone,
        final boolean enableHiveSupport,
        final boolean startThriftserver, 
        final int thriftserverPort,
        final boolean useHiveDataFolder,
        final String hiveDataFolder) {

        if (contextID == null) {
            throw new IllegalArgumentException("Spark context ID must not be null");
        }

        if (contextName == null || contextName.isEmpty()) {
            throw new IllegalArgumentException("Context name must not be empty");
        }

        if (numberOfThreads < 1) {
            throw new IllegalArgumentException("Number of threads must be greater than 0");
        }

        if (useCustomSparkSettings && (customSparkSettings == null || customSparkSettings.isEmpty())) {
            throw new IllegalArgumentException("Can't override spark settings with empty settings");
        }

        if (timeShiftStrategy == null) {
            throw new IllegalArgumentException("Time shift strategy requiered.");
        }

        if (timeShiftStrategy == TimeShiftStrategy.FIXED && timeShiftZoneId == null) {
            throw new IllegalArgumentException("Time zone required with fixed time shift strategy.");
        }

        if (useHiveDataFolder && (hiveDataFolder == null || hiveDataFolder.isEmpty())) {
            throw new IllegalArgumentException("Name of persistent Hive folder must not be empty.");
        }

        // Spark
        m_contextID = contextID;
        m_contextName = contextName;
        m_numberOfThreads = numberOfThreads;
        m_deleteObjectsOnDispose = deleteObjectsOnDispose;
        m_useCustomSparkSettings = useCustomSparkSettings;
        m_customSparkSettings = customSparkSettings;
        m_timeShiftStrategy = timeShiftStrategy;
        m_timeShiftZoneId = timeShiftZoneId;
        m_failOnDifferentClusterTimeZone = failOnDifferentClusterTimeZone;

        // Hive
        m_enableHiveSupport = enableHiveSupport;
        m_startThriftserver = startThriftserver;
        m_thriftserverPort = thriftserverPort;
        m_useHiveDataFolder = useHiveDataFolder;
        m_hiveDataFolder = hiveDataFolder;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SparkVersion getSparkVersion() {
        return m_sparkVersion;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getContextName() {
        return m_contextName;
    }

    /**
     * @return the numberOfThreads
     */
    public int getNumberOfThreads() {
        return m_numberOfThreads;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean deleteObjectsOnDispose() {
        return m_deleteObjectsOnDispose;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useCustomSparkSettings() {
        return m_useCustomSparkSettings;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getCustomSparkSettings() {
        return m_customSparkSettings;
    }

    /**
     * Add context specific settings like custom spark settings and time shift settings to given spark settings.
     *
     * @param sparkConf Spark settings to add context settings
     */
    public void addSparkSettings(final Map<String, String> sparkConf) {
        if (m_timeShiftStrategy == TimeShiftStrategy.FIXED) {
            sparkConf.put("spark.sql.session.timeZone", m_timeShiftZoneId.getId());
        }

        if (m_useCustomSparkSettings) {
            sparkConf.putAll(m_customSparkSettings);
        }
    }

    /**
     * @return the time shift strategy to use
     */
    protected TimeShiftStrategy getTimeShiftStrategy() {
        return m_timeShiftStrategy;
    }


    /**
     * @return Time shift ZoneId in {@link TimeShiftStrategy#FIXED} or  {@link TimeShiftStrategy#DEFAULT_CLIENT} mode.
     * @throws KNIMESparkException on other time shift strategy
     */
    protected ZoneId getTimeShiftZone() throws KNIMESparkException{
        if (m_timeShiftStrategy == TimeShiftStrategy.FIXED) {
            return m_timeShiftZoneId;
        } else {
            throw new KNIMESparkException(
                "Unsupported time zone parameter on " + m_timeShiftStrategy + " time shift strategy.");
        }
    }

    /**
     * Creates the context specific converter parameters.
     *
     * @return converter parameters
     * @throws KNIMESparkException on unknown time shift strategy
     */
    protected KNIMEToIntermediateConverterParameter getConverterParameter() throws KNIMESparkException {
        switch (m_timeShiftStrategy) {
            case NONE:
                return KNIMEToIntermediateConverterParameter.DEFAULT;
            case FIXED:
                return new KNIMEToIntermediateConverterParameter(m_timeShiftZoneId);
            case DEFAULT_CLUSTER:
                return new KNIMEToIntermediateConverterParameter(ZoneId.systemDefault());
            default:
                throw new KNIMESparkException("Unsuported time shift settings.");
        }
    }

    /**
     * @return {@code true} if context creation should fail on different cluster time zone
     */
    public boolean failOnDifferentClusterTimeZone() {
        return m_failOnDifferentClusterTimeZone;
    }

	/**
	 * @return true, if HiveQL support should be enabled in Spark, false
	 *         otherwise.
	 */
	public boolean enableHiveSupport() {
		return m_enableHiveSupport;
	}

	/**
	 * 
	 * @return true, if the Spark Thriftserver for JDBC connectivity should be
	 *         started in Spark, false otherwise.
	 */
	public boolean startThriftserver() {
		return m_startThriftserver;
	}
	
	
    /**
     * 
     * @return The TCP port on which Spark Thriftserver shall listen for JDBC connections. May return -1, if no port was
     *         specified.
     */
	public int getThriftserverPort() {
        return m_thriftserverPort;
    }

    /**
	 * 
	 * @return true, if the Hive metastore DB and warehouse should be persisted
	 *         in a non-ephemeral folder, false otherwise.
	 */
	public boolean useHiveDataFolder() {
		return m_useHiveDataFolder;
	}
	
	/**
	 * 
	 * @return a folder where the Hive metastore DB and warehouse should be persisted.
	 */
	public String getHiveDataFolder() {
		return m_hiveDataFolder;
	}
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getSparkContextID() {
        return m_contextID;
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_contextName == null) ? 0 : m_contextName.hashCode());
		result = prime * result + ((m_customSparkSettings == null) ? 0 : m_customSparkSettings.hashCode());
		result = prime * result + (m_deleteObjectsOnDispose ? 1231 : 1237);
		result = prime * result + (m_enableHiveSupport ? 1231 : 1237);
		result = prime * result + ((m_hiveDataFolder == null) ? 0 : m_hiveDataFolder.hashCode());
		result = prime * result + m_numberOfThreads;
		result = prime * result + ((m_sparkVersion == null) ? 0 : m_sparkVersion.hashCode());
		result = prime * result + (m_startThriftserver ? 1231 : 1237);
        result = prime * result + m_thriftserverPort;
        result = prime * result + ((m_timeShiftStrategy == null) ? 0 : m_timeShiftStrategy.hashCode());
        result = prime * result + ((m_timeShiftZoneId == null) ? 0 : m_timeShiftZoneId.hashCode());
        result = prime * result + (m_failOnDifferentClusterTimeZone ? 1231 : 1237);
		result = prime * result + (m_useCustomSparkSettings ? 1231 : 1237);
		result = prime * result + (m_useHiveDataFolder ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		LocalSparkContextConfig other = (LocalSparkContextConfig) obj;
		if (m_contextName == null) {
			if (other.m_contextName != null)
				return false;
		} else if (!m_contextName.equals(other.m_contextName))
			return false;
		if (m_customSparkSettings == null) {
			if (other.m_customSparkSettings != null)
				return false;
		} else if (!m_customSparkSettings.equals(other.m_customSparkSettings))
			return false;
		if (m_deleteObjectsOnDispose != other.m_deleteObjectsOnDispose)
			return false;
		if (m_enableHiveSupport != other.m_enableHiveSupport)
			return false;
		if (m_hiveDataFolder == null) {
			if (other.m_hiveDataFolder != null)
				return false;
		} else if (!m_hiveDataFolder.equals(other.m_hiveDataFolder))
			return false;
		if (m_numberOfThreads != other.m_numberOfThreads)
			return false;
		if (m_sparkVersion == null) {
			if (other.m_sparkVersion != null)
				return false;
		} else if (!m_sparkVersion.equals(other.m_sparkVersion))
			return false;
		if (m_startThriftserver != other.m_startThriftserver)
			return false;
        if (m_thriftserverPort != other.m_thriftserverPort)
            return false;
		if (m_useCustomSparkSettings != other.m_useCustomSparkSettings)
			return false;
		if (m_failOnDifferentClusterTimeZone != other.m_failOnDifferentClusterTimeZone)
		    return false;
        if (m_timeShiftStrategy != other.m_timeShiftStrategy) {
            return false;
        }
        if (m_timeShiftZoneId == null) {
            if (other.m_timeShiftZoneId != null) {
                return false;
            }
        } else if (!m_timeShiftZoneId.equals(other.m_timeShiftZoneId)) {
            return false;
        }
		if (m_useHiveDataFolder != other.m_useHiveDataFolder)
			return false;
		return true;
	}
}
