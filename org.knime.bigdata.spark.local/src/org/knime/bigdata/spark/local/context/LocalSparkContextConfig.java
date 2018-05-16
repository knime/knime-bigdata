/**
 * 
 */
package org.knime.bigdata.spark.local.context;

import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.local.LocalSparkVersion;

/**
 * Implementation of {@link SparkContextConfig} interface for local Spark.
 * 
 * @author Oleg Yasnev, KNIME GmbH
 */
public class LocalSparkContextConfig implements SparkContextConfig {
    
	private final SparkVersion m_sparkVersion = LocalSparkVersion.SUPPORTED_SPARK_VERSION;

    private final String m_contextName;

    private final int m_numberOfThreads;

    private final boolean m_deleteObjectsOnDispose;

    private final boolean m_useCustomSparkSettings;

    private final Map<String, String> m_customSparkSettings;
    
    private final boolean m_enableHiveSupport;
    
    private final boolean m_startThriftserver;
    
    private final boolean m_useHiveDataFolder;

    private final String m_hiveDataFolder;

    /**
     * Constructor.
     * 
     * @param contextName Name of the local Spark context.
     * @param numberOfThreads Number of worker threads in Spark.
     * @param deleteObjectsOnDispose Whether to delete named objects on dispose.
     * @param useCustomSparkSettings Whether to inject the given custom Spark settings into SparkConf.
     * @param customSparkSettings Key-value pairs of custom settings to inject into SparkConf.
     * @param enableHiveSupport Whether to provide HiveQL (or just SparkSQL).
     * @param startThriftserver Whether to start Spark Thriftserver or not.
     * @param useHiveDataFolder Whether to store Hive's Metastore and warehouse in the given Hive data folder.
     * @param hiveDataFolder A folder where to store Hive's Metastore and warehouse.
     */
    public LocalSparkContextConfig(
        final String contextName, 
        final int numberOfThreads,
        final boolean deleteObjectsOnDispose, 
        final boolean useCustomSparkSettings, 
        final Map<String, String> customSparkSettings,
        final boolean enableHiveSupport,
        final boolean startThriftserver, 
        final boolean useHiveDataFolder,
        final String hiveDataFolder) {

        if (contextName == null || contextName.isEmpty()) {
            throw new IllegalArgumentException("Context name must not be empty");
        }

        if (numberOfThreads < 1) {
            throw new IllegalArgumentException("Number of threads must be greater than 0");
        }

        if (useCustomSparkSettings && (customSparkSettings == null || customSparkSettings.isEmpty())) {
            throw new IllegalArgumentException("Can't override spark settings with empty settings");
        }
        
		if (useHiveDataFolder && (hiveDataFolder == null || hiveDataFolder.isEmpty())) {
			throw new IllegalArgumentException("Name of persistent Hive folder must not be empty.");
		}

        // Spark
        this.m_contextName = contextName;
        this.m_numberOfThreads = numberOfThreads;
        this.m_deleteObjectsOnDispose = deleteObjectsOnDispose;
        this.m_useCustomSparkSettings = useCustomSparkSettings;
        this.m_customSparkSettings = customSparkSettings;
        
        // Hive
        this.m_enableHiveSupport = enableHiveSupport;
        this.m_startThriftserver = startThriftserver;
        this.m_useHiveDataFolder = useHiveDataFolder;
        this.m_hiveDataFolder = hiveDataFolder;
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
        return new SparkContextID(SparkContextIDScheme.SPARK_LOCAL + "://" + m_contextName);
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
		if (m_useCustomSparkSettings != other.m_useCustomSparkSettings)
			return false;
		if (m_useHiveDataFolder != other.m_useHiveDataFolder)
			return false;
		return true;
	}
}
