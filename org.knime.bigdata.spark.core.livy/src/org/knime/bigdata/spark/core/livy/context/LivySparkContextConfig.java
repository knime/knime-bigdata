package org.knime.bigdata.spark.core.livy.context;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Configuration class for a Spark context running on Apache Livy. This class
 * holds all required information to create and configure such a context.
 */
public class LivySparkContextConfig implements SparkContextConfig, Serializable {

	private static final long serialVersionUID = 421835560130535315L;

	private final SparkVersion m_sparkVersion = SparkVersion.V_2_2;
	
	private final SparkContextID m_contextId;

	private final String m_livyUrl;
	
	private final String m_contextName;

	private final boolean m_deleteObjectsOnDispose;

	private final boolean m_useCustomSparkSettings;

	private final Map<String, String> m_customSparkSettings;

	public LivySparkContextConfig(final String livyUrl, final String contextName,
			final boolean deleteObjectsOnDispose, final boolean useCustomSparkSettings,
			final Map<String, String> customSparkSettings) {
		
		if (livyUrl == null || livyUrl.isEmpty()) {
			throw new IllegalArgumentException("Livy URL name must not be empty");
		}

		if (contextName == null || contextName.isEmpty()) {
			throw new IllegalArgumentException("Context name must not be empty");
		}

		if (useCustomSparkSettings && (customSparkSettings == null || customSparkSettings.isEmpty())) {
			throw new IllegalArgumentException("Can't override spark settings with empty settings");
		}

		m_contextId = createSparkContextID(livyUrl, contextName);
		m_livyUrl = livyUrl;
		m_contextName = contextName;
		m_deleteObjectsOnDispose = deleteObjectsOnDispose;
		m_useCustomSparkSettings = useCustomSparkSettings;
		m_customSparkSettings = customSparkSettings;
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
	 * {@inheritDoc}
	 */
	@Override
	public SparkContextID getSparkContextID() {
		return m_contextId;
	}

	public String getLivyUrl() {
		return m_livyUrl;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((m_contextName == null) ? 0 : m_contextName.hashCode());
		result = prime * result + ((m_customSparkSettings == null) ? 0 : m_customSparkSettings.hashCode());
		result = prime * result + (m_deleteObjectsOnDispose ? 1231 : 1237);
		result = prime * result + ((m_livyUrl == null) ? 0 : m_livyUrl.hashCode());
		result = prime * result + ((m_sparkVersion == null) ? 0 : m_sparkVersion.hashCode());
		result = prime * result + (m_useCustomSparkSettings ? 1231 : 1237);
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
		LivySparkContextConfig other = (LivySparkContextConfig) obj;
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
		if (m_livyUrl == null) {
			if (other.m_livyUrl != null)
				return false;
		} else if (!m_livyUrl.equals(other.m_livyUrl))
			return false;
		if (m_sparkVersion == null) {
			if (other.m_sparkVersion != null)
				return false;
		} else if (!m_sparkVersion.equals(other.m_sparkVersion))
			return false;
		if (m_useCustomSparkSettings != other.m_useCustomSparkSettings)
			return false;
		return true;
	}
	
	/**
	 * Utility function to generate a {@link SparkContextID} from Livy
	 * connection settings. This should act as the single source of truth when
	 * generating IDs for Livy Spark contexts.
	 * 
	 * @param livyUrlString
	 *            Livy connection URL (e.g. http://host:port)
	 * @param livyContextName
	 *            A user-defined context name.
	 * @return a {@link SparkContextID} identifying the Spark Context on the
	 *         given Livy server.
	 */
	public static SparkContextID createSparkContextID(final String livyUrlString, final String livyContextName) {
    	URL livyUrl;
		try {
			livyUrl = new URL(livyUrlString);
		} catch (MalformedURLException e) {
			// should never happen, because the URL was verified.
			throw new RuntimeException(e);
		}
    	
    	// at this point we rely on previous validation to ensure that the user-specified
    	// Livy URL has a proper host and port specified (path is optional).  
    	final String host = livyUrl.getHost();
    	final int port  = livyUrl.getPort();
    	final String path = livyUrl.getPath();
    	
    	final String id = String.format("%s://%s:%d/%s#%s",
    			SparkContextIDScheme.SPARK_LIVY,
    			host,
    			port,
    			path,
    			livyContextName);
    	
        return new SparkContextID(id);
	}
}
