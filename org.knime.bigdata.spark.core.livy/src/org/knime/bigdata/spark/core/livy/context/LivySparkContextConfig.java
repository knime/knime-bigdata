package org.knime.bigdata.spark.core.livy.context;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;

/**
 * {@link SparkContextConfig} implementation for a Spark context running on Apache Livy. This class holds all required
 * information to create and configure such a context.
 */
public class LivySparkContextConfig implements SparkContextConfig {

    private final SparkVersion m_sparkVersion;

    private final String m_livyUrl;

    private final AuthenticationType m_authenticationType;
    
    private final String m_stagingAreaFolder;

    private final int m_connectTimeoutSeconds;

    private final int m_responseTimeoutSeconds;

    private final int m_jobCheckFrequencySeconds;

    private final Map<String, String> m_customSparkSettings;

    private final SparkContextID m_sparkContextId;
    
    private final ConnectionInformation m_remoteFsConnectionInfo;

    /**
     * Constructor.
     * 
     * @param sparkVersion
     * @param livyUrl
     * @param authenticationType
     * @param stagingAreaFolder
     * @param connectTimeoutSeconds
     * @param responseTimeoutSeconds
     * @param jobCheckFrequencySeconds
     * @param customSparkSettings
     * @param sparkContextId
     * @param remoteFsConnectionInfo 
     */
    public LivySparkContextConfig(final SparkVersion sparkVersion, final String livyUrl,
        final AuthenticationType authenticationType, final String stagingAreaFolder, final int connectTimeoutSeconds,
        final int responseTimeoutSeconds, final int jobCheckFrequencySeconds,
        final Map<String, String> customSparkSettings, final SparkContextID sparkContextId,
        ConnectionInformation remoteFsConnectionInfo) {

        m_sparkVersion = sparkVersion;
        m_livyUrl = livyUrl;
        m_authenticationType = authenticationType;
        m_stagingAreaFolder = stagingAreaFolder;
        m_connectTimeoutSeconds = connectTimeoutSeconds;
        m_responseTimeoutSeconds = responseTimeoutSeconds;
        m_jobCheckFrequencySeconds = jobCheckFrequencySeconds;
        m_customSparkSettings = customSparkSettings;
        m_sparkContextId = sparkContextId;
        m_remoteFsConnectionInfo = remoteFsConnectionInfo;
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
    public boolean deleteObjectsOnDispose() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useCustomSparkSettings() {
        return true;
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
        return m_sparkContextId;
    }

    /**
     * 
     * @return a {@link ConnectionInformation} object to use for the staging area
     */
    public ConnectionInformation getRemoteFsConnectionInfo() {
        return m_remoteFsConnectionInfo;
    }

    /**
     * @return the http(s) URL for Livy without credentials
     */
    public String getLivyUrlWithoutAuthentication() {
        final URI uri = URI.create(m_livyUrl);
        try {
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()).toString();
        } catch (URISyntaxException e) {
            // should never happen (validated in model)
            throw new RuntimeException("Unable to build Livy URL: " + e, e);
        }
    }

    /**
     * @return the http(s) URL for Livy with credentials
     */
    String getLivyUrlWithAuthentication() {
        return m_livyUrl;
    }

    /**
     * @return how to authenticate against Livy
     */
    protected AuthenticationType getAuthenticationType() {
        return m_authenticationType;
    }
    
    /**
     * 
     * @return the staging area folder to use, or null if none was set.
     */
    public String getStagingAreaFolder() {
        return m_stagingAreaFolder;
    }

    /**
     * @return the TCP socket connect timeout when making connections to Livy.
     */
    protected int getConnectTimeoutSeconds() {
        return m_connectTimeoutSeconds;
    }

    /**
     * 
     * @return a timeout in seconds for HTTP requests to Livy.
     */
    protected int getResponseTimeoutSeconds() {
        return m_responseTimeoutSeconds;
    }

    /**
     * 
     * @return how often in seconds .to poll the status of a Spark job running on Livy.
     */
    protected int getJobCheckFrequencySeconds() {
        return m_jobCheckFrequencySeconds;
    }

    /**
     * Autogenerated {@link #hashCode()} implementation over all members.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((m_authenticationType == null) ? 0 : m_authenticationType.hashCode());
        result = prime * result + ((m_stagingAreaFolder == null) ? 0 : m_stagingAreaFolder.hashCode());
        result = prime * result + m_connectTimeoutSeconds;
        result = prime * result + ((m_customSparkSettings == null) ? 0 : m_customSparkSettings.hashCode());
        result = prime * result + m_jobCheckFrequencySeconds;
        result = prime * result + ((m_livyUrl == null) ? 0 : m_livyUrl.hashCode());
        result = prime * result + m_responseTimeoutSeconds;
        result = prime * result + ((m_sparkContextId == null) ? 0 : m_sparkContextId.hashCode());
        result = prime * result + ((m_sparkVersion == null) ? 0 : m_sparkVersion.hashCode());
        return result;
    }

    /**
     * Autogenerated {@link #equals(Object)} implementation over all members.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        LivySparkContextConfig other = (LivySparkContextConfig)obj;
        if (m_authenticationType != other.m_authenticationType)
            return false;
        if (m_stagingAreaFolder == null) {
            if (other.m_stagingAreaFolder != null)
                return false;
        } else if (!m_stagingAreaFolder.equals(other.m_stagingAreaFolder))
            return false;
        if (m_connectTimeoutSeconds != other.m_connectTimeoutSeconds)
            return false;
        if (m_customSparkSettings == null) {
            if (other.m_customSparkSettings != null)
                return false;
        } else if (!m_customSparkSettings.equals(other.m_customSparkSettings))
            return false;
        if (m_jobCheckFrequencySeconds != other.m_jobCheckFrequencySeconds)
            return false;
        if (m_livyUrl == null) {
            if (other.m_livyUrl != null)
                return false;
        } else if (!m_livyUrl.equals(other.m_livyUrl))
            return false;
        if (m_responseTimeoutSeconds != other.m_responseTimeoutSeconds)
            return false;
        if (m_sparkContextId == null) {
            if (other.m_sparkContextId != null)
                return false;
        } else if (!m_sparkContextId.equals(other.m_sparkContextId))
            return false;
        if (m_sparkVersion == null) {
            if (other.m_sparkVersion != null)
                return false;
        } else if (!m_sparkVersion.equals(other.m_sparkVersion))
            return false;
        return true;
    }

    @Override
    public String getContextName() {
        final String uniqueId = m_sparkContextId.asURI().getHost();
        return "KNIME Spark Context " + uniqueId;
    }
}
