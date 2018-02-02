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
 *   Created on 26.06.2015 by koetter
 */
package org.knime.bigdata.spark.core.port.context;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;

/**
 * Class that holds all information about a SparkContext that is used in KNIME e.g. the id
 * of the context and the requested resources.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class JobServerSparkContextConfig implements Serializable, SparkContextConfig {

    private static final long serialVersionUID = 1L;

    private final String m_jobServerUrl;
    private final boolean m_authentication;
    private final String m_user;
    private final String m_password;
    private final Duration m_receiveTimeout;
    private final int m_jobCheckFrequency;

    private final SparkVersion m_sparkVersion;
    private final String m_contextName;
    private final boolean m_deleteObjectsOnDispose;
    private final String m_sparkJobLogLevel;
    private final boolean m_overrideSparkSettings;
    private final Map<String, String> m_customSparkSettings;


    /**
     * create spark context container with default values
     */
    public JobServerSparkContextConfig() {
        this(KNIMEConfigContainer.getJobServerUrl(),
            KNIMEConfigContainer.useAuthentication(), KNIMEConfigContainer.getUserName(), KNIMEConfigContainer.getPassword(),
            KNIMEConfigContainer.getReceiveTimeout(), KNIMEConfigContainer.getJobCheckFrequency(),
            KNIMEConfigContainer.getSparkVersion(), KNIMEConfigContainer.getSparkContext(), KNIMEConfigContainer.deleteSparkObjectsOnDispose(),
            KNIMEConfigContainer.getSparkJobLogLevel(), KNIMEConfigContainer.overrideSparkSettings(), KNIMEConfigContainer.getCustomSparkSettings());
    }


    /**
     * @param jobServerUrl Spark job server url
     * @param authentication <code>true</code> for authentication
     * @param user login username
     * @param password login password
     * @param receiveTimeout Spark job server REST receive timeout
     * @param jobCheckFrequency job check frequency
     * @param sparkVersion Spark version
     * @param contextName context name
     * @param deleteObjectsOnDispose <code>true</code> if objects should be deleted on dispose
     * @param sparkJobLogLevel the log level for Spark jobs
     * @param overrideSparkSettings <code>true</code> for custom Spark settings
     * @param customSparkSettings custom Spark settings
     */
    public JobServerSparkContextConfig(final String jobServerUrl,
        final boolean authentication, final String user, final String password,
        final Duration receiveTimeout, final int jobCheckFrequency,
        final SparkVersion sparkVersion, final String contextName, final boolean deleteObjectsOnDispose,
        final String sparkJobLogLevel, final boolean overrideSparkSettings, final Map<String,String> customSparkSettings) {

        if (jobServerUrl == null || jobServerUrl.isEmpty()) {
            throw new IllegalArgumentException("url must not be empty");
        }

        if (authentication && (user == null || user.isEmpty())) {
            throw new IllegalArgumentException("can't use authentication with empty user");
        }

        if (receiveTimeout.toMillis() < 0) {
            throw new IllegalArgumentException("Receive timeout must be positive");
        }

        if (jobCheckFrequency < 0) {
            throw new IllegalArgumentException("Spark job check frequency must be positive");
        }

        if (sparkVersion == null) {
            throw new IllegalArgumentException("Spark version must not be null");
        }

        if (contextName == null || contextName.isEmpty()) {
            throw new IllegalArgumentException("contextName must not be empty");
        }

        if (sparkJobLogLevel == null || sparkJobLogLevel.isEmpty()) {
            throw new IllegalArgumentException("No spark job log level provided");
        }

        if (overrideSparkSettings && (customSparkSettings == null || customSparkSettings.isEmpty())) {
            throw new IllegalArgumentException("Can't override spark settings with empty settings");
        }

        this.m_jobServerUrl = jobServerUrl;
        this.m_authentication = authentication;
        this.m_user = user;
        this.m_password = password;
        this.m_receiveTimeout = receiveTimeout;
        this.m_jobCheckFrequency = jobCheckFrequency;

        this.m_sparkVersion = sparkVersion;
        this.m_contextName = contextName;
        this.m_deleteObjectsOnDispose = deleteObjectsOnDispose;
        this.m_sparkJobLogLevel = sparkJobLogLevel;
        this.m_overrideSparkSettings = overrideSparkSettings;
        this.m_customSparkSettings = customSparkSettings;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        final JobServerSparkContextConfig other = (JobServerSparkContextConfig)obj;
        if (!m_jobServerUrl.equals(other.m_jobServerUrl)) {
            return false;
        }
        if (m_authentication != other.m_authentication) {
            return false;
        }
        if (m_authentication && !m_user.equals(other.m_user)) {
            return false;
        }
        if (m_authentication && !Objects.equals(m_password, other.m_password)) {
            return false;
        }
        if (!m_receiveTimeout.equals(other.m_receiveTimeout)) {
            return false;
        }
        if (m_jobCheckFrequency != other.m_jobCheckFrequency) {
            return false;
        }

        if (!m_sparkVersion.equals(other.m_sparkVersion)) {
            return false;
        }
        if (!m_contextName.equals(other.m_contextName)) {
            return false;
        }
        if (m_deleteObjectsOnDispose != other.m_deleteObjectsOnDispose) {
            return false;
        }
        if (!m_sparkJobLogLevel.equals(other.m_sparkJobLogLevel)) {
            return false;
        }
        if (m_overrideSparkSettings != other.m_overrideSparkSettings) {
            return false;
        }
        if (m_overrideSparkSettings && !m_customSparkSettings.equals(other.m_customSparkSettings)) {
            return false;
        }

        return true;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;

        result = prime * result + m_jobServerUrl.hashCode();
        result = prime * result + (m_authentication ? 1 : 0);
        result = prime * result + (m_authentication ? m_user.hashCode() : 0);
        result = prime * result + (m_authentication ? ((m_password != null) ? m_password.hashCode() : 0) : 0);
        result = prime * result + m_receiveTimeout.hashCode();
        result = prime * result + m_jobCheckFrequency;

        result = prime * result + m_sparkVersion.hashCode();
        result = prime * result + m_contextName.hashCode();
        result = prime * result + (m_deleteObjectsOnDispose ? 1231 : 1237);
        result = prime * result + m_sparkJobLogLevel.hashCode();
        result = prime * result + (m_overrideSparkSettings ? 1 : 0);
        result = prime * result + (m_overrideSparkSettings ? m_customSparkSettings.hashCode() : 0);

        return result;
    }

    /**
     * @return Spark job server url
     */
    public String getJobServerUrl() {
        return m_jobServerUrl;
    }


    /**
     * @return <code>true</code> for authentication
     */
    public boolean useAuthentication() {
        return m_authentication;
    }


    /**
     * @return the login user
     */
    public String getUser() {
        return m_user;
    }


    /**
     * @return login password
     */
    public String getPassword() {
        return m_password;
    }


    /**
     * @return job server REST receive timeout in seconds
     */
    public Duration getReceiveTimeout() {
        return m_receiveTimeout;
    }


    /**
     * @return frequency to check job status in seconds
     */
    public int getJobCheckFrequency() {
        return m_jobCheckFrequency;
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
     * @return the Spark job log level
     */
    public String getSparkJobLogLevel() {
        return m_sparkJobLogLevel;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean useCustomSparkSettings() {
        return m_overrideSparkSettings;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getCustomSparkSettings() {
        return m_customSparkSettings;
    }

    private static final String LEGACY_CFG_HOST = "host";

    private static final String LEGACY_CFG_PORT = "port";

    private static final String LEGACY_CFG_PROTOCOL = "protocol";

    private static final String LEGACY_CFG_CONTEXTNAME = "id";

    /**
     * @param conf legacy {@link ConfigRO}
     * @return the {@link SparkContextID}
     * @throws InvalidSettingsException if the config does not contain the Spark ID
     */
    public static SparkContextID createSparkContextIDFromLegacyConfig(final ConfigRO conf)
        throws InvalidSettingsException {

        return SparkContextID.fromConnectionDetails(String.format("%s://%s:%d", conf.getString(LEGACY_CFG_PROTOCOL),
            conf.getString(LEGACY_CFG_HOST), conf.getInt(LEGACY_CFG_PORT)), conf.getString(LEGACY_CFG_CONTEXTNAME));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public SparkContextID getSparkContextID() {
        return SparkContextID.fromConnectionDetails(m_jobServerUrl, m_contextName);
    }
}