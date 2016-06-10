/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 03.07.2015 by koetter
 *   Changes on 07.06.2016 by Sascha Wolke:
 *     - fields added: jobManagerUrl, authentication, sparkJobLogLevel, overrideSparkSettings, customSparkSettings
 *     - protocol+host+port migrated into jobManagerUrl
 *     - authentication flag added
 *     - deleteRDDsOnDispose renamed to deleteObjectsOnDispose
 *     - memPerNode migrated into overrideSparkSettings+customSparkSettings
 */
package com.knime.bigdata.spark.node.util.context.create;

import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;
import com.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Settings model that transfers Spark context information between the node model and its dialog.
 *
 * FIXME: We have to migrate old data (07.06.2016):
 *   - protocol+host+port -> jobManagerUrl
 *   - user+pass -> authentication+user+pass
 *   - deleteRDDsOnDispose -> deleteObjectsOnDispose
 *   - memPerNode -> overrideSparkSettings+customSparkSettings
 *
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class ContextSettings {

    private final SettingsModelString m_jobManagerUrl =
            new SettingsModelString("jobManagerUrl", KNIMEConfigContainer.getJobServerUrl());

    // Job manager URL fall back fields
    @Deprecated private final SettingsModelString oldProtocol = new SettingsModelString("protocol", "http");
    @Deprecated private final SettingsModelString oldHost = new SettingsModelString("host", "localhost");
    @Deprecated private final SettingsModelInteger oldPort = new SettingsModelInteger("port", 8090);

    private final SettingsModelBoolean m_authentication =
            new SettingsModelBoolean("authentication", KNIMEConfigContainer.useAuthentication());

    private final SettingsModelString m_user =
            new SettingsModelString("user", KNIMEConfigContainer.getUserName());

    private final SettingsModelString m_password =
            new SettingsModelString("password", KNIMEConfigContainer.getPassword() == null ? null : String.valueOf(KNIMEConfigContainer.getUserName()));

    private final SettingsModelInteger m_jobTimeout =
            new SettingsModelIntegerBounded("sparkJobTimeout", KNIMEConfigContainer.getJobTimeout(), 1, Integer.MAX_VALUE);

    private final SettingsModelInteger m_jobCheckFrequency =
            new SettingsModelIntegerBounded("sparkJobCheckFrequency",
                KNIMEConfigContainer.getJobCheckFrequency(), 1, Integer.MAX_VALUE);

    private final SettingsModelString m_sparkVersion =
            new SettingsModelString("sparkVersion", KNIMEConfigContainer.getSparkVersion().getLabel());

    private final SettingsModelString m_contextName =
            new SettingsModelString("contextName", KNIMEConfigContainer.getSparkContext());

    private final SettingsModelBoolean m_deleteObjectsOnDispose =
            new SettingsModelBoolean("deleteObjectsOnDispose", KNIMEConfigContainer.deleteSparkObjectsOnDispose());

    /** @deprecated renamed into deleteObjectsOnDispose */
    @Deprecated
    private final SettingsModelBoolean m_LegacyDeleteRDDsOnDispose = new SettingsModelBoolean("deleteRDDsOnDispose", false);

    private final SettingsModelString m_sparkJobLogLevel =
            new SettingsModelString("sparkJobLogLevel", KNIMEConfigContainer.getSparkJobLogLevel());

    private final SettingsModelBoolean m_overrideSparkSettings =
            new SettingsModelBoolean("overrideSparkSettings", KNIMEConfigContainer.overrideSparkSettings());

    private final SettingsModelString m_customSparkSettings =
            new SettingsModelString("customSparkSettings", KNIMEConfigContainer.getCustomSparkSettings());

    /** @deprecated merged into customSparkSettings */
    @Deprecated
    private final SettingsModelString m_LegacyMemPerNode = new SettingsModelString("memPerNode", null);

    private static final char[] MY =
            "3O}accA80479[7b@05b9378K}18358QLG32P�92JZW76b76@2eb9$a\\23-c0a397a%ee'e35!89afFfA64#8bB8GRl".toCharArray();

    protected SettingsModelString getJobManagerUrlModel() {
        return m_jobManagerUrl;
    }

    protected SettingsModelBoolean getAuthenticateModel() {
        return m_authentication;
    }

    protected SettingsModelString getUserModel() {
        return m_user;
    }

    protected SettingsModelString getPasswordModel() {
        return m_password;
    }

    protected SettingsModelInteger getJobTimeoutModel() {
        return m_jobTimeout;
    }

    protected SettingsModelInteger getJobCheckFrequencyModel() {
        return m_jobCheckFrequency;
    }

    protected SettingsModelString getSparkVersionModel() {
        return m_sparkVersion;
    }

    protected SettingsModelString getContextNameModel() {
        return m_contextName;
    }

    protected SettingsModelBoolean getDeleteObjectsOnDisposeModel() {
        return m_deleteObjectsOnDispose;
    }

    protected SettingsModelString getSparkJobLogLevelModel() {
        return m_sparkJobLogLevel;
    }

    protected SettingsModelBoolean getOverrideSparkSettingsModel() {
        return m_overrideSparkSettings;
    }

    protected SettingsModelString getCustomSparkSettingsModel() {
        return m_customSparkSettings;
    }



    public String getJobManagerUrl() {
        return m_jobManagerUrl.getStringValue();
    }

    public boolean useAuthentication() {
        return m_authentication.getBooleanValue();
    }

    public String getUser() {
        return m_user.getStringValue();
    }

    public String getPassword() {
        return m_password.getStringValue();
    }

    public int getJobTimeout() {
        return m_jobTimeout.getIntValue();
    }

    public int getJobCheckFrequency() {
        return m_jobCheckFrequency.getIntValue();
    }

    public SparkVersion getSparkVersion() {
        return SparkVersion.getVersion(m_sparkVersion.getStringValue());
    }

    public String getContextName() {
        return m_contextName.getStringValue();
    }

    public boolean deleteObjectsOnDispose() {
        return m_deleteObjectsOnDispose.getBooleanValue();
    }

    public String getSparkJobLogLevel() {
        return m_sparkJobLogLevel.getStringValue();
    }

    public boolean overrideSparkSettings() {
        return m_overrideSparkSettings.getBooleanValue();
    }

    public String getCustomSparkSettings() {
        return m_customSparkSettings.getStringValue();
    }


    /**
     * @return the {@link SparkContextID} derived from the configuration settings
     */
    public SparkContextID getSparkContextID() {
        return SparkContextID.fromConnectionDetails(getJobManagerUrl(), getContextName());
    }

    /**
     * @param m_password
     * @return
     */
    private static String demix(final String p) {
        if (p == null) {
            return null;
        }
        final char[] cs = p.toCharArray();
        ArrayUtils.reverse(cs, 0, cs.length);
        return  String.valueOf(cs);
    }

    /**
     * @param m_password
     * @return
     */
    private static String mix(final String p) {
        if (p == null) {
            return null;
        }
        final char[] c = p.toCharArray();
        final char[] cs = Arrays.copyOf(c, c.length);
        ArrayUtils.reverse(cs, 0, cs.length);
        return String.valueOf(cs);
    }

    /**
     * @param settings the NodeSettingsWO to write to
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_jobManagerUrl.saveSettingsTo(settings);
        m_authentication.saveSettingsTo(settings);
        m_user.saveSettingsTo(settings);
        settings.addPassword(m_password.getKey(), String.valueOf(MY), mix(m_password.getStringValue()));
        m_jobTimeout.saveSettingsTo(settings);
        m_jobCheckFrequency.saveSettingsTo(settings);
        m_sparkVersion.saveSettingsTo(settings);
        m_contextName.saveSettingsTo(settings);
        m_deleteObjectsOnDispose.saveSettingsTo(settings);
        m_sparkJobLogLevel.saveSettingsTo(settings);
        m_overrideSparkSettings.saveSettingsTo(settings);
        m_customSparkSettings.saveSettingsTo(settings);
    }

    /**
     * @param settings the NodeSettingsRO to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // try to find jobManagerUrl or fall back to protocol+host+port
        try {
            m_jobManagerUrl.validateSettings(settings);
        } catch(InvalidSettingsException e) {
            oldProtocol.validateSettings(settings);
            oldHost.validateSettings(settings);
            oldPort.validateSettings(settings);
        }
        // optional: authentication, user, password
        m_jobTimeout.validateSettings(settings);
        m_jobCheckFrequency.validateSettings(settings);

        // optional: sparkVersion
        m_contextName.validateSettings(settings);
        // try to find deleteObjectsOnDispose or fall back to oldDeleteRDDsOnDispose
        try {
            m_deleteObjectsOnDispose.validateSettings(settings);
        } catch(InvalidSettingsException e) {
            m_LegacyDeleteRDDsOnDispose.validateSettings(settings);
        }
        // optional: sparkJobLogLevel, overrideSparkSettings, customSparkSettings
    }

    /**
     * @param settings the NodeSettingsRO to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        // try to find jobManagerUrl or fall back to protocol+host+port
        try {
            m_jobManagerUrl.loadSettingsFrom(settings);
        } catch(InvalidSettingsException e) {
            oldProtocol.loadSettingsFrom(settings);
            oldHost.loadSettingsFrom(settings);
            oldPort.loadSettingsFrom(settings);
            m_jobManagerUrl.setStringValue(String.format("%s://%s:%d",
                oldProtocol.getStringValue(),
                oldHost.getStringValue(),
                oldPort.getIntValue()));
        }
        m_user.loadSettingsFrom(settings);
        m_password.setStringValue(demix(settings.getPassword(m_password.getKey(), String.valueOf(MY), null)));
        try {
            m_authentication.loadSettingsFrom(settings);
        } catch(InvalidSettingsException e) {
            m_authentication.setBooleanValue(!m_user.getStringValue().isEmpty());
        }
        m_jobTimeout.loadSettingsFrom(settings);
        m_jobCheckFrequency.loadSettingsFrom(settings);

        try { m_sparkVersion.loadSettingsFrom(settings); } catch (InvalidSettingsException e) {}
        m_contextName.loadSettingsFrom(settings);
        try {
            m_deleteObjectsOnDispose.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            m_LegacyDeleteRDDsOnDispose.loadSettingsFrom(settings);
            m_deleteObjectsOnDispose.setBooleanValue(m_LegacyDeleteRDDsOnDispose.getBooleanValue());
        }
        try { m_sparkJobLogLevel.loadSettingsFrom(settings); } catch (InvalidSettingsException e) {}
        try {
            m_overrideSparkSettings.loadSettingsFrom(settings);
            m_customSparkSettings.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            try {
                m_LegacyMemPerNode.loadSettingsFrom(settings);
                m_overrideSparkSettings.setBooleanValue(true);
                m_customSparkSettings.setStringValue("memory-per-node: " + m_LegacyMemPerNode.getStringValue() + "\n");
            } catch (InvalidSettingsException e2) {
                m_overrideSparkSettings.setBooleanValue(false);
            }
        }
    }

    /**
     * @return the KNIMESparkContext object with the specified settings
     */
    public SparkContextConfig createContextConfig() {
        return new SparkContextConfig(
            getJobManagerUrl(), useAuthentication(), getUser(), getPassword(),
            getJobCheckFrequency(), getJobTimeout(),
            getSparkVersion(), getContextName(), deleteObjectsOnDispose(),
            getSparkJobLogLevel(), overrideSparkSettings(), getCustomSparkSettings());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        final StringBuilder builder = new StringBuilder();
        builder.append("KNIMESparkContext [url=");
        builder.append(getJobManagerUrl());
        builder.append(", auth=");
        builder.append(useAuthentication());
        builder.append(", user=");
        builder.append(getUser());
        builder.append(", password set=");
        builder.append(getPassword() != null);
        builder.append(", jobCheckFrequency=");
        builder.append(getJobCheckFrequency());
        builder.append(", jobTimeout=");
        builder.append(getJobTimeout());
        builder.append(", sparkVersion=");
        builder.append(getSparkVersion());
        builder.append(", contextName=");
        builder.append(getContextName());
        builder.append(", deleteObjectsOnDispose=");
        builder.append(deleteObjectsOnDispose());
        builder.append(", sparkJobLogLevel=");
        builder.append(getSparkJobLogLevel());
        builder.append(", overrideSparkSettings=");
        builder.append(overrideSparkSettings());
        builder.append(", customSparkSettings=");
        builder.append(getCustomSparkSettings());
        builder.append("]");
        return builder.toString();
    }
}
