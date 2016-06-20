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
 *     - fields added: jobServerUrl, authentication, sparkJobLogLevel, overrideSparkSettings, customSparkSettings
 *     - protocol+host+port migrated into jobServerUrl
 *     - authentication flag added
 *     - deleteRDDsOnDispose renamed to deleteObjectsOnDispose
 *     - memPerNode migrated into overrideSparkSettings+customSparkSettings
 */
package com.knime.bigdata.spark.node.util.context.create;

import java.net.URI;
import java.net.URISyntaxException;
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
 * Changes from initial version to version 1.6:
 *   - v1_6 prefix added on all config names
 *   - settings format version added
 *   - protocol+host+port -> jobServerUrl
 *   - user+pass -> authentication+user+pass
 *   - deleteRDDsOnDispose -> deleteObjectsOnDispose
 *   - memPerNode -> overrideSparkSettings+customSparkSettings
 *
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class ContextSettings {

    private final SettingsModelString m_settingsFormatVersion =
            new SettingsModelString("settingsFormatVersion", "1.6.0");

    private final SettingsModelString m_jobServerUrl =
            new SettingsModelString("v1_6.jobServerUrl", KNIMEConfigContainer.getJobServerUrl());

    private final SettingsModelBoolean m_authentication =
            new SettingsModelBoolean("v1_6.authentication", KNIMEConfigContainer.useAuthentication());

    private final SettingsModelString m_user =
            new SettingsModelString("v1_6.user", KNIMEConfigContainer.getUserName());

    private final SettingsModelString m_password =
            new SettingsModelString("v1_6.password", KNIMEConfigContainer.getPassword() == null ? null : String.valueOf(KNIMEConfigContainer.getUserName()));

    private final SettingsModelInteger m_jobTimeout =
            new SettingsModelIntegerBounded("v1_6.sparkJobTimeout", KNIMEConfigContainer.getJobTimeout(), 1, Integer.MAX_VALUE);

    private final SettingsModelInteger m_jobCheckFrequency =
            new SettingsModelIntegerBounded("v1_6.sparkJobCheckFrequency", KNIMEConfigContainer.getJobCheckFrequency(), 1, Integer.MAX_VALUE);

    private final SettingsModelString m_sparkVersion =
            new SettingsModelString("v1_6.sparkVersion", KNIMEConfigContainer.getSparkVersion().getLabel());

    private final SettingsModelString m_contextName =
            new SettingsModelString("v1_6.contextName", KNIMEConfigContainer.getSparkContext());

    private final SettingsModelBoolean m_deleteObjectsOnDispose =
            new SettingsModelBoolean("v1_6.deleteObjectsOnDispose", KNIMEConfigContainer.deleteSparkObjectsOnDispose());

    private final SettingsModelString m_sparkJobLogLevel =
            new SettingsModelString("v1_6.sparkJobLogLevel", KNIMEConfigContainer.getSparkJobLogLevel());

    private final SettingsModelBoolean m_overrideSparkSettings =
            new SettingsModelBoolean("v1_6.overrideSparkSettings", KNIMEConfigContainer.overrideSparkSettings());

    private final SettingsModelString m_customSparkSettings =
            new SettingsModelString("v1_6.customSparkSettings", KNIMEConfigContainer.getCustomSparkSettings());


    /** @deprecated use m_jobServerUrl instead */
    @Deprecated private final SettingsModelString m_legacyProtocol = new SettingsModelString("protocol", "http");
    /** @deprecated use m_jobServerUrl instead */
    @Deprecated private final SettingsModelString m_legacyHost = new SettingsModelString("host", "localhost");
    /** @deprecated use m_jobServerUrl instead */
    @Deprecated private final SettingsModelInteger m_legacyPort = new SettingsModelInteger("port", 8090);
    /** @deprecated use m_user instead */
    @Deprecated private final SettingsModelString m_legacyUser =
            new SettingsModelString("user", KNIMEConfigContainer.getUserName());
    /** @deprecated use m_password instead */
    @Deprecated private final SettingsModelString m_legacyPassword =
            new SettingsModelString("password", KNIMEConfigContainer.getPassword() == null ? null : String.valueOf(KNIMEConfigContainer.getUserName()));
    /** @deprecated use m_jobTimeout instead */
    @Deprecated private final SettingsModelInteger m_legacyJobTimeout =
            new SettingsModelIntegerBounded("sparkJobTimeout", KNIMEConfigContainer.getJobTimeout(), 1, Integer.MAX_VALUE);
    /** @deprecated use m_jobCheckFrequency instead */
    @Deprecated private final SettingsModelInteger m_legacyJobCheckFrequency =
            new SettingsModelIntegerBounded("sparkJobCheckFrequency", KNIMEConfigContainer.getJobCheckFrequency(), 1, Integer.MAX_VALUE);
    /** @deprecated use m_contextName instead */
    @Deprecated private final SettingsModelString m_legacyContextName =
            new SettingsModelString("contextName", KNIMEConfigContainer.getSparkContext());
    /** @deprecated renamed into m_deleteObjectsOnDispose */
    @Deprecated private final SettingsModelBoolean m_legacyDeleteRDDsOnDispose = new SettingsModelBoolean("deleteRDDsOnDispose", false);
    /** @deprecated merged into customSparkSettings */
    @Deprecated private final SettingsModelString m_legacyMemPerNode = new SettingsModelString("memPerNode", null);

    private static final char[] MY =
            "3O}accA80479[7b@05b9378K}18358QLG32P�92JZW76b76@2eb9$a\\23-c0a397a%ee'e35!89afFfA64#8bB8GRl".toCharArray();

    public ContextSettings() {
        m_user.setEnabled(m_authentication.getBooleanValue());
        m_password.setEnabled(m_authentication.getBooleanValue());
        m_customSparkSettings.setEnabled(m_overrideSparkSettings.getBooleanValue());
    }

    protected SettingsModelString getJobServerUrlModel() {
        return m_jobServerUrl;
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



    public String getJobServerUrl() {
        return m_jobServerUrl.getStringValue();
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
        return SparkContextID.fromConnectionDetails(getJobServerUrl(), getContextName());
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
        m_settingsFormatVersion.saveSettingsTo(settings);
        m_jobServerUrl.saveSettingsTo(settings);
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
     * @param settings the NodeSettingsRO to validate
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        boolean legacy = false;
        try {
            m_settingsFormatVersion.validateSettings(settings);
        } catch(InvalidSettingsException e) {
            legacy = true;
        }

        if (legacy) {
            validateSettingsLegacy(settings);
        } else {
            validateSettings_v1_6(settings);
        }
    }

    public void validateSettingsLegacy(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_legacyProtocol.validateSettings(settings);
        m_legacyHost.validateSettings(settings);
        m_legacyPort.validateSettings(settings);
        m_legacyUser.validateSettings(settings);
        m_legacyJobTimeout.validateSettings(settings);
        m_legacyJobCheckFrequency.validateSettings(settings);

        m_legacyContextName.validateSettings(settings);
        m_legacyDeleteRDDsOnDispose.validateSettings(settings);
        m_legacyMemPerNode.validateSettings(settings);
    }

    public void validateSettings_v1_6(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_jobServerUrl.validateSettings(settings);
        m_authentication.validateSettings(settings);
        if (m_authentication.getBooleanValue()) {
            m_user.validateSettings(settings);
        }
        m_jobTimeout.validateSettings(settings);
        m_jobCheckFrequency.validateSettings(settings);

        m_sparkVersion.validateSettings(settings);
        m_contextName.validateSettings(settings);
        m_deleteObjectsOnDispose.validateSettings(settings);
        m_sparkJobLogLevel.validateSettings(settings);
        m_overrideSparkSettings.validateSettings(settings);
        if (m_overrideSparkSettings.getBooleanValue()) {
            m_customSparkSettings.validateSettings(settings);
        }
    }

    /** Validate current values of this setting. */
    public void validateSettings() throws InvalidSettingsException {
        // Job server URL
        try {
            URI uri = new URI(m_jobServerUrl.getStringValue());

            if (uri.getScheme() == null || uri.getScheme().isEmpty()) {
                throw new InvalidSettingsException("Protocol in job server URL required (http or https)");
            } else if (uri.getHost() == null || uri.getHost().isEmpty()) {
                throw new InvalidSettingsException("Hostname in job server URL required.");
            } else if (uri.getPort() < 0) {
                throw new InvalidSettingsException("Port in job server URL required.");
            }

        } catch(URISyntaxException e) {
            throw new InvalidSettingsException("Invalid job server url: " + e.getMessage());
        }

        // Username
        if (m_authentication.getBooleanValue() && (m_user.getStringValue() == null || m_user.getStringValue().isEmpty())) {
            throw new InvalidSettingsException("Username required with authentication enabled.");
        }

        // Context name
        if (m_contextName.getStringValue() == null || m_contextName.getStringValue().isEmpty()) {
            throw new InvalidSettingsException("Context name required.");
        } else if (!m_contextName.getStringValue().matches("^[A-Za-z].*")) {
            throw new InvalidSettingsException("Context name must start with letters.");
        } else if (m_contextName.getStringValue().contains("/")) {
            throw new InvalidSettingsException("Slash chararacter is not support in context name.");
        }

        // Custom spark settings
        if (m_overrideSparkSettings.getBooleanValue()) {
            String lines[] = m_customSparkSettings.getStringValue().split("\n");
            for (int i = 0; i < lines.length; i++) {
                if (!lines[i].isEmpty() && !lines[i].startsWith("#") && !lines[i].startsWith("//")) {
                    String kv[] = lines[i].split(": ", 2);

                    if (kv.length != 2 || kv[0].isEmpty() || kv[1].isEmpty()) {
                        throw new InvalidSettingsException("Failed to parse custom spark config line " + (i + 1) + ".");
                    }
                }
            }
        }
    }

    /**
     * @param settings the NodeSettingsRO to read from
     * @throws InvalidSettingsException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        boolean legacy = false;
        try {
            m_settingsFormatVersion.loadSettingsFrom(settings);
        } catch(InvalidSettingsException e) {
            legacy = true;
        }

        if (legacy) {
            migrateSettingsFromLegacy(settings);
        } else {
            loadSettingsFrom_v1_6(settings);
        }
    }

    public void migrateSettingsFromLegacy(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_legacyProtocol.loadSettingsFrom(settings);
        m_legacyHost.loadSettingsFrom(settings);
        m_legacyPort.loadSettingsFrom(settings);
        m_legacyUser.loadSettingsFrom(settings);
        m_legacyPassword.setStringValue(demix(settings.getPassword(m_password.getKey(), String.valueOf(MY), null)));
        m_legacyJobTimeout.loadSettingsFrom(settings);
        m_legacyJobCheckFrequency.loadSettingsFrom(settings);

        m_legacyContextName.loadSettingsFrom(settings);
        m_legacyDeleteRDDsOnDispose.loadSettingsFrom(settings);
        m_legacyMemPerNode.loadSettingsFrom(settings);

        m_jobServerUrl.setStringValue(String.format("%s://%s:%d",
            m_legacyProtocol.getStringValue(),
            m_legacyHost.getStringValue(),
            m_legacyPort.getIntValue()));
        m_user.setStringValue(m_legacyUser.getStringValue());
        m_password.setStringValue(m_legacyPassword.getStringValue());
        if (m_user.getStringValue() != null && m_user.getStringValue().isEmpty()) {
            m_authentication.setBooleanValue(true);
        } else {
            m_user.setEnabled(false);
            m_password.setEnabled(false);
        }
        m_jobTimeout.setIntValue(m_legacyJobTimeout.getIntValue());
        m_jobCheckFrequency.setIntValue(m_legacyJobCheckFrequency.getIntValue());

        // use default: m_sparkVersion
        m_contextName.setStringValue(m_legacyContextName.getStringValue());
        m_deleteObjectsOnDispose.setBooleanValue(m_legacyDeleteRDDsOnDispose.getBooleanValue());
        // use default: m_sparkJobLogLevel
        final String memPerNode = m_legacyMemPerNode.getStringValue();
        if (memPerNode != null && !memPerNode.isEmpty() && !memPerNode.equals("512m")) {
            m_overrideSparkSettings.setBooleanValue(true);
            m_customSparkSettings.setStringValue("memory-per-node: " + memPerNode + "\n");
        } else {
            m_overrideSparkSettings.setBooleanValue(false);
            m_customSparkSettings.setEnabled(false);
        }
    }

    public void loadSettingsFrom_v1_6(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_jobServerUrl.loadSettingsFrom(settings);
        m_authentication.loadSettingsFrom(settings);
        m_user.loadSettingsFrom(settings);
        m_password.setStringValue(demix(settings.getPassword(m_password.getKey(), String.valueOf(MY), null)));
        m_jobTimeout.loadSettingsFrom(settings);
        m_jobCheckFrequency.loadSettingsFrom(settings);

        m_sparkVersion.loadSettingsFrom(settings);
        m_contextName.loadSettingsFrom(settings);
        m_deleteObjectsOnDispose.loadSettingsFrom(settings);
        m_overrideSparkSettings.loadSettingsFrom(settings);
        m_customSparkSettings.loadSettingsFrom(settings);
    }

    /**
     * @return the KNIMESparkContext object with the specified settings
     */
    public SparkContextConfig createContextConfig() {
        return new SparkContextConfig(
            getJobServerUrl(), useAuthentication(), getUser(), getPassword(),
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
        builder.append(getJobServerUrl());
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
