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
 *   Created on 03.07.2015 by koetter
 *   Changes on 07.06.2016 by Sascha Wolke:
 *     - fields added: jobServerUrl, authentication, sparkJobLogLevel, overrideSparkSettings, customSparkSettings
 *     - protocol+host+port migrated into jobServerUrl
 *     - authentication flag added
 *     - deleteRDDsOnDispose renamed to deleteObjectsOnDispose
 *     - memPerNode migrated into overrideSparkSettings+customSparkSettings
 */
package org.knime.bigdata.spark.node.util.context.create;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang3.ArrayUtils;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.port.context.JobServerSparkContextConfig;
import org.knime.bigdata.spark.core.preferences.KNIMEConfigContainer;
import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication;
import org.knime.core.node.defaultnodesettings.SettingsModelAuthentication.AuthenticationType;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelInteger;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelLong;
import org.knime.core.node.defaultnodesettings.SettingsModelLongBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.workflow.CredentialsProvider;
import org.knime.core.node.workflow.ICredentials;

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
 * Changes in version 1.6:
 *   - receive timeout added
 *   - job timeout removed
 *
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class ContextSettings {

    private final SettingsModelString m_settingsFormatVersion =
            new SettingsModelString("settingsFormatVersion", "1.6.0");

    private final SettingsModelString m_jobServerUrl =
            new SettingsModelString("v1_6.jobServerUrl", KNIMEConfigContainer.getJobServerUrl());

    private final SettingsModelAuthentication m_authentication =
            new SettingsModelAuthentication("v1_6.authentication", AuthenticationType.NONE, null, null, null);

    private final SettingsModelLong m_receiveTimeout =
            new SettingsModelLongBounded("v1_6.sparkReceiveTimeout", KNIMEConfigContainer.getReceiveTimeout().getSeconds(), 0, Integer.MAX_VALUE);

    private final SettingsModelInteger m_jobCheckFrequency =
            new SettingsModelIntegerBounded("v1_6.sparkJobCheckFrequency", KNIMEConfigContainer.getJobCheckFrequency(), 1, Integer.MAX_VALUE);

    private final SettingsModelString m_sparkVersion =
            new SettingsModelString("v1_6.sparkVersion", KNIMEConfigContainer.getSparkVersion().getLabel());

    private final SettingsModelString m_contextName =
            new SettingsModelString("v1_6.contextName", KNIMEConfigContainer.getSparkContext());

    private final SettingsModelBoolean m_deleteContextOnDispose =
            new SettingsModelBoolean("v1_6.deleteContextOnDispose", false);

    private final SettingsModelBoolean m_deleteObjectsOnDispose =
            new SettingsModelBoolean("v1_6.deleteObjectsOnDispose", KNIMEConfigContainer.deleteSparkObjectsOnDispose());

    private final SettingsModelBoolean m_overrideSparkSettings =
            new SettingsModelBoolean("v1_6.overrideSparkSettings", KNIMEConfigContainer.overrideSparkSettings());

    private final SettingsModelString m_customSparkSettings =
            new SettingsModelString("v1_6.customSparkSettings", KNIMEConfigContainer.getCustomSparkSettingsString());

    private final SettingsModelBoolean m_hideExistsWarning =
            new SettingsModelBoolean("v1_6.hideExistsWarning", false);

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
        m_customSparkSettings.setEnabled(m_overrideSparkSettings.getBooleanValue());
    }

    protected SettingsModelString getJobServerUrlModel() {
        return m_jobServerUrl;
    }

    protected SettingsModelAuthentication getAuthenticateModel() {
        return m_authentication;
    }

    protected SettingsModelLong getReceiveTimeoutModel() {
        return m_receiveTimeout;
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

    protected SettingsModelBoolean getDeleteContextOnDisposeModel() {
        return m_deleteContextOnDispose;
    }

    protected SettingsModelBoolean getDeleteObjectsOnDisposeModel() {
        return m_deleteObjectsOnDispose;
    }

    protected SettingsModelBoolean getOverrideSparkSettingsModel() {
        return m_overrideSparkSettings;
    }

    protected SettingsModelString getCustomSparkSettingsModel() {
        return m_customSparkSettings;
    }

    protected SettingsModelBoolean getHideExistsWarningModel() {
        return m_hideExistsWarning;
    }



    public String getJobServerUrl() {
        return m_jobServerUrl.getStringValue();
    }

    public Duration getReceiveTimeout() {
        return Duration.ofSeconds(m_receiveTimeout.getLongValue());
    }

    public int getJobCheckFrequency() {
        return m_jobCheckFrequency.getIntValue();
    }

    public SparkVersion getSparkVersion() {
        // the settings model for the Spark version operates with label strings
        return SparkVersion.fromLabel(m_sparkVersion.getStringValue());
    }

    public String getContextName() {
        return m_contextName.getStringValue();
    }

    public boolean deleteContextOnDispose() {
        return m_deleteContextOnDispose.getBooleanValue();
    }

    public boolean deleteObjectsOnDispose() {
        return m_deleteObjectsOnDispose.getBooleanValue();
    }

    public boolean overrideSparkSettings() {
        return m_overrideSparkSettings.getBooleanValue();
    }

    public String getCustomSparkSettingsString() {
        return m_customSparkSettings.getStringValue();
    }

    public Map<String, String> getCustomSparkSettings() {
        return SparkPreferenceValidator.parseSettingsString(getCustomSparkSettingsString());
    }

    public boolean hideExistsWarning() {
        return m_hideExistsWarning.getBooleanValue();
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
        m_receiveTimeout.saveSettingsTo(settings);
        m_jobCheckFrequency.saveSettingsTo(settings);

        m_sparkVersion.saveSettingsTo(settings);
        m_contextName.saveSettingsTo(settings);
        m_deleteContextOnDispose.saveSettingsTo(settings);
        m_deleteObjectsOnDispose.saveSettingsTo(settings);
        m_overrideSparkSettings.saveSettingsTo(settings);
        m_customSparkSettings.saveSettingsTo(settings);

        m_hideExistsWarning.saveSettingsTo(settings);
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

        // Load settings into a temporary object + migrate data if required and validate it
        ContextSettings contextSettings = new ContextSettings();
        contextSettings.loadSettingsFrom(settings);
        contextSettings.validateSettings();
    }

    public void validateSettingsLegacy(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_legacyProtocol.validateSettings(settings);
        m_legacyHost.validateSettings(settings);
        m_legacyPort.validateSettings(settings);
        m_legacyUser.validateSettings(settings);
        m_legacyJobCheckFrequency.validateSettings(settings);

        m_legacyContextName.validateSettings(settings);
        m_legacyDeleteRDDsOnDispose.validateSettings(settings);
        m_legacyMemPerNode.validateSettings(settings);
    }

    public void validateSettings_v1_6(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_jobServerUrl.validateSettings(settings);
        m_authentication.validateSettings(settings);
        // optional: receive timeout
        m_jobCheckFrequency.validateSettings(settings);

        m_sparkVersion.validateSettings(settings);
        m_contextName.validateSettings(settings);
        m_deleteObjectsOnDispose.validateSettings(settings);
        m_overrideSparkSettings.validateSettings(settings);
        if (m_overrideSparkSettings.getBooleanValue()) {
            m_customSparkSettings.validateSettings(settings);
        }
    }

    /**
     * Validate current values of this setting.
     *
     * @throws InvalidSettingsException if validation failed.
     */
    public void validateSettings() throws InvalidSettingsException {

        final ArrayList<String> errors = new ArrayList<>();

        SparkPreferenceValidator.validateSparkContextName(getContextName(), errors);
        SparkPreferenceValidator.validateRESTEndpointURL(getJobServerUrl(), errors, "jobserver");
        SparkPreferenceValidator.validateReceiveTimeout(getReceiveTimeout(), errors);
        SparkPreferenceValidator.validateCustomSparkSettings(overrideSparkSettings(), getCustomSparkSettingsString(), errors);

        switch(m_authentication.getAuthenticationType()) {
            case USER:
            case USER_PWD:
                SparkPreferenceValidator.validateUsernameAndPassword(m_authentication.getUsername(), m_authentication.getPassword(), errors);
                break;
            case CREDENTIALS:
                SparkPreferenceValidator.validateCredential(m_authentication.getCredential(), errors);
                break;
            default:
                break;
        }

        if (!errors.isEmpty()) {
            throw new InvalidSettingsException(SparkPreferenceValidator.mergeErrors(errors));
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
        m_legacyPassword.setStringValue(demix(settings.getPassword(m_legacyPassword.getKey(), String.valueOf(MY), null)));
        m_legacyJobCheckFrequency.loadSettingsFrom(settings);

        m_legacyContextName.loadSettingsFrom(settings);
        m_legacyDeleteRDDsOnDispose.loadSettingsFrom(settings);
        m_legacyMemPerNode.loadSettingsFrom(settings);

        m_jobServerUrl.setStringValue(String.format("%s://%s:%d",
            m_legacyProtocol.getStringValue(),
            m_legacyHost.getStringValue(),
            m_legacyPort.getIntValue()));
        if (m_legacyUser.getStringValue() != null && !m_legacyUser.getStringValue().isEmpty()) {
            if (m_legacyPassword.getStringValue() != null && !m_legacyPassword.getStringValue().isEmpty()) {
                m_authentication.setValues(AuthenticationType.USER_PWD, null, m_legacyUser.getStringValue(), m_legacyPassword.getStringValue());
            } else {
                m_authentication.setValues(AuthenticationType.USER, null, m_legacyUser.getStringValue(), null);
            }
        } else {
            m_authentication.setValues(AuthenticationType.NONE, null, null, null);
        }
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
        try {
            m_receiveTimeout.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            // setting can be empty on old workflows
        }
        m_jobCheckFrequency.loadSettingsFrom(settings);

        m_sparkVersion.loadSettingsFrom(settings);
        m_contextName.loadSettingsFrom(settings);
        m_deleteObjectsOnDispose.loadSettingsFrom(settings);
        m_overrideSparkSettings.loadSettingsFrom(settings);
        m_customSparkSettings.loadSettingsFrom(settings);

        try {
            m_deleteContextOnDispose.loadSettingsFrom(settings);
        } catch(InvalidSettingsException e) {
            // optional setting, nothing to do if it does not exists in given settings
        }

        try {
            m_hideExistsWarning.loadSettingsFrom(settings);
        } catch(InvalidSettingsException e) {
            // optional setting, nothing to do if it does not exists in given settings
        }
    }

    /**
     * @return the KNIMESparkContext object with the specified settings
     */
    public JobServerSparkContextConfig createContextConfig(final CredentialsProvider cp) {
        AuthenticationType authType = m_authentication.getAuthenticationType();

        if (authType == AuthenticationType.NONE) {
            return new JobServerSparkContextConfig(
                getJobServerUrl(), false, null, null,
                getReceiveTimeout(), getJobCheckFrequency(),
                getSparkVersion(), getContextName(), deleteObjectsOnDispose(),
                overrideSparkSettings(), getCustomSparkSettings());

        } else if (authType == AuthenticationType.USER) {
            return new JobServerSparkContextConfig(
                getJobServerUrl(), true, m_authentication.getUsername(), "",
                getReceiveTimeout(), getJobCheckFrequency(),
                getSparkVersion(), getContextName(), deleteObjectsOnDispose(),
                overrideSparkSettings(), getCustomSparkSettings());

        } else if (authType == AuthenticationType.USER_PWD) {
            return new JobServerSparkContextConfig(
                getJobServerUrl(), true, m_authentication.getUsername(), m_authentication.getPassword(),
                getReceiveTimeout(), getJobCheckFrequency(),
                getSparkVersion(), getContextName(), deleteObjectsOnDispose(),
                overrideSparkSettings(), getCustomSparkSettings());

        } else if (authType == AuthenticationType.CREDENTIALS) {
            ICredentials cred = cp.get(m_authentication.getCredential());

            return new JobServerSparkContextConfig(
                getJobServerUrl(), true, cred.getLogin(), cred.getPassword(),
                getReceiveTimeout(), getJobCheckFrequency(),
                getSparkVersion(), getContextName(), deleteObjectsOnDispose(),
                overrideSparkSettings(), getCustomSparkSettings());

        } else {
            throw new RuntimeException("Unsupported authentication method: " + authType);
        }
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
        if (m_authentication.getAuthenticationType() == AuthenticationType.USER) {
            builder.append("true, user=");
            builder.append(m_authentication.getUsername());
            builder.append(", password set=false");
        } else if (m_authentication.getAuthenticationType() == AuthenticationType.USER_PWD) {
            builder.append("true, user=");
            builder.append(m_authentication.getUsername());
            builder.append(", password set=");
            builder.append(m_authentication.getPassword() != null);
        } else if (m_authentication.useCredential()) {
            builder.append("true, credentials=");
            builder.append(m_authentication.getCredential());
        }
        builder.append(", jobCheckFrequency=");
        builder.append(getJobCheckFrequency());
        builder.append(", receiveTimeout=");
        builder.append(getReceiveTimeout().getSeconds());
        builder.append(", sparkVersion=");
        builder.append(getSparkVersion());
        builder.append(", contextName=");
        builder.append(getContextName());
        builder.append(", deleteContextOnDispose=");
        builder.append(deleteContextOnDispose());
        builder.append(", deleteObjectsOnDispose=");
        builder.append(deleteObjectsOnDispose());
        builder.append(", overrideSparkSettings=");
        builder.append(overrideSparkSettings());
        builder.append(", customSparkSettings=");
        builder.append(getCustomSparkSettings());
        builder.append(", hideExistsWarning=");
        builder.append(hideExistsWarning());
        builder.append("]");
        return builder.toString();
    }
}
