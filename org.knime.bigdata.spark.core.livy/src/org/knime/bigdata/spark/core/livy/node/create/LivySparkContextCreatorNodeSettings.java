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
package org.knime.bigdata.spark.core.livy.node.create;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.livy.LivySparkContextProvider;
import org.knime.bigdata.spark.core.livy.context.LivySparkContextConfig;
import org.knime.bigdata.spark.core.livy.node.create.ui.KeyDescriptor;
import org.knime.bigdata.spark.core.livy.node.create.ui.SettingsModelKeyValue;
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
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.util.ButtonGroupEnumInterface;

/**
 * Settings class for the "Create Local Big Data Environment" node.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 * @see LivySparkContextCreatorNodeModel
 * @see LivySparkContextCreatorNodeDialog
 */
public class LivySparkContextCreatorNodeSettings {

    /**
     * Described the executor allocation strategy for Spark.
     * 
     * @author Bjoern Lohrmann, KNIME GmbH
     *
     */
    public enum ExecutorAllocation implements ButtonGroupEnumInterface {
            /**
             * The allocation strategy configured by default on the cluster (we don't set anything).
             */
            DEFAULT,

            /**
             * A fixed number of executors.
             */
            FIXED,

            /**
             * Dynamic execution allocation (aka. dynamic worker allocation).
             */
            DYNAMIC;

        @Override
        public String getText() {
            switch (this) {
                case DEFAULT:
                    return "Default allocation";
                case FIXED:
                    return "Fixed allocation";
                case DYNAMIC:
                    return "Dynamic allocation";
                default:
                    throw new IllegalArgumentException("Unknown allocation strategy: " + toString());
            }
        }

        @Override
        public String getActionCommand() {
            return this.toString();
        }

        @Override
        public String getToolTip() {
            switch (this) {
                case DEFAULT:
                    return "Allocates Spark executors based on default the configuration on the cluster.";
                case FIXED:
                    return "Allocates a fixed number of Spark executors.";
                case DYNAMIC:
                    return "Allocates a dynamic number of Spark executors, depending on the workload.";
                default:
                    throw new IllegalArgumentException("Unknown allocation strategy: " + toString());
            }
        }

        @Override
        public boolean isDefault() {
            return this == FIXED;
        }
    }

    /**
     * This is an invisible setting used to allow the node settings to evolve over time. It allows us to detect the
     * current version and take appropriate measures to update the settings in a controlled manner.
     */
    private final SettingsModelInteger m_settingsVersion = new SettingsModelInteger("settingsVersion", 1);

    private final SettingsModelString m_sparkVersion =
        new SettingsModelString("sparkVersion", LivySparkContextProvider.HIGHEST_SUPPORTED_SPARK_VERSION.getLabel());

    private final SettingsModelString m_livyUrl = new SettingsModelString("livyUrl", "http://localhost:8998/");

    private final SettingsModelAuthentication m_authentication =
        new SettingsModelAuthentication("authentication", AuthenticationType.KERBEROS, null, null, null);

    private final ContainerResourceSettings m_executorResources = new ContainerResourceSettings();

    private final ContainerResourceSettings m_driverResources = new ContainerResourceSettings();

    private final SettingsModelString m_executorAllocation =
        new SettingsModelString("executorAllocation", ExecutorAllocation.FIXED.getActionCommand());

    private final SettingsModelIntegerBounded m_fixedExecutors =
        new SettingsModelIntegerBounded("fixedExecutors", 1, 1, 1000000);

    private final SettingsModelIntegerBounded m_dynamicExecutorsMin =
        new SettingsModelIntegerBounded("dynamicExecutorsMin", 1, 1, 1000000);

    private final SettingsModelIntegerBounded m_dynamicExecutorsMax =
        new SettingsModelIntegerBounded("dynamicExecutorsMax", 10, 1, 1000000);

    private final SettingsModelBoolean m_overrideSparkSettings =
        new SettingsModelBoolean("overrideSparkSettings", KNIMEConfigContainer.overrideSparkSettings());

    private final SettingsModelKeyValue m_customSparkSettings;

    private final SettingsModelIntegerBounded m_connectTimeout =
        new SettingsModelIntegerBounded("connectTimeout", 30, 0, Integer.MAX_VALUE);

    private final SettingsModelIntegerBounded m_responseTimeout =
        new SettingsModelIntegerBounded("requestTimeout", 30, 0, Integer.MAX_VALUE);

    private final SettingsModelIntegerBounded m_jobCheckFrequency =
        new SettingsModelIntegerBounded("jobCheckFrequency", 1, 1, Integer.MAX_VALUE);

    private final static Set<String> SPARK_SETTINGS_BLACKLIST = new HashSet<>(Arrays.asList(
        // blacklisted because the are defined via other settings models
        "spark.driver.cores", "spark.driver.memory", "spark.executor.memory", "spark.executor.cores", "spark.master",
        "spark.executor.instances", "spark.dynamicAllocation.enabled", "spark.dynamicAllocation.minExecutors",
        "spark.dynamicAllocation.maxExecutors",

        // blacklisted because they only pertain to yarn-client or Mesos deploy modes, which Livy does not support
        "spark.yarn.am.memory", "spark.yarn.am.cores", "spark.yarn.am.memoryOverhead", "spark.yarn.am.extraJavaOptions",
        "spark.yarn.am.extraLibraryPath"));

    /**
     * Constructor.
     */
    @SuppressWarnings("unchecked")
    public LivySparkContextCreatorNodeSettings() {

        try {
            final List<?> supportedSettings = SparkSetting.getSupportedSettings(getSparkVersion());
            m_customSparkSettings = new SettingsModelKeyValue("customSparkSettings",
                filterSupportedSparkSettings((List<KeyDescriptor>)supportedSettings));
        } catch (IOException e) {
            // rethrowing as RuntimeException because (a) this should never happen and (b) it would introduce exception handling in very
            // inconvenient places
            throw new RuntimeException(e);
        }
        updateEnabledness();
    }

    private static List<KeyDescriptor> filterSupportedSparkSettings(List<KeyDescriptor> supportedSettings) {
        return supportedSettings.stream().filter(setting -> !SPARK_SETTINGS_BLACKLIST.contains(setting.getKey()))
            .collect(Collectors.toList());
    }

    /**
     * Updates the enabledness of the underlying settings models.
     */
    public void updateEnabledness() {
        m_executorResources.updateEnabledness();
        m_driverResources.updateEnabledness();
        m_customSparkSettings.setEnabled(m_overrideSparkSettings.getBooleanValue());
    }

    /**
     * @return the settings model for the Livy URL.
     * @see #getLivyUrl()
     */
    protected SettingsModelString getLivyUrlModel() {
        return m_livyUrl;
    }

    /**
     * 
     * @return the Livy URL.
     */
    public String getLivyUrl() {
        return m_livyUrl.getStringValue();
    }

    /**
     * @return the settings model for the Spark version to assume.
     */
    protected SettingsModelString getSparkVersionModel() {
        return m_sparkVersion;
    }

    /**
     * @return the {@link SparkVersion} to assume.
     */
    public SparkVersion getSparkVersion() {
        return SparkVersion.fromLabel(m_sparkVersion.getStringValue());
    }

    /**
     * @return settings model for the type of authentication to use against Livy.
     */
    protected SettingsModelAuthentication getAuthenticationModel() {
        return m_authentication;
    }

    /**
     * @return the type of authentication to use against Livy.
     */
    protected AuthenticationType getAuthenticationType() {
        return m_authentication.getAuthenticationType();
    }

    /**
     * @return resource settings to use for Spark executors.
     */
    protected ContainerResourceSettings getExecutorResources() {
        return m_executorResources;
    }

    /**
     * @return resource settings to use for the Spark driver.
     */
    protected ContainerResourceSettings getDriverResources() {
        return m_driverResources;
    }

    /**
     * @return settings model for the executor allocation strategy for Spark.
     */
    protected SettingsModelString getExecutorAllocationModel() {
        return m_executorAllocation;
    }

    /**
     * @return the executor allocation strategy for Spark.
     */
    protected ExecutorAllocation getExecutorAllocation() {
        return ExecutorAllocation.valueOf(m_executorAllocation.getStringValue());
    }

    /**
     * 
     * @return settings model for the number of executors to request when using the "fixed" executor allocation
     *         strategy.
     */
    protected SettingsModelIntegerBounded getFixedExecutorsModel() {
        return m_fixedExecutors;
    }

    /**
     * 
     * @return the number of executors to request when using the "fixed" executor allocation strategy.
     */
    public int getFixedExecutors() {
        return m_fixedExecutors.getIntValue();
    }

    /**
     * 
     * @return settings model for the min number of executors to request when using the "dynamic" executor allocation
     *         strategy.
     */
    protected SettingsModelIntegerBounded getDynamicExecutorsMinModel() {
        return m_dynamicExecutorsMin;
    }

    /**
     * 
     * @return the min number of executors to request when using the "dynamic" executor allocation strategy.
     */
    public int getDynamicExecutorsMin() {
        return m_dynamicExecutorsMin.getIntValue();
    }

    /**
     * 
     * @return settings model for the max number of executors to request when using the "dynamic" executor allocation
     *         strategy.
     */
    protected SettingsModelIntegerBounded getDynamicExecutorsMaxModel() {
        return m_dynamicExecutorsMax;
    }

    /**
     * 
     * @return the max number of executors to request when using the "dynamic" executor allocation strategy.
     */
    public int getDynamicExecutorsMax() {
        return m_dynamicExecutorsMax.getIntValue();
    }

    /**
     * 
     * @return settings model for the TCP socket connection timeout in seconds when connecting to Livy.
     */
    protected SettingsModelIntegerBounded getConnectTimeoutModel() {
        return m_connectTimeout;
    }

    /**
     * 
     * @return the TCP socket connection timeout in seconds when connecting to Livy.
     */
    public int getConnectTimeout() {
        return m_connectTimeout.getIntValue();
    }

    /**
     * 
     * @return settings model for the HTTP response timeout in seconds when talking to Livy.
     */
    protected SettingsModelIntegerBounded getResponseTimeoutModel() {
        return m_responseTimeout;
    }

    /**
     * 
     * @return the HTTP response timeout in seconds when talking to Livy.
     */
    public int getResponseTimeout() {
        return m_responseTimeout.getIntValue();
    }

    /**
     * 
     * @return settings model for the Spark job status polling frequency in seconds.
     */
    protected SettingsModelIntegerBounded getJobCheckFrequencyModel() {
        return m_jobCheckFrequency;
    }

    /**
     * 
     * @return the Spark job status polling frequency in seconds.
     */
    public int getJobCheckFrequency() {
        return m_jobCheckFrequency.getIntValue();
    }

    /**
     * 
     * @return settings model for whether to use custom spark settings or not.
     * @see #useCustomSparkSettings()
     */
    protected SettingsModelBoolean getUseCustomSparkSettingsModel() {
        return m_overrideSparkSettings;
    }

    /**
     * 
     * @return settings model that says which custom spark settings to use.
     * @see #getCustomSparkSettings()
     */
    protected SettingsModelKeyValue getCustomSparkSettingsModel() {
        return m_customSparkSettings;
    }

    /**
     * 
     * @return a map that contains the custom Spark settings
     */
    public Map<String, String> getCustomSparkSettings() {
        return m_customSparkSettings.getKeyValuePairs();
    }

    /**
     * 
     * @return whether to use custom spark settings or not.
     */
    public boolean useCustomSparkSettings() {
        return m_overrideSparkSettings.getBooleanValue();
    }

    /**
     * Saves the the settings of this instance to the given {@link NodeSettingsWO}.
     * 
     * @param settings the NodeSettingsWO to write to.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_settingsVersion.saveSettingsTo(settings);
        m_sparkVersion.saveSettingsTo(settings);
        m_livyUrl.saveSettingsTo(settings);
        m_authentication.saveSettingsTo(settings);

        m_executorResources.saveSettingsTo(settings.addNodeSettings("executor"));
        m_driverResources.saveSettingsTo(settings.addNodeSettings("driver"));

        m_executorAllocation.saveSettingsTo(settings);
        m_fixedExecutors.saveSettingsTo(settings);
        m_dynamicExecutorsMin.saveSettingsTo(settings);
        m_dynamicExecutorsMax.saveSettingsTo(settings);

        m_overrideSparkSettings.saveSettingsTo(settings);
        m_customSparkSettings.saveSettingsTo(settings);

        m_connectTimeout.saveSettingsTo(settings);
        m_responseTimeout.saveSettingsTo(settings);
        m_jobCheckFrequency.saveSettingsTo(settings);
    }

    /**
     * Validates the settings in the given {@link NodeSettingsRO}.
     * 
     * @param settings the NodeSettingsRO to validate.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settingsVersion.validateSettings(settings);
        m_sparkVersion.validateSettings(settings);
        m_livyUrl.validateSettings(settings);
        m_authentication.validateSettings(settings);

        m_executorResources.validateSettings(settings.getNodeSettings("executor"));
        m_driverResources.validateSettings(settings.getNodeSettings("driver"));

        m_executorAllocation.validateSettings(settings);
        m_fixedExecutors.validateSettings(settings);
        m_dynamicExecutorsMin.validateSettings(settings);
        m_dynamicExecutorsMax.validateSettings(settings);

        m_overrideSparkSettings.validateSettings(settings);
        if (m_overrideSparkSettings.getBooleanValue()) {
            m_customSparkSettings.validateSettings(settings);
        }

        m_connectTimeout.validateSettings(settings);
        m_responseTimeout.validateSettings(settings);
        m_jobCheckFrequency.validateSettings(settings);

        final LivySparkContextCreatorNodeSettings tmpSettings = new LivySparkContextCreatorNodeSettings();
        tmpSettings.loadSettingsFrom(settings);
        tmpSettings.validateDeeper();
    }

    /**
     * Validate current settings values.
     * 
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void validateDeeper() throws InvalidSettingsException {
        final List<String> errors = new ArrayList<>();

        SparkPreferenceValidator.validateRESTEndpointURL(getLivyUrl(), errors, "Livy");

        try {
            getExecutorResources().getMemoryUnit();
        } catch (Exception e) {
            errors.add(
                "Unknown memory unit for executor: " + getExecutorResources().getMemoryUnitModel().getStringValue());
        }

        try {
            getDriverResources().getMemoryUnit();
        } catch (Exception e) {
            errors.add("Unknown memory unit for driver: " + getDriverResources().getMemoryUnitModel().getStringValue());
        }

        if (getDynamicExecutorsMax() < getDynamicExecutorsMin()) {
            errors.add("Maximum number of Spark executors to allocate must not be smaller than the minimum.");
        }

        if (!errors.isEmpty()) {
            throw new InvalidSettingsException(SparkPreferenceValidator.mergeErrors(errors));
        }
    }

    /**
     * @param settings the NodeSettingsRO to read from.
     * @throws InvalidSettingsException if the settings are invalid.
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settingsVersion.loadSettingsFrom(settings);
        m_sparkVersion.loadSettingsFrom(settings);
        m_livyUrl.loadSettingsFrom(settings);
        m_authentication.loadSettingsFrom(settings);

        m_executorResources.loadSettingsFrom(settings.getNodeSettings("executor"));
        m_driverResources.loadSettingsFrom(settings.getNodeSettings("driver"));

        m_executorAllocation.loadSettingsFrom(settings);
        m_fixedExecutors.loadSettingsFrom(settings);
        m_dynamicExecutorsMin.loadSettingsFrom(settings);
        m_dynamicExecutorsMax.loadSettingsFrom(settings);

        m_overrideSparkSettings.loadSettingsFrom(settings);
        m_customSparkSettings.loadSettingsFrom(settings);

        m_connectTimeout.loadSettingsFrom(settings);
        m_responseTimeout.loadSettingsFrom(settings);
        m_jobCheckFrequency.loadSettingsFrom(settings);
        updateEnabledness();
    }

    /**
     * Utility function to generate a Livy {@link SparkContextID}. This should act as the single source of truth when
     * generating IDs for Livy Spark contexts.
     * 
     * @param uniqueId A unique ID for the context. It is the responsibility of the caller to ensure uniqueness.
     * @return a new {@link SparkContextID}
     */
    public static SparkContextID createSparkContextID(final String uniqueId) {
        return new SparkContextID(String.format("%s://%s", SparkContextIDScheme.SPARK_LIVY, uniqueId));
    }

    /**
     * @param contextId The ID of the Spark context for which to create the config object.
     * @return a new {@link LivySparkContextConfig} derived from the current settings.
     */
    public LivySparkContextConfig createContextConfig(final SparkContextID contextId) {
        final Map<String, String> sparkSettings = generateSparkSettings();

        return new LivySparkContextConfig(getSparkVersion(), getLivyUrl(), getAuthenticationType(), getConnectTimeout(),
            getResponseTimeout(), getJobCheckFrequency(), sparkSettings, contextId);
    }

    private Map<String, String> generateSparkSettings() {
        final Map<String, String> toReturn = new HashMap<>();

        if (useCustomSparkSettings()) {
            toReturn.putAll(getCustomSparkSettings());
        }

        if (m_executorResources.overrideDefault()) {
            toReturn.put("spark.executor.memory", String.format("%d%s", m_executorResources.getMemory(),
                m_executorResources.getMemoryUnit().getSparkSettingsUnit()));
            toReturn.put("spark.executor.cores", Integer.toString(m_executorResources.getCores()));
        }

        if (m_driverResources.overrideDefault()) {
            toReturn.put("spark.driver.memory", String.format("%d%s", m_driverResources.getMemory(),
                m_driverResources.getMemoryUnit().getSparkSettingsUnit()));
            toReturn.put("spark.driver.cores", Integer.toString(m_driverResources.getCores()));
        }

        switch (getExecutorAllocation()) {
            case FIXED:
                toReturn.put("spark.executor.instances", Integer.toString(getFixedExecutors()));
                toReturn.put("spark.dynamicAllocation.enabled", "false");
                break;
            case DYNAMIC:
                toReturn.put("spark.dynamicAllocation.minExecutors", Integer.toString(getDynamicExecutorsMin()));
                toReturn.put("spark.dynamicAllocation.maxExecutors", Integer.toString(getDynamicExecutorsMax()));
                toReturn.put("spark.dynamicAllocation.enabled", "true");
                break;
            default:
                // set nothing (use cluster defaults)
                break;
        }

        return toReturn;
    }
}
