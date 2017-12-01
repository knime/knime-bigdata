/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ------------------------------------------------------------------------
 */
package org.knime.bigdata.spark.core.preferences;

import org.apache.log4j.Logger;
import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.core.runtime.preferences.IEclipsePreferences;
import org.eclipse.core.runtime.preferences.InstanceScope;
import org.eclipse.jface.preference.IPreferenceStore;
import org.knime.bigdata.spark.core.SparkPlugin;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.osgi.service.prefs.BackingStoreException;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author Tobias Koetter, KNIME.com
 * @author Sascha Wolke, KNIME.com
 */
public class SparkPreferenceInitializer extends AbstractPreferenceInitializer {

    /**All supported log levels.*/
    public static final String ALL_LOG_LEVELS[] = new String[]{"DEBUG", "INFO", "WARN", "ERROR"};

    /** Preference key to determine whether Spark preferences have already been initialized */
    public static final String PREF_SPARK_PREFERENCES_INITIALIZED = "org.knime.bigdata.spark.v1_6.initialized";

    /** Preference key for a Spark jobserver URL. */
    public static final String PREF_JOB_SERVER_URL = "org.knime.bigdata.spark.v1_6.jobserver.connection.url";

    /** Preference key for enabling authentication (boolean). */
    public static final String PREF_AUTHENTICATION = "org.knime.bigdata.spark.v1_6.jobserver.connection.authentication";

    /** Preference key for a Spark jobserver username. */
    public static final String PREF_USER_NAME = "org.knime.bigdata.spark.v1_6.jobserver.connection.user";

    /**Preference key for the password.*/
    public static final String PREF_PWD = "org.knime.bigdata.spark.v1_6.jobserver.connection.pwd";

    /**Preference key for the context name.*/
    public static final String PREF_CONTEXT_NAME = "org.knime.bigdata.spark.v1_6.jobserver.context.name";

    /**Preference key for the Spark version.*/
    public static final String PREF_SPARK_VERSION = "org.knime.bigdata.spark.v1_6.jobserver.context.sparkVersion";

    /** Preference key for enabling custom spark settings. */
    public static final String PREF_OVERRIDE_SPARK_SETTINGS =
        "org.knime.bigdata.spark.v1_6.jobserver.context.overrideSparkSetting";
    /**Preference key for the custom Spark settings.*/
    public static final String PREF_CUSTOM_SPARK_SETTINGS =
        "org.knime.bigdata.spark.v1_6.jobserver.context.customSparkSetting";

    /** Preference key for client side job status polling in seconds (integer). */
    public static final String PREF_JOB_CHECK_FREQUENCY =
        "org.knime.bigdata.spark.v1_6.jobserver.context.jobCheckFrequency";

    /** Preference key for job timeout in seconds (integer). */
    public static final String PREF_JOB_TIMEOUT = "org.knime.bigdata.spark.v1_6.jobserver.context.jobTimeout";

    /**Preference key to indicate that objects should be deleted on dispose.*/
    public static final String PREF_DELETE_OBJECTS_ON_DISPOSE = "org.knime.bigdata.spark.v1_6.deleteObjectsOnDispose";

    /** Preference key for spark side log level (e.g. INFO, DEBUG...). */
    public static final String PREF_JOB_LOG_LEVEL = "org.knime.bigdata.spark.v1_6.jobLogLevel";

    /** Preference key for verbose logging on KNIME/client side. */
    public static final String PREF_VERBOSE_LOGGING = "org.knime.bigdata.spark.v1_6.verboseLogging";

    /** All context specific settings requiring context reset on change. */
    public static final String[] PREF_ALL_CONTEXT_SETTINGS = new String[]{PREF_SPARK_VERSION, PREF_CONTEXT_NAME,
        PREF_DELETE_OBJECTS_ON_DISPOSE, PREF_JOB_LOG_LEVEL, PREF_OVERRIDE_SPARK_SETTINGS, PREF_CUSTOM_SPARK_SETTINGS};


    private static final String LEGACY_SPARK_1_2_PLUGIN = "org.knime.bigdata.spark";

    private static final String LEGACY_SPARK_1_3_PLUGIN = "org.knime.bigdata.spark1_3";

    /** @deprecated use PREF_JOB_SERVER_URL instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_SERVER = "org.knime.bigdata.spark.jobServer";

    /** @deprecated use PREF_JOB_SERVER_URL instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_SERVER_PORT = "org.knime.bigdata.spark.jobServer.port";

    /** @deprecated use PREF_JOB_SERVER_URL instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_SERVER_PROTOCOL = "org.knime.bigdata.spark.jobServer.protocol";

    /** @deprecated use PREF_CONTEXT_NAME instead */
    @Deprecated
    public static final String LEGACY_PREF_CONTEXT_NAME = "org.knime.bigdata.spark.context";

    /** @deprecated use PREF_DELETE_OBJECTS_ON_DISPOSE instead */
    @Deprecated
    public static final String LEGACY_PREF_DELETE_RDDS_ON_DISPOSE = "org.knime.bigdata.spark.deleteRDDsOnDispose";

    /** @deprecated use PREF_CUSTOM_SPARK_SETTINGS instead */
    @Deprecated
    public static final String LEGACY_PREF_MEM_PER_NODE = "org.knime.bigdata.spark.memperNode";

    /** @deprecated use PREF_USER_NAME instead */
    @Deprecated
    public static final String LEGACY_PREF_USER_NAME = "org.knime.bigdata.spark.user";

    /** @deprecated use PREF_USER_PWD instead */
    @Deprecated
    public static final String LEGACY_PREF_PWD = "org.knime.bigdata.spark.pwd";

    /** @deprecated use PREF_USER_JOB_TIMEOUT instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_TIMEOUT = "org.knime.bigdata.spark.jobTimeout";

    /** @deprecated use PREF_JOB_CHECK_FREQUENCY instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_CHECK_FREQUENCY = "org.knime.bigdata.spark.jobCheckFrequency";

    /** @deprecated use PREF_VERBOSE_LOGGING instead */
    @Deprecated
    public static final String LEGACY_PREF_VERBOSE_LOGGING = "org.knime.bigdata.spark.verboseLogging";

    @Override
    public void initializeDefaultPreferences() {
//        final IPreferenceStore store = SparkPlugin.getDefault().getPreferenceStore();
        final IPreferenceStore store = getPreferenceStore();

        loadDefaultValues(store);

        final boolean isAlreadyInitialized = store.getBoolean(PREF_SPARK_PREFERENCES_INITIALIZED);

        if (!isAlreadyInitialized) {
            try {
                if (hasPreferences(LEGACY_SPARK_1_3_PLUGIN)) {
                    initializeFromLegacySettings(store, LEGACY_SPARK_1_3_PLUGIN, SparkVersion.V_1_3);
                } else if (hasPreferences(LEGACY_SPARK_1_2_PLUGIN)) {
                    initializeFromLegacySettings(store, LEGACY_SPARK_1_2_PLUGIN, SparkVersion.V_1_2);
                } // otherwise use the defaults that are already set
                store.setValue(PREF_SPARK_PREFERENCES_INITIALIZED, true);
            } catch (Exception e) {
                Logger.getLogger(SparkPreferenceInitializer.class)
                    .error("Error when trying to import old KNIME Extension for Apache Spark preferences. Falling back to defaults.", e);
            }
        }
    }

    /**
     * @return the {@link IPreferenceStore} to use
     */
    static synchronized IPreferenceStore getPreferenceStore() {
        return SparkPlugin.getDefault().getPreferenceStore();
    }

    /** @return true if given plugin has at least one preference key set. */
    private boolean hasPreferences(final String oldPluginId) {
        IEclipsePreferences oldPrefs = InstanceScope.INSTANCE.getNode(oldPluginId);

        try {
            return oldPrefs != null && oldPrefs.keys().length > 0;
        } catch (BackingStoreException e) {
            return false;
        }
    }

    /**
     * Imports existing legacy preferences (from com.knime.bigdata.spark and .spark1_3) into the new format.
     *
     * @param store The store for the new preferences.
     * @param oldPluginId The legacy plugin name.
     */
    private void initializeFromLegacySettings(final IPreferenceStore store, final String oldPluginId, final SparkVersion sparkVersion) {

        IEclipsePreferences oldPrefs = InstanceScope.INSTANCE.getNode(oldPluginId);

        final String jobserverProtocol = oldPrefs.get(LEGACY_PREF_JOB_SERVER_PROTOCOL, "http");
        final String jobserverHost = oldPrefs.get(LEGACY_PREF_JOB_SERVER, "localhost");
        final int jobserverPort = oldPrefs.getInt(LEGACY_PREF_JOB_SERVER_PORT, 8090);

        final String user = oldPrefs.get(LEGACY_PREF_USER_NAME, "guest");
        final String password = oldPrefs.get(LEGACY_PREF_PWD, null);

        final String jobserverContextName = oldPrefs.get(LEGACY_PREF_CONTEXT_NAME, "knimeSparkContext");
        final String memPerNode = oldPrefs.get(LEGACY_PREF_MEM_PER_NODE, "512m");

        final int jobTimeout = oldPrefs.getInt(LEGACY_PREF_JOB_TIMEOUT, -1);
        final int jobCheckFreq = oldPrefs.getInt(LEGACY_PREF_JOB_CHECK_FREQUENCY, -1);

        final boolean deleteRDDsOnDispose = oldPrefs.getBoolean(LEGACY_PREF_DELETE_RDDS_ON_DISPOSE,
            store.getDefaultBoolean(PREF_DELETE_OBJECTS_ON_DISPOSE));
        final boolean verboseLogging =
            oldPrefs.getBoolean(LEGACY_PREF_VERBOSE_LOGGING, store.getDefaultBoolean(PREF_VERBOSE_LOGGING));

        String jobserverUrl = String.format("%s://%s:%d/", jobserverProtocol, jobserverHost, jobserverPort);
        if (!jobserverUrl.equals(store.getDefaultString(PREF_JOB_SERVER_URL))) {
            store.setValue(PREF_JOB_SERVER_URL, jobserverUrl);
        }

        if (user != null && !user.equals(store.getDefaultString(PREF_USER_NAME)) && password != null) {
            store.setValue(PREF_AUTHENTICATION, true);
            store.setValue(PREF_USER_NAME, user);
            store.setValue(PREF_PWD, password);
        }

        if (jobserverContextName != null && !jobserverContextName.equals(store.getDefaultString(PREF_CONTEXT_NAME))) {
            store.setValue(PREF_CONTEXT_NAME, jobserverContextName);
        }

        if (jobTimeout != -1 && jobTimeout != store.getDefaultInt(PREF_JOB_TIMEOUT)) {
            store.setValue(PREF_JOB_TIMEOUT, jobTimeout);
        }

        if (jobCheckFreq != -1 && jobCheckFreq != 5) {
            // jobCheckFreq=5 is the old default
            store.setValue(PREF_JOB_CHECK_FREQUENCY, jobCheckFreq);
        }

        if (memPerNode != null && !memPerNode.isEmpty() && !memPerNode.equals("512m") ) {
            // memPerNode=512m is the old default
            store.setValue(PREF_OVERRIDE_SPARK_SETTINGS, true);
            store.setValue(PREF_CUSTOM_SPARK_SETTINGS, "memory-per-node: " + memPerNode + "\n");
        }

        if (deleteRDDsOnDispose != store.getDefaultBoolean(PREF_DELETE_OBJECTS_ON_DISPOSE)) {
            store.setValue(PREF_DELETE_OBJECTS_ON_DISPOSE, deleteRDDsOnDispose);
        }

        store.setValue(PREF_VERBOSE_LOGGING, verboseLogging);
        store.setValue(PREF_SPARK_VERSION, sparkVersion.toString());
    }

    private void loadDefaultValues(final IPreferenceStore store) {
        final Config config = ConfigFactory.load(getClass().getClassLoader());

        // setup connection defaults
        store.setDefault(PREF_SPARK_PREFERENCES_INITIALIZED, false);
        store.setDefault(PREF_JOB_SERVER_URL, getPresetString(config, "jobserver.connection.url"));
        store.setDefault(PREF_AUTHENTICATION, getPresetBoolean(config, "jobserver.connection.authentication"));
        store.setDefault(PREF_USER_NAME, getPresetString(config, "jobserver.connection.userName"));
        store.setDefault(PREF_PWD, getPresetString(config, "jobserver.connection.password"));

        // setup jobserver context defaults
        store.setDefault(PREF_SPARK_VERSION, getPresetString(config, "jobserver.context.sparkVersion"));
        store.setDefault(PREF_CONTEXT_NAME, getPresetString(config, "jobserver.context.name"));
        store.setDefault(PREF_JOB_TIMEOUT, getPresetInt(config, "jobserver.context.jobTimeout"));
        store.setDefault(PREF_JOB_CHECK_FREQUENCY, getPresetInt(config, "jobserver.context.jobCheckFrequency"));
        store.setDefault(PREF_OVERRIDE_SPARK_SETTINGS, getPresetBoolean(config, "jobserver.context.overrideSettings"));
        store.setDefault(PREF_CUSTOM_SPARK_SETTINGS, getPresetString(config, "jobserver.context.customSettings"));

        // setup general KNIME Extension for Apache Spark settings
        store.setDefault(PREF_DELETE_OBJECTS_ON_DISPOSE, getPresetBoolean(config, "knime.deleteObjectsOnDispose"));
        store.setDefault(PREF_JOB_LOG_LEVEL, getPresetString(config, "knime.jobLogLevel"));
        store.setDefault(PREF_VERBOSE_LOGGING, getPresetBoolean(config, "knime.verboseLogging"));
    }

    private String getPresetString(final Config config, final String settingsPath) {
        if (!config.hasPath(settingsPath)) {
            throw new IllegalArgumentException("No default setting for: " + settingsPath);
        }
        return config.getString(settingsPath);
    }

    private String getPresetString(final Config config, final String settingsPath, final String defaultVal) {
        if (config.hasPath(settingsPath)) {
            return config.getString(settingsPath);
        }
        return defaultVal;
    }

    private final int getPresetInt(final Config config, final String settingsPath) {
        if (!config.hasPath(settingsPath)) {
            throw new IllegalArgumentException("No default setting for: " + settingsPath);
        }

        return config.getInt(settingsPath);
    }

    private final boolean getPresetBoolean(final Config config, final String settingsPath) {
        if (!config.hasPath(settingsPath)) {
            throw new IllegalArgumentException("No default setting for: " + settingsPath);
        }
        return config.getBoolean(settingsPath);
    }

    private final boolean getPresetBoolean(final Config config, final String settingsPath, final boolean defaultVal) {
        if (config.hasPath(settingsPath)) {
            return config.getBoolean(settingsPath);
        }
        return defaultVal;
    }
}
