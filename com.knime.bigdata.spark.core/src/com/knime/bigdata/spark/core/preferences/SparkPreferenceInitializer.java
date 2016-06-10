/*
 * ------------------------------------------------------------------------
 *  Copyright by KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
package com.knime.bigdata.spark.core.preferences;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.jface.preference.IPreferenceStore;

import com.knime.bigdata.spark.core.SparkPlugin;
import com.knime.bigdata.spark.core.version.SparkVersion;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @author Tobias Koetter, University of Konstanz
 * @author Sascha Wolke, KNIME.com
 */
public class SparkPreferenceInitializer extends
        AbstractPreferenceInitializer {

    public static final String ALL_LOG_LEVELS[] = new String[] { "DEBUG", "INFO", "WARN", "ERROR" };


    /** Preference key for a Spark jobserver URL. */
    public static final String PREF_JOB_SERVER_URL = "com.knime.bigdata.spark.v1_6.jobserver.connection.url";

    /** Preference key for enabling authentication (boolean). */
    public static final String PREF_AUTHENTICATION = "com.knime.bigdata.spark.v1_6.jobserver.connection.authentication";

    /** Preference key for a Spark jobserver username. */
    public static final String PREF_USER_NAME = "com.knime.bigdata.spark.v1_6.jobserver.connection.user";

    public static final String PREF_PWD = "com.knime.bigdata.spark.v1_6.jobserver.connection.pwd";

    public static final String PREF_CONTEXT_NAME = "com.knime.bigdata.spark.v1_6.jobserver.context.name";

    public static final String PREF_SPARK_VERSION = "com.knime.bigdata.spark.v1_6.jobserver.context.sparkVersion";

    /** Preference key for enabling custom spark settings. */
    public static final String PREF_OVERRIDE_SPARK_SETTINGS = "com.knime.bigdata.spark.v1_6.jobserver.context.overrideSparkSetting";

    public static final String PREF_CUSTOM_SPARK_SETTINGS = "com.knime.bigdata.spark.v1_6.jobserver.context.customSparkSetting";

    /** Preference key for client side job status polling in seconds (integer). */
    public static final String PREF_JOB_CHECK_FREQUENCY = "com.knime.bigdata.spark.v1_6.jobserver.context.jobCheckFrequency";

    /** Preference key for job timeout in seconds (integer). */
    public static final String PREF_JOB_TIMEOUT = "com.knime.bigdata.spark.v1_6.jobserver.context.jobTimeout";

    public static final String PREF_DELETE_OBJECTS_ON_DISPOSE = "com.knime.bigdata.spark.v1_6.deleteObjectsOnDispose";

    /** Preference key for spark side log level (e.g. INFO, DEBUG...). */
    public static final String PREF_JOB_LOG_LEVEL = "com.knime.bigdata.spark.v1_6.jobLogLevel";

    /** Preference key for verbose logging on KNIME/client side. */
    public static final String PREF_VERBOSE_LOGGING = "com.knime.bigdata.spark.v1_6.verboseLogging";


    /** All context specific settings requiring context reset on change. */
    public static final String[] PREF_ALL_CONTEXT_SETTINGS = new String[] {
        PREF_SPARK_VERSION, PREF_CONTEXT_NAME, PREF_DELETE_OBJECTS_ON_DISPOSE,
        PREF_JOB_LOG_LEVEL, PREF_OVERRIDE_SPARK_SETTINGS, PREF_CUSTOM_SPARK_SETTINGS
    };

    /** @deprecated use PREF_JOB_SERVER_URL instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_SERVER = "com.knime.bigdata.spark.jobServer";
    /** @deprecated use PREF_JOB_SERVER_URL instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_SERVER_PORT = "com.knime.bigdata.spark.jobServer.port";
    /** @deprecated use PREF_JOB_SERVER_URL instead */
    @Deprecated
    public static final String LEGACY_PREF_JOB_SERVER_PROTOCOL = "com.knime.bigdata.spark.jobServer.protocol";
    /** @deprecated use PREF_DELETE_OBJECTS_ON_DISPOSE instead */
    @Deprecated
    public static final String LEGACY_PREF_DELETE_RDDS_ON_DISPOSE = "com.knime.bigdata.spark.deleteRDDsOnDispose";
    /** @deprecated use PREF_CUSTOM_SPARK_SETTINGS instead */
    @Deprecated
    public static final String LEGACY_PREF_MEM_PER_NODE = "com.knime.bigdata.spark.memperNode";


    @Override
    public void initializeDefaultPreferences() {
        final IPreferenceStore store = SparkPlugin.getDefault().getPreferenceStore();
        final Config config = ConfigFactory.load();

        // fallback on old protocol/server/port settings if present
        if (store.contains(LEGACY_PREF_JOB_SERVER_PROTOCOL) && store.contains(LEGACY_PREF_JOB_SERVER)) {
            String defaultUrl = store.getString(LEGACY_PREF_JOB_SERVER_PROTOCOL) + "://" + store.getString(LEGACY_PREF_JOB_SERVER);
            if (store.contains(LEGACY_PREF_JOB_SERVER_PORT)) {
                defaultUrl += ":" + store.getString(LEGACY_PREF_JOB_SERVER_PORT);
                store.setToDefault(LEGACY_PREF_JOB_SERVER_PORT);
            }
            store.setValue(PREF_JOB_SERVER_URL, defaultUrl);
            store.setToDefault(LEGACY_PREF_JOB_SERVER_PROTOCOL);
            store.setToDefault(LEGACY_PREF_JOB_SERVER);
        }

        // fallback on renamed delete objects on dispose
        if (store.contains(LEGACY_PREF_DELETE_RDDS_ON_DISPOSE)) {
            store.setValue(PREF_DELETE_OBJECTS_ON_DISPOSE, store.getString(LEGACY_PREF_DELETE_RDDS_ON_DISPOSE));
            store.setToDefault(LEGACY_PREF_DELETE_RDDS_ON_DISPOSE);
        }

        // fallback on memory per node settings
        if (store.contains(LEGACY_PREF_MEM_PER_NODE)) {
            store.setValue(PREF_OVERRIDE_SPARK_SETTINGS, true);
            store.setValue(PREF_CUSTOM_SPARK_SETTINGS, "memory-per-node: " + store.getInt(LEGACY_PREF_MEM_PER_NODE) + "\n");
            store.setToDefault(LEGACY_PREF_MEM_PER_NODE);
        }

        // setup connection defaults
        store.setDefault(PREF_JOB_SERVER_URL, getPresetString(config, "jobserver.connection.url", "http://localhost:8090"));
        store.setDefault(PREF_AUTHENTICATION, getPresetBoolean(config, "jobserver.connection.authentication", false));
        store.setDefault(PREF_USER_NAME, getPresetString(config, "jobserver.connection.userName", "guest"));
        store.setDefault(PREF_PWD, getPresetString(config, "jobserver.connection.password"));

        // setup jobserver context defaults
        store.setDefault(PREF_SPARK_VERSION, getPresetString(config, "jobserver.context.sparkVersion", SparkVersion.V_1_2.getLabel()));
        store.setDefault(PREF_CONTEXT_NAME, getPresetString(config, "jobserver.context.contextName", "knime"));
        store.setDefault(PREF_JOB_TIMEOUT, getPresetInt(config, "jobserver.context.jobTimeout", 7200));
        store.setDefault(PREF_JOB_CHECK_FREQUENCY, getPresetInt(config, "jobserver.context.jobCheckFrequency", 5));
        store.setDefault(PREF_OVERRIDE_SPARK_SETTINGS, getPresetBoolean(config, "jobserver.context.overrideSettings", false));
        store.setDefault(PREF_CUSTOM_SPARK_SETTINGS, getPresetString(config, "jobserver.context.customSettings"));

        // setup general KNIME Spark Executor settings
        store.setDefault(PREF_DELETE_OBJECTS_ON_DISPOSE, getPresetBoolean(config, "knime.deleteObjectsOnDispose", true));
        store.setDefault(PREF_JOB_LOG_LEVEL, getPresetString(config, "knime.jobLogLevel", "WARN"));
        store.setDefault(PREF_VERBOSE_LOGGING, getPresetBoolean(config, "knime.verboseLogging", false));
    }

    private String getPresetString(final Config config, final String string) {
        return getPresetString(config, string, "");
    }

    private String getPresetString(final Config config, final String string, final String defaultVal) {
        if (config.hasPath(string)) {
            return config.getString(string);
        }
        return defaultVal;
    }

    private final int getPresetInt(final Config config, final String string, final int defaultVal) {
        if (config.hasPath(string)) {
            return config.getInt(string);
        }
        return defaultVal;
    }

    private final boolean getPresetBoolean(final Config config, final String string, final boolean defaultVal) {
        if (config.hasPath(string)) {
            return config.getBoolean(string);
        }
        return defaultVal;
    }
}
