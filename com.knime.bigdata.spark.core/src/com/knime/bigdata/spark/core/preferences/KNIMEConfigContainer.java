package com.knime.bigdata.spark.core.preferences;

import org.eclipse.jface.preference.IPreferenceStore;

import com.knime.bigdata.spark.core.SparkPlugin;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class KNIMEConfigContainer {

	private static IPreferenceStore PREFERENCE_STORE = SparkPlugin.getDefault().getPreferenceStore();

    public static String getJobServerUrl() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_JOB_SERVER_URL);
    }

    public static boolean useAuthentication() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION);
    }

    public static String getUserName() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_USER_NAME);
    }

    public static String getPassword() {
        String pwd = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_PWD);
        if (pwd == null || pwd.trim().isEmpty()) {
            return null;
        }
        return pwd;
    }

    public static int getJobTimeout() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_JOB_TIMEOUT);
    }

    public static int getJobCheckFrequency() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY);
    }

    public static SparkVersion getSparkVersion() {
        String stringVersion = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_SPARK_VERSION);
        if (stringVersion.isEmpty()) {
            return SparkVersion.V_1_2;
        }
        return SparkVersion.getVersion(stringVersion);
    }

    public static String getSparkContext() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_CONTEXT_NAME);
    }

    public static boolean deleteSparkObjectsOnDispose() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_DELETE_OBJECTS_ON_DISPOSE);
    }

    public static String getSparkJobLogLevel() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_JOB_LOG_LEVEL);
    }

    public static boolean overrideSparkSettings() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS);
    }

    public static String getCustomSparkSettings() {
        final String settings = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_CUSTOM_SPARK_SETTINGS);
        if (settings.matches("memory-per-node: 512m\\s+") && !overrideSparkSettings()) {
            //these are the old default settings from the application.conf file prior KNIME 3.4
            //so overwrite them with the new default settings
            return "memory-per-node: 1G\nspark.executor.cores: 2\nspark.executor.instances: 3\n";
        }
        return settings;
    }



    public static boolean verboseLogging() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_VERBOSE_LOGGING);
    }
}
