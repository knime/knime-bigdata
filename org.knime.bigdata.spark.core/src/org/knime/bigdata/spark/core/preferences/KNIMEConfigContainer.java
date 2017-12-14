package org.knime.bigdata.spark.core.preferences;

import java.time.Duration;

import org.eclipse.jface.preference.IPreferenceStore;
import org.knime.bigdata.spark.core.version.SparkVersion;

/**
 * @author Tobias Koetter, KNIME.com
 */
public class KNIMEConfigContainer {

	private static IPreferenceStore PREFERENCE_STORE = SparkPreferenceInitializer.getPreferenceStore();

    /**
     * @return Spark job server url
     */
    public static String getJobServerUrl() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_JOB_SERVER_URL);
    }

    /**
     * @return <code>true</code> if authentication should be used
     */
    public static boolean useAuthentication() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_AUTHENTICATION);
    }

    /**
     * @return login user name
     */
    public static String getUserName() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_USER_NAME);
    }

    /**
     * @return login password
     */
    public static String getPassword() {
        String pwd = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_PWD);
        if (pwd == null || pwd.trim().isEmpty()) {
            return null;
        }
        return pwd;
    }

    /**
     * @return the job server REST receive timeout
     */
    public static Duration getReceiveTimeout() {
        return Duration.ofSeconds(PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_RECEIVE_TIMEOUT));
    }

    /**
     * @return the frequency the job status should be checked
     */
    public static int getJobCheckFrequency() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY);
    }

    /**
     * @return the Spark version
     */
    public static SparkVersion getSparkVersion() {
        String stringVersion = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_SPARK_VERSION);
        if (stringVersion.isEmpty()) {
            return SparkVersion.V_1_2;
        }
        return SparkVersion.fromString(stringVersion);
    }

    /**
     * @return the name of the Spark context
     */
    public static String getSparkContext() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_CONTEXT_NAME);
    }

    /**
     * @return <code>true</code> if Spark objects should be destroyed on dispose
     */
    public static boolean deleteSparkObjectsOnDispose() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_DELETE_OBJECTS_ON_DISPOSE);
    }

    /**
     * @return the logging level for Spark job log messages
     */
    public static String getSparkJobLogLevel() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_JOB_LOG_LEVEL);
    }

    /**
     * @return <code>true</code> if the user has specified Spark settings
     * @see #getCustomSparkSettings()
     */
    public static boolean overrideSparkSettings() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_OVERRIDE_SPARK_SETTINGS);
    }

    /**
     * @return the custom Spark settings
     * @see #overrideSparkSettings()
     */
    public static String getCustomSparkSettings() {
        final String settings = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_CUSTOM_SPARK_SETTINGS);
        if (settings.matches("memory-per-node: 512m\\s+") && !overrideSparkSettings()) {
            //these are the old default settings from the application.conf file prior KNIME 3.4
            //so overwrite them with the new default settings
            return "memory-per-node: 1G\nspark.executor.cores: 2\nspark.executor.instances: 3\n";
        }
        return settings;
    }

    /**
     * @return <code>true</code> if verbose logging should be enabled
     */
    public static boolean verboseLogging() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_VERBOSE_LOGGING);
    }
}
