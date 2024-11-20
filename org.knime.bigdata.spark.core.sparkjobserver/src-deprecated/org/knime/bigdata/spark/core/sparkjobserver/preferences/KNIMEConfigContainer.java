package org.knime.bigdata.spark.core.sparkjobserver.preferences;

import java.time.Duration;
import java.util.Map;

import org.knime.bigdata.spark.core.preferences.SparkPreferenceValidator;
import org.knime.bigdata.spark.core.version.SparkVersion;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * Container with defaults from {@code conf/application.conf}.
 *
 * @author Tobias Koetter, KNIME.com
 */
public class KNIMEConfigContainer {

	private static final Config CONFIG = ConfigFactory.load(KNIMEConfigContainer.class.getClassLoader());


    /**
     * @return Spark job server url
     */
    public static String getJobServerUrl() {
        return getPresetString(CONFIG, "jobserver.connection.url");
    }

    /**
     * @return <code>true</code> if authentication should be used
     */
    public static boolean useAuthentication() {
        return getPresetBoolean(CONFIG, "jobserver.connection.authentication");
    }

    /**
     * @return login user name
     */
    public static String getUserName() {
        return getPresetString(CONFIG, "jobserver.connection.userName");
    }

    /**
     * @return login password
     */
    public static String getPassword() {
        String pwd = getPresetString(CONFIG, "jobserver.connection.password");
        if (pwd == null || pwd.trim().isEmpty()) {
            return null;
        }
        return pwd;
    }

    /**
     * @return the job server REST receive timeout
     */
    public static Duration getReceiveTimeout() {
        return Duration.ofSeconds(getPresetInt(CONFIG, "jobserver.connection.receiveTimeout"));
    }

    /**
     * @return the frequency the job status should be checked
     */
    public static int getJobCheckFrequency() {
        return getPresetInt(CONFIG, "jobserver.context.jobCheckFrequency");
    }

    /**
     * @return the Spark version
     */
    public static SparkVersion getSparkVersion() {
        String stringVersion = getPresetString(CONFIG, "jobserver.context.sparkVersion");
        if (stringVersion.isEmpty()) {
            return SparkVersion.V_1_2;
        }
        return SparkVersion.fromString(stringVersion);
    }

    /**
     * @return the name of the Spark context
     */
    public static String getSparkContext() {
        return getPresetString(CONFIG, "jobserver.context.name");
    }

    /**
     * @return <code>true</code> if Spark objects should be destroyed on dispose
     */
    public static boolean deleteSparkObjectsOnDispose() {
        return getPresetBoolean(CONFIG, "knime.deleteObjectsOnDispose");
    }

    /**
     * @return <code>true</code> if the user has specified Spark settings
     * @see #getCustomSparkSettings()
     */
    public static boolean overrideSparkSettings() {
        return getPresetBoolean(CONFIG, "jobserver.context.overrideSettings");
    }

    /**
     * @return the default custom Spark settings as a map
     * @see #overrideSparkSettings()
     */
    public static Map<String,String> getCustomSparkSettings() {
        return SparkPreferenceValidator.parseSettingsString(getCustomSparkSettingsString());
    }

    /**
     * @return the default custom Spark settings as a String
     * @see #overrideSparkSettings()
     */
    public static String getCustomSparkSettingsString() {
        String settingsString = getPresetString(CONFIG, "jobserver.context.customSettings");
        if (settingsString.matches("memory-per-node: 512m\\s+") && !overrideSparkSettings()) {
            //these are the old default settings from the application.conf file prior KNIME 3.4
            //so overwrite them with the new default settings
            settingsString = "memory-per-node: 1G\nspark.executor.cores: 2\nspark.executor.instances: 3\n";
        }
        return settingsString;
    }

    private static String getPresetString(final Config config, final String settingsPath) {
        if (!config.hasPath(settingsPath)) {
            throw new IllegalArgumentException("No default setting for: " + settingsPath);
        }
        return config.getString(settingsPath);
    }

    private static String getPresetString(final Config config, final String settingsPath, final String defaultVal) {
        if (config.hasPath(settingsPath)) {
            return config.getString(settingsPath);
        }
        return defaultVal;
    }

    private static int getPresetInt(final Config config, final String settingsPath) {
        if (!config.hasPath(settingsPath)) {
            throw new IllegalArgumentException("No default setting for: " + settingsPath);
        }

        return config.getInt(settingsPath);
    }

    private static boolean getPresetBoolean(final Config config, final String settingsPath) {
        if (!config.hasPath(settingsPath)) {
            throw new IllegalArgumentException("No default setting for: " + settingsPath);
        }
        return config.getBoolean(settingsPath);
    }

    private static boolean getPresetBoolean(final Config config, final String settingsPath, final boolean defaultVal) {
        if (config.hasPath(settingsPath)) {
            return config.getBoolean(settingsPath);
        }
        return defaultVal;
    }
}
