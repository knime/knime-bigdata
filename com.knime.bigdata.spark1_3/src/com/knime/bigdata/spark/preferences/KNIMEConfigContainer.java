package com.knime.bigdata.spark.preferences;

import org.eclipse.jface.preference.IPreferenceStore;

import com.knime.bigdata.spark.SparkPlugin;

/**
 * @author Tobias Koetter
 *
 */
public class KNIMEConfigContainer {

	private static IPreferenceStore PREFERENCE_STORE = SparkPlugin.getDefault().getPreferenceStore();

    /**
     * @return the default job server url
     */
    public static String getJobServer() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_JOB_SERVER);
    }

    /**
     * @return the default job server port
     */
    public static int getJobServerPort() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_JOB_SERVER_PORT);
    }

    /**
     * @return the default job server protocol
     */
    public static String getJobServerProtocol() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_JOB_SERVER_PROTOCOL);
    }

    /**
     * @return the default user name
     */
    public static String getUserName() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_USER_NAME);
    }

    /**
     * @return the default password
     */
    public static char[] getPassword() {
        String pwd = PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_PWD);
        if (pwd == null || pwd.trim().isEmpty()) {
            return null;
        }
        return pwd.toCharArray();
    }

    /**
     * @return the default Spark context name
     */
    public static String getSparkContext() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_CONTEXT_NAME);
    }

    /**
     * @return the default number of cores
     */
    public static int getNumOfCPUCores() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_NUM_CPU_CORES);
    }

    /**
     * @return the default memory per node
     */
    public static String getMemoryPerNode() {
        return PREFERENCE_STORE.getString(SparkPreferenceInitializer.PREF_MEM_PER_NODE);
    }

    /**
     * @return the default job timeout
     */
    public static int getJobTimeout() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_JOB_TIMEOUT);
    }

    /**
     * @return the default job timeout check frequency
     */
    public static int getJobCheckFrequency() {
        return PREFERENCE_STORE.getInt(SparkPreferenceInitializer.PREF_JOB_CHECK_FREQUENCY);
    }

    /**
     * @return the delete RDDS on dispose flag
     */
    public static boolean deleteRDDsOnDispose() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_DELETE_RDDS_ON_DISPOSE);
    }

    public static boolean validateRDDsPriorExecution() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_VALIDATE_RDDS);
    }

    /**
     * @return <code>true</code> if verbose logging is enabled
     */
    public static boolean verboseLogging() {
        return PREFERENCE_STORE.getBoolean(SparkPreferenceInitializer.PREF_VERBOSE_LOGGING);
    }
}
