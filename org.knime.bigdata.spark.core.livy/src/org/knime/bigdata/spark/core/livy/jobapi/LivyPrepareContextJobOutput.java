package org.knime.bigdata.spark.core.livy.jobapi;

import java.util.Map;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

@SparkClass
public class LivyPrepareContextJobOutput extends JobOutput {

    private static final String KEY_SPARK_WEB_UI = "sparkWebUI";

    private static final String KEY_SPARK_CONF = "sparkConf";

    private static final String KEY_SYS_PROPS = "systemProperties";

    private static final String KEY_TESTFILE_NAME = "testfileName";

    private static final String KEY_ADAPTIVE_EXECUTION_ENABLED = "adaptiveExecutionEnabled";

    /**
     * Empty constructor for (de)serialization.
     */
    public LivyPrepareContextJobOutput() {
    }

    /**
     * Spark 2.x and below constructor without adaptive execution support.
     *
     * @param sparkWebUI URL of spark web UI
     * @param sparkConf map with spark configuration
     * @param sysProps map with system properties
     * @param testfileName name of test file in staging area
     */
    public LivyPrepareContextJobOutput(String sparkWebUI, final Map<String, String> sparkConf,
        final Map<String, String> sysProps, String testfileName) {

        set(KEY_SPARK_WEB_UI, sparkWebUI);
        set(KEY_SPARK_CONF, sparkConf);
        set(KEY_SYS_PROPS, sysProps);
        set(KEY_TESTFILE_NAME, testfileName);
        set(KEY_ADAPTIVE_EXECUTION_ENABLED, false);
    }

    /**
     * Spark 3.x and above constructor with adaptive execution support.
     *
     * @param sparkWebUI URL of spark web UI
     * @param sparkConf map with spark configuration
     * @param sysProps map with system properties
     * @param testfileName name of test file in staging area
     * @param adaptiveExecutionEnabled {@code true} if adaptive query execution is enabled
     */
    public LivyPrepareContextJobOutput(String sparkWebUI, final Map<String, String> sparkConf,
        final Map<String, String> sysProps, String testfileName, boolean adaptiveExecutionEnabled) {

        set(KEY_SPARK_WEB_UI, sparkWebUI);
        set(KEY_SPARK_CONF, sparkConf);
        set(KEY_SYS_PROPS, sysProps);
        set(KEY_TESTFILE_NAME, testfileName);
        set(KEY_ADAPTIVE_EXECUTION_ENABLED, adaptiveExecutionEnabled);
    }

    /**
     * @return URL of spark web UI
     */
    public String getSparkWebUI() {
        return get(KEY_SPARK_WEB_UI);
    }

    /**
     * @return map with spark configuration
     */
    public Map<String, String> getSparkConf() {
        return get(KEY_SPARK_CONF);
    }

    /**
     * @return sparkConf map with system properties
     */
    public Map<String, String> getSystemProperties() {
        return get(KEY_SYS_PROPS);
    }

    /**
     * @return name of test file in staging area
     */
    public String getTestfileName() {
        return get(KEY_TESTFILE_NAME);
    }

    /**
     * @return {@code true} if adaptive query execution is enabled
     */
    public boolean adaptiveExecutionEnabled() {
        return get(KEY_ADAPTIVE_EXECUTION_ENABLED);
    }
}
