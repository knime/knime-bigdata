package org.knime.bigdata.spark.core.livy.jobapi;

import java.util.Map;

import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.SparkClass;

@SparkClass
public class LivyPrepareContextJobOutput extends JobOutput {

    private static final String KEY_SPARK_WEB_UI = "sparkWebUI";

    private static final String KEY_SPARK_CONF = "sparkConf";

    private static final String KEY_TESTFILE_NAME = "testfileName";

    /**
     * Empty constructor for (de)serialization.
     */
    public LivyPrepareContextJobOutput() {
    }

    public LivyPrepareContextJobOutput(String sparkWebUI, final Map<String, String> sparkConf, String testfileName) {
        set(KEY_SPARK_WEB_UI, sparkWebUI);
        set(KEY_SPARK_CONF, sparkConf);
        set(KEY_TESTFILE_NAME, testfileName);
    }

    public String getSparkWebUI() {
        return get(KEY_SPARK_WEB_UI);
    }

    public Map<String, String> getSparkConf() {
        return get(KEY_SPARK_CONF);
    }

    public String getTestfileName() {
        return get(KEY_TESTFILE_NAME);
    }
}
