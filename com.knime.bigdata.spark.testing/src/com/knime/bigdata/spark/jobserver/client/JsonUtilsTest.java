package com.knime.bigdata.spark.jobserver.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import javax.json.JsonArray;
import javax.json.JsonValue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.jobs.KMeansLearnerJob;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class JsonUtilsTest {

    /**
     *
     * @throws Throwable
     */
    @Test
    public void utilShouldConvertSimplePropertyValueString2JSon() throws Throwable {
        final String jsonStr = JsonUtils.asJson(new String[]{"key", "value"});
        final Config config = ConfigFactory.parseString(jsonStr);
        assertTrue("key should be stored as path", config.hasPath("key"));
        assertEquals("value should be accessible for key", config.getString("key"), "value");
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void utilShouldConvertArrayOfPropertyValueArrays2JSon() throws Throwable {
        final String jsonStr =
            JsonUtils.asJson(new Object[]{
                "key",
                JsonUtils.toJsonArray(new Object[]{new String[]{"k1", "v1", "k2", "v2"},
                    new String[]{"k21", "v21", "k22", "v22"},
                    new String[]{"k31", "v31", "k32", "v32", "kk33", "val456"}})});
        final Config config = ConfigFactory.parseString(jsonStr);
        assertTrue("key should be stored as path", config.hasPath("key"));
        final ConfigList subConfig = config.getList("key");
        assertEquals("unexpected number of sub-tasks", 3, subConfig.size());

        assertEquals("value v2 should be accessible for key k2", "v2", ((ConfigObject)subConfig.get(0)).toConfig()
            .getString("k2"));
        assertEquals("value val456 should be accessible for key kk33", "val456", ((ConfigObject)subConfig.get(2))
            .toConfig().getString("kk33"));
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void utilShouldConvertArray2JSon() throws Throwable {
        final String jsonStr = JsonUtils.toJsonArray(new Object[]{"a", 1, null, "bkx", false});

        final JsonArray jsonArray = JsonUtils.toJsonArray(jsonStr);

        assertEquals("json array should have 5 element", 5, jsonArray.size());
        assertEquals("null should be kept", JsonValue.NULL, jsonArray.get(2));
        
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void utilShouldConvertInputOutputGroups2JSon() throws Throwable {
        final String jsonStr =
            JsonUtils.asJson(new Object[]{
                ParameterConstants.PARAM_INPUT,
                new String[]{KnimeSparkJob.PARAM_INPUT_TABLE, "dataPath", KMeansLearnerJob.PARAM_NUM_CLUSTERS, "9",
                    ParameterConstants.PARAM_NUM_ITERATIONS, "63"},
                ParameterConstants.PARAM_OUTPUT,
                new String[]{ParameterConstants.PARAM_MODEL_NAME, "modelFile", KnimeSparkJob.PARAM_RESULT_TABLE,
                    "outputDataPath"}

            });
        final Config config = ConfigFactory.parseString(jsonStr);
        assertTrue("input should be stored as path", config.hasPath(ParameterConstants.PARAM_INPUT));
        assertTrue("output should be stored as path", config.hasPath(ParameterConstants.PARAM_OUTPUT));
        final Config inputConfig = config.getConfig(ParameterConstants.PARAM_INPUT);
        assertEquals("value should be accessible for key", "dataPath",
            inputConfig.getString(KnimeSparkJob.PARAM_INPUT_TABLE));
        assertEquals("value should be accessible for key", 9, inputConfig.getInt(KMeansLearnerJob.PARAM_NUM_CLUSTERS));
        assertEquals("value should be accessible for key", 63,
            inputConfig.getInt(ParameterConstants.PARAM_NUM_ITERATIONS));

        final Config outputConfig = config.getConfig(ParameterConstants.PARAM_OUTPUT);
        assertEquals("value should be accessible for key", "outputDataPath",
            outputConfig.getString(KnimeSparkJob.PARAM_RESULT_TABLE));
        assertEquals("value should be accessible for key", "modelFile",
            outputConfig.getString(ParameterConstants.PARAM_MODEL_NAME));
    }

 
}