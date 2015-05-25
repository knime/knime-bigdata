package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
public class JsonUtilsTest {

	@Test
	public void utilShouldConvertSimplePropertyValueString2JSon()
			throws Throwable {
		String jsonStr = JsonUtils.asJson(new String[] { "key", "value" });
		Config config = ConfigFactory.parseString(jsonStr);
		assertTrue("key should be stored as path", config.hasPath("key"));
		assertEquals("value should be accessible for key",
				config.getString("key"), "value");
	}

//	@Test
//	public void utilShouldConvertArrayOfPropertyValueArrays2JSon()
//			throws Throwable {
//		String jsonStr = JsonUtils.asJson(new Object[] { "key",
//				JsonUtils.toJsonArray(new String[] {"k1", "v1", "k2", "v2"}, new String[] {"k21", "v21", "k22", "v22"},
//				new String[] {"k31", "v31", "k32", "v32", "kk33", "val456"})} );
//		Config config = ConfigFactory.parseString(jsonStr);
//		assertTrue("key should be stored as path", config.hasPath("key"));
//		ConfigList subConfig = config.getList("key");
//		assertEquals("unexpected number of sub-tasks", 3,subConfig.size());
//
//		assertEquals("value v2 should be accessible for key k2", "v2",
//				((ConfigObject) subConfig.get(0)).toConfig().getString("k2"));
//		assertEquals("value val456 should be accessible for key kk33", "val456",
//				((ConfigObject) subConfig.get(2)).toConfig().getString("kk33"));
//	}

	@Test
	public void utilShouldConvertInputOutputGroups2JSon() throws Throwable {
		String jsonStr = JsonUtils.asJson(new Object[] {
				ParameterConstants.PARAM_INPUT,
				new String[] { ParameterConstants.PARAM_DATA_PATH, "dataPath",
						ParameterConstants.PARAM_NUM_CLUSTERS, "9",
						ParameterConstants.PARAM_NUM_ITERATIONS, "63" },
				ParameterConstants.PARAM_OUTPUT,
				new String[] { ParameterConstants.PARAM_MODEL_NAME,
						"modelFile", ParameterConstants.PARAM_DATA_PATH,
						"outputDataPath" }

		});
		Config config = ConfigFactory.parseString(jsonStr);
		assertTrue("input should be stored as path",
				config.hasPath(ParameterConstants.PARAM_INPUT));
		assertTrue("output should be stored as path",
				config.hasPath(ParameterConstants.PARAM_OUTPUT));
		Config inputConfig = config.getConfig(ParameterConstants.PARAM_INPUT);
		assertEquals("value should be accessible for key", "dataPath",
				inputConfig.getString("dataPath"));
		assertEquals("value should be accessible for key", 9,
				inputConfig.getInt(ParameterConstants.PARAM_NUM_CLUSTERS));
		assertEquals("value should be accessible for key", 63,
				inputConfig.getInt(ParameterConstants.PARAM_NUM_ITERATIONS));

		Config outputConfig = config.getConfig(ParameterConstants.PARAM_OUTPUT);
		assertEquals("value should be accessible for key", "outputDataPath",
				outputConfig.getString(ParameterConstants.PARAM_DATA_PATH));
		assertEquals("value should be accessible for key", "modelFile",
				outputConfig.getString(ParameterConstants.PARAM_MODEL_NAME));
	}

}