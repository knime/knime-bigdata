package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.ConcatenateRDDsTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ConcatenateRDDsJobTest extends LocalSparkSpec {

	// we don't really care which exception is thrown as long as some exception is thrown
	@Test(expected = Exception.class)
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = ConcatenateRDDsTaskTest.paramsAsJason(null, "out");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckTooFewInputTablesParameter()
			throws Throwable {
		String params = ConcatenateRDDsTaskTest.paramsAsJason(new String[] {"in1"}, "out");
		myCheck(params, "Input parameter '" + KnimeSparkJob.PARAM_INPUT_TABLE + "' missing or too few tables (minimum 2 required).");
	}

	@Test
	public void jobValidationShouldCheckMissingOutputDataParameter() throws Throwable {
		
		String params = ConcatenateRDDsTaskTest.paramsAsJason(new String[] {"in1", "in2"}, null);
		myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = ConcatenateRDDsTaskTest.paramsAsJason(new String[] {"in1", "in2"}, "out");
		KnimeSparkJob testObj = new ConcatenateRDDsJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new ConcatenateRDDsJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}