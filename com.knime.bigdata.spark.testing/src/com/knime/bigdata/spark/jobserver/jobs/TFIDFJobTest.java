package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.tfidf.TFIDFTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class TFIDFJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = TFIDFTaskTest.paramsAsJason(null, 1, 23, 5, " ", "out");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}
	
	@Test
	public void jobValidationShouldCheckMissingOutputDataParameter()
			throws Throwable {
		String params = TFIDFTaskTest.paramsAsJason("in",1, 23, 5, " ",null);
		myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
	}

	@Test
	public void jobValidationShouldCheckMissing_COL_INDEX_Parameter()
			throws Throwable {
		String params = TFIDFTaskTest.paramsAsJason("in",null, 23, 5, " ","out");
		myCheck(params, TFIDFJob.PARAM_COL_INDEX, "Input");
	}

	//optional param
//	@Test
//	public void jobValidationShouldCheckMissing_PARAM_MAX_NUM_TERMS_Parameter()
//			throws Throwable {
//		String params = TFIDFTaskTest.paramsAsJason("in",1, null,23,  " ","out");
//		myCheck(params, TFIDFJob.PARAM_MAX_NUM_TERMS, "Input");
//	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_MIN_FREQUENCY_Parameter()
			throws Throwable {
		String params = TFIDFTaskTest.paramsAsJason("in",1, 5, null, " ","out");
		myCheck(params, TFIDFJob.PARAM_MIN_FREQUENCY, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_SEPARATOR_Parameter()
			throws Throwable {
		String params = TFIDFTaskTest.paramsAsJason("in",1, 23, 5, null,"out");
		myCheck(params, TFIDFJob.PARAM_SEPARATOR, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = TFIDFTaskTest.paramsAsJason("in",1, 23, 5, " ","out");
		KnimeSparkJob testObj = new TFIDFJob();
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
		KnimeSparkJob testObj = new TFIDFJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}