package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.sorter.SortTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SortJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = SortTaskTest.paramsAsJason(null,
				new Integer[] { 0, 1 }, new Boolean[] { false, false }, "out");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckColumnSelectionParameter()
			throws Throwable {
		String params = SortTaskTest.paramsAsJason("tab1", null, new Boolean[] { false, false }, "out");
		myCheck(params, "Input parameter '" + ParameterConstants.PARAM_COL_IDXS
				+ "' is not of expected type 'integer list'.");
	}

	@Test(expected = Exception.class)
	public void jobValidationShouldCheckSortOrderParameter()
			throws Throwable {
		String params = SortTaskTest.paramsAsJason("tab1", new Integer[] { 0, 1 }, null, "out");
		myCheck(params, "Input parameter '" + SortJob.PARAM_SORT_IS_ASCENDING
				+ "' is not of expected type 'integer list'.");
	}

	@Test
	public void jobValidationShouldCheckMissingOutputDataParameter()
			throws Throwable {

		String params = SortTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, new Boolean[] { false, false }, null);
		myCheck(params, KnimeSparkJob.PARAM_RESULT_TABLE, "Output");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = SortTaskTest.paramsAsJason("tab1",
				new Integer[] { 0, 1 }, new Boolean[] { false, false }, "out");
		KnimeSparkJob testObj = new SortJob();
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
		KnimeSparkJob testObj = new SortJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}