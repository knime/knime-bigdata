package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.GradientType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.UpdaterType;
import com.knime.bigdata.spark.node.mllib.prediction.logisticregression.LogisticRegressionTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class LogisticRegressionJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(null,
				new Integer[] { 0, 1 }, 1, 101, 0.2d, true);
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_COL_IDXS_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in", null, 1,
				101, 0.2d, true);
		myCheck(params, ParameterConstants.PARAM_COL_IDXS, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_LABEL_INDEX_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, null, 101, 0.2d, true);
		myCheck(params, ParameterConstants.PARAM_LABEL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_REGULARIZATION_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, 4, null, true);
		myCheck(params, LogisticRegressionJob.PARAM_REGULARIZATION, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_NUM_ITERATIONS_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, null, 0.2d, false, 77, 0.5d);
		myCheck(params, LogisticRegressionJob.PARAM_NUM_ITERATIONS, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_USE_SGD_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, 4, 0.2d, null);
		myCheck(params, LogisticRegressionJob.PARAM_USE_SGD, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_NUM_CORRECTIONS_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, 4, 0.2d, false, null, 0.4d);
		myCheck(params, LogisticRegressionJob.PARAM_NUM_CORRECTIONS, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_TOLERANCE_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, 4, 0.2d, false, 10, null);
		myCheck(params, LogisticRegressionJob.PARAM_TOLERANCE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_UPDATER_TYPE_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(null, true,
				false, true, GradientType.LeastSquaresGradient, 0.6, 0.9);
		myCheck(params, LogisticRegressionJob.PARAM_UPDATER_TYPE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_VALIDATE_DATA_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(
				UpdaterType.L1Updater, null, false, true,
				GradientType.LeastSquaresGradient, 0.6, 0.9);
		myCheck(params, LogisticRegressionJob.PARAM_VALIDATE_DATA, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_ADD_INTERCEPT_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(
				UpdaterType.L1Updater, true, null, true,
				GradientType.LeastSquaresGradient, 0.6, 0.9);
		myCheck(params, LogisticRegressionJob.PARAM_ADD_INTERCEPT, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_USE_FEATURE_SCALING_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(
				UpdaterType.L1Updater, true, false, null,
				GradientType.LeastSquaresGradient, 0.6, 0.9);
		myCheck(params, LogisticRegressionJob.PARAM_USE_FEATURE_SCALING,
				"Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_GRADIENT_TYPE_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(
				UpdaterType.L1Updater, true, false, true,
				null, 0.6, 0.9);
		myCheck(params, LogisticRegressionJob.PARAM_GRADIENT_TYPE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_STEP_SIZE_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(
				UpdaterType.L1Updater, true, false, true,
				GradientType.LeastSquaresGradient, null, 0.9);
		myCheck(params, LogisticRegressionJob.PARAM_STEP_SIZE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_FRACTION_Parameter()
			throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason(
				UpdaterType.L1Updater, true, false, true,
				GradientType.LeastSquaresGradient, 0.6, null);
		myCheck(params, LogisticRegressionJob.PARAM_FRACTION, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = LogisticRegressionTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, 25, 0.2d, true);
		KnimeSparkJob testObj = new LogisticRegressionJob();
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
		KnimeSparkJob testObj = new LogisticRegressionJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}