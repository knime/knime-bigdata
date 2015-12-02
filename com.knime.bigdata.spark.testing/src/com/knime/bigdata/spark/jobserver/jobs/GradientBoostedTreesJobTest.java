package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class GradientBoostedTreesJobTest extends LocalSparkSpec {

	// not tested since optional: PARAM_NO_OF_CLASSES,
	// PARAM_NOMINAL_FEATURE_INFO
	final static EnumContainer.EnsembleLossesType loss = EnumContainer.EnsembleLossesType.LogLoss;

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = GradientBoostedTreesTaskTest.paramsAsJason(null,
				new Integer[] { 0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25,
				101, 0.9d, true, loss);
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_COL_IDXS_Parameter()
			throws Throwable {
		String params = GradientBoostedTreesTaskTest.paramsAsJason("in", null,
				new NominalFeatureInfo(), 1, 2l, 4, 25, 101, 0.9d, true, loss);
		myCheck(params, ParameterConstants.PARAM_COL_IDXS, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_LABEL_INDEX_Parameter()
			throws Throwable {
		String params = GradientBoostedTreesTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, new NominalFeatureInfo(), null, 2l, 4,
				25, 101, 0.9d, true, loss);
		myCheck(params, ParameterConstants.PARAM_LABEL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_LOSS_FUNCTION_Parameter()
			throws Throwable {
		String params = GradientBoostedTreesTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, new NominalFeatureInfo(), 1, 2l, 4,
				25, 101, 0.9d, true, null);
		myCheck(params, GradientBoostedTreesLearnerJob.PARAM_LOSS_FUNCTION, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = GradientBoostedTreesTaskTest.paramsAsJason("in",
				new Integer[] { 0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25,
				101, 0.9d, true, loss);
		KnimeSparkJob testObj = new GradientBoostedTreesLearnerJob();
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
		KnimeSparkJob testObj = new GradientBoostedTreesLearnerJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}