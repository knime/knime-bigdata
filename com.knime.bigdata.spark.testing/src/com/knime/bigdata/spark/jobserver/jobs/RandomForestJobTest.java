package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.FeatureSubsetStrategies;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.randomforest.RandomForestTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class RandomForestJobTest extends LocalSparkSpec {

	// params to test: PARAM_INFORMATION_GAIN, VALUE_GINI, VALUE_ENTROPY,
	// PARAM_MAX_DEPTH, PARAM_MAX_BINS, PARAM_INPUT_TABLE
	// PARAM_LABEL_INDEX, PARAM_COL_IDXS, PARAM_NOMINAL_FEATURE_INFO
	// specific for RF: PARAM_FEATURE_SUBSET_STRATEGY , PARAM_NUM_TREES ,
	// PARAM_IS_CLASSIFICATION
	// not tested since optional: PARAM_NO_OF_CLASSES,
	// PARAM_NOMINAL_FEATURE_INFO

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason(null, new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", 101,
				true, EnumContainer.FeatureSubsetStrategies.auto);
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_INFORMATION_GAIN_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, null, 99, true,
				FeatureSubsetStrategies.auto);
		myCheck(params, AbstractTreeLearnerJob.PARAM_INFORMATION_GAIN, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_MAX_DEPTH_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, null, 25, "gini", 99,
				true, FeatureSubsetStrategies.auto);
		myCheck(params, AbstractTreeLearnerJob.PARAM_MAX_DEPTH, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_MAX_BINS_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, null, "gini", 99,
				true, FeatureSubsetStrategies.auto);
		myCheck(params, AbstractTreeLearnerJob.PARAM_MAX_BINS, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_LABEL_INDEX_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), null, 2l, 4, 25, "gini", 99,
				true, FeatureSubsetStrategies.auto);
		myCheck(params, ParameterConstants.PARAM_LABEL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_COL_IDXS_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", null,
				new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", 99, true,
				FeatureSubsetStrategies.auto);
		myCheck(params, ParameterConstants.PARAM_COL_IDXS, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_FEATURE_SUBSET_STRATEGY_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", 99,
				true, null);
		myCheck(params, RandomForestLearnerJob.PARAM_FEATURE_SUBSET_STRATEGY,
				"Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_NUM_TREES_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", null,
				false, FeatureSubsetStrategies.auto);
		myCheck(params, RandomForestLearnerJob.PARAM_NUM_TREES, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_IS_CLASSIFICATION_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", 138,
				null, FeatureSubsetStrategies.auto);
		myCheck(params, RandomForestLearnerJob.PARAM_IS_CLASSIFICATION, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_SEED_Parameter()
			throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", 138,
				false, FeatureSubsetStrategies.auto, null);
		myCheck(params, ParameterConstants.PARAM_SEED, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = RandomForestTaskTest.paramsAsJason("in", new Integer[] {
				0, 1 }, new NominalFeatureInfo(), 1, 2l, 4, 25, "gini", 138,
				false, FeatureSubsetStrategies.auto);
		KnimeSparkJob testObj = new RandomForestLearnerJob();
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
		KnimeSparkJob testObj = new RandomForestLearnerJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}