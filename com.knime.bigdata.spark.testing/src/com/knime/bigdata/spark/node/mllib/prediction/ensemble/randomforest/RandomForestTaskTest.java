package com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.AbstractTreeLearnerJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.RandomForestLearnerJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.RandomForestFeatureSubsetStrategies;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.randomforest.RandomForestTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class RandomForestTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputRDD,
			final Integer[] featureColIdxs,
			final NominalFeatureInfo nominalFeatureInfo,
			final Integer classColIdx, final Long noOfClasses,
			final Integer maxDepth, final Integer maxNoOfBins,
			final String qualityMeasure, Integer aNumTrees,
			Boolean aIsClassification, RandomForestFeatureSubsetStrategies aFSStrategy, final Integer aSeed)
			throws GenericKnimeSparkException {
		return RandomForestTask.paramsAsJason(aInputRDD, featureColIdxs,
				nominalFeatureInfo, classColIdx, noOfClasses, maxDepth,
				maxNoOfBins, qualityMeasure, aNumTrees, aIsClassification,
				aFSStrategy, aSeed);
	}

	public static String paramsAsJason(final String aInputRDD,
			final Integer[] featureColIdxs,
			final NominalFeatureInfo nominalFeatureInfo,
			final Integer classColIdx, final Long noOfClasses,
			final Integer maxDepth, final Integer maxNoOfBins,
			final String qualityMeasure, Integer aNumTrees,
			Boolean aIsClassification, RandomForestFeatureSubsetStrategies aFSStrategy)
			throws GenericKnimeSparkException {
		return RandomForestTask.paramsAsJason(aInputRDD, featureColIdxs,
				nominalFeatureInfo, classColIdx, noOfClasses, maxDepth,
				maxNoOfBins, qualityMeasure, aNumTrees, aIsClassification,
				aFSStrategy, 99);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final RandomForestTask testObj = new RandomForestTask(null, "inputRDD",
				new Integer[] { 0, 1 }, new NominalFeatureInfo(), 1, 2l, 1, 2,
				"gini", 125, false, RandomForestFeatureSubsetStrategies.auto, 875634);
		final String params = testObj.learnerDef();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new RandomForestLearnerJob().validate(config));
	}

	@Test
	public void createRandomForestFromEntirelyNumericDataWithoutNominalFeatureInfo()
			throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1 }, { 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 2 }, { 4.6, 3.1, 1.5, 1 } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		// data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 2, 1 };
		final RandomForestTask testObj = new RandomForestTask(CONTEXT_ID,
				"tab1", cols, null, 3, 3l, 11, 12,
				AbstractTreeLearnerJob.VALUE_GINI, 133, true,
				RandomForestFeatureSubsetStrategies.auto, 875634);

		final RandomForestModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....

	}
	
	@Test
	public void createRandomRegressionForestFromEntirelyNumericDataWithoutNominalFeatureInfo()
			throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1 }, { 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 2 }, { 4.6, 3.1, 1.5, 1 } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		// data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 3, 1 };
		final RandomForestTask testObj = new RandomForestTask(CONTEXT_ID,
				"tab1", cols, null, 2, 3l, 11, 12,
				AbstractTreeLearnerJob.VALUE_VARIANCE, 133, false,
				RandomForestFeatureSubsetStrategies.auto, 875634);

		final RandomForestModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....

	}
}
