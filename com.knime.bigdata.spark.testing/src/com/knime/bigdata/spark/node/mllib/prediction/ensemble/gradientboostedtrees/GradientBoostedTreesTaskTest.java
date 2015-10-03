package com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import javax.annotation.Nullable;

import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.GradientBoostedTreesLearnerJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.EnumContainer;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.ensemble.gradientboostedtrees.GradientBoostedTreesTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class GradientBoostedTreesTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputRDD,
			final Integer[] featureColIdxs,
			@Nullable final NominalFeatureInfo nominalFeatureInfo,
			final Integer classColIdx, final Long aNrOfClasses,
			final int maxDepth, final int maxNoOfBins,
			final Integer aNumIterations, final double aLearningRate,
			final boolean aIsClassification, final EnumContainer.LossFunctions aLossFunction)
			throws GenericKnimeSparkException {
		return GradientBoostedTreesTask.paramsAsJason(aInputRDD,
				featureColIdxs, nominalFeatureInfo, classColIdx, aNrOfClasses,
				maxDepth, maxNoOfBins, aNumIterations, aLearningRate,
				aIsClassification, aLossFunction);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final GradientBoostedTreesTask testObj = new GradientBoostedTreesTask(
				null, "inputRDD", new Integer[] { 0, 1 },
				new NominalFeatureInfo(), 1, 2l, 99, 15, 23, 0.4d, false);
		final String params = testObj.learnerDef();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new GradientBoostedTreesLearnerJob().validate(config));
	}

	@Test
	public void createGradientBoostedTreesFromEntirelyNumericDataWithoutNominalFeatureInfo()
			throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1 }, { 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 0 }, { 4.6, 3.1, 1.5, 1 } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		// data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 2, 1 };
		final GradientBoostedTreesTask testObj = new GradientBoostedTreesTask(
				CONTEXT_ID, "tab1", cols, null, 3, 2l, 29, 15, 23, 0.4d, true);

		final GradientBoostedTreesModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....

	}

	@Test
	public void createRegressionForestFromEntirelyNumericDataWithoutNominalFeatureInfo()
			throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1 }, { 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 2 }, { 4.6, 3.1, 1.5, 1 } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		// data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 3, 1 };
		final GradientBoostedTreesTask testObj = new GradientBoostedTreesTask(
				CONTEXT_ID, "tab1", cols, null, 2, 3l, 19, 15, 23, 0.4d, false);

		final GradientBoostedTreesModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....

	}
}
