package com.knime.bigdata.spark.node.mllib.prediction.logisticregression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.LogisticRegressionJob;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.GradientType;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.UpdaterType;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.linear.SGDLearnerTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class LogisticRegressionTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputRDD,
			final Integer[] featureColIdxs, final Integer classColIdx,
			final Integer aNumIterations, final Double aRegularization,
			final Boolean aUseSGD) throws GenericKnimeSparkException {
		return paramsAsJason(aInputRDD, featureColIdxs, classColIdx,
				aNumIterations, aRegularization, aUseSGD, 8, 0.9d);
	}

	public static String paramsAsJason(final String aInputRDD,
			final Integer[] featureColIdxs, final Integer classColIdx,
			final Integer aNumIterations, final Double aRegularization,
			final Boolean aUseSGD, final Integer aNumCorrections,
			final Double aTolerance) throws GenericKnimeSparkException {
		return SGDLearnerTask.paramsAsJason(aInputRDD, featureColIdxs,
				classColIdx, aNumIterations, aRegularization, aUseSGD,
				aNumCorrections, aTolerance, UpdaterType.L1Updater, true,
				false, true, GradientType.LeastSquaresGradient, 0.6, 0.9);
	}

	// PARAM_UPDATER_TYPE,PARAM_VALIDATE_DATA,PARAM_ADD_INTERCEPT ,
	// PARAM_USE_FEATURE_SCALING,PARAM_STEP_SIZE,PARAM_GRADIENT_TYPE,PARAM_FRACTION
	public static String paramsAsJason(final UpdaterType aUpdaterType,
			final Boolean aValidateData, final Boolean aAddIntercept,
			final Boolean aUseFeatureScaling, final GradientType aGradientType,
			final Double aStepSize, final Double aFraction)
			throws GenericKnimeSparkException {
		return SGDLearnerTask.paramsAsJason("in",
				new Integer[] { 0, 1 }, 1, 25, 0.2d, true, 8, 0.9d,
				aUpdaterType, aValidateData, aAddIntercept, aUseFeatureScaling,
				aGradientType, aStepSize, aFraction);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final LogisticRegressionTask testObj = new LogisticRegressionTask(null,
				"inputRDD", new Integer[] { 0, 1 }, 1, 23, 0.4d, false, 5,
				0.4d, UpdaterType.L1Updater, true, false, true,
				GradientType.LeastSquaresGradient, 0.6, 0.9);
		final String params = testObj.learnerDef();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new LogisticRegressionJob().validate(config));
	}

	@Test
	public void createLogisticRegressionFromEntirelyNumericDataLBFGS()
			throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1 }, { 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 0 }, { 4.6, 3.1, 1.5, 1 } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		// data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 2, 1 };
		final LogisticRegressionTask testObj = new LogisticRegressionTask(
				CONTEXT_ID, "tab1", cols, 3, 29, 0.4d, false, 5, 0.2d, UpdaterType.L1Updater, true, false, true,
				GradientType.LeastSquaresGradient, null, null);

		final LogisticRegressionModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....

	}

	@Test
	public void createLogisticRegressionFromEntirelyNumericDataSGD()
			throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1 }, { 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 0 }, { 4.6, 3.1, 1.5, 1 } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		// data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 2, 1 };
		final LogisticRegressionTask testObj = new LogisticRegressionTask(
				CONTEXT_ID, "tab1", cols, 3, 15, 0.4d, true, null, null, UpdaterType.SquaredL2Updater, true, false, true,
				GradientType.LeastSquaresGradient, 0.6, 0.9);

		final LogisticRegressionModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....

	}
}
