package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.CollaborativeFilteringJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.CollaborativeFilteringModel;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.predictor.PredictionTask;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class CollaborativeFilteringTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final Integer aUserIndex, final Integer aProductIndex,
			final Integer aRatingIndex, final Double aLambda) {
		return paramsAsJason(aInputTableName,
				aUserIndex, aProductIndex, aRatingIndex, aLambda, null);
	}

	public static String paramsAsJason(final String aInputTableName,
			final Integer aUserIndex, final Integer aProductIndex,
			final Integer aRatingIndex, final Double aLambda, final String aPredictions) {
		return CollaborativeFilteringTask.paramsAsJason(aInputTableName,
				aUserIndex, aProductIndex, aRatingIndex, aLambda, null, null,
				null, null, null, null, null, null, null, aPredictions);
	}
	
	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		CollaborativeFilteringTask testObj = new CollaborativeFilteringTask(
				null, "inputRDD", 4, 0, 1, 2d);
		final String params = testObj.paramsAsJason("out");
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new CollaborativeFilteringJob().validate(config));
	}

	@Test
	public void verifyThatCollaborativeFilteringJobStoresResultMatrixAsNamedRDDAndReturnsModel()
			throws Throwable {
		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_RATING_TABLE,
				"tab1");

		// data must be entirely numeric!
		final double alpha = 0.01;
		final int rank = 10;
		final int numIterations = 20;
		CollaborativeFilteringTask testObj = new CollaborativeFilteringTask(
				CONTEXT_ID, "tab1", 0, 1, 2, null);
		testObj.withAlpha(alpha).withRank(rank)
				.withNumIterations(numIterations);

		//learn and run predictor
		CollaborativeFilteringModel model = testObj.execute(null, "predictions1");
		assertTrue("model expected", model != null);

		Object[][] predictions1 = fetchResultTable(CONTEXT_ID, "predictions", MINI_RATING_TABLE.length);
		
		int numErrors = 0;
		for (Object[] r : predictions1) {
			if (((Double)r[2]).intValue() != Math.round(((Double)r[3]))) {
				numErrors++;
			}
		}
		assertTrue("unexpected large number of errors", numErrors < 0.01*predictions1.length);
		
		//predict again, with re-constructed model
		PredictionTask.predict(null, CONTEXT_ID, "tab1", model, new Integer[] {0,1,2}, "predictions2");
		// not sure what else to check here....
		Object[][] predictions2 = fetchResultTable(CONTEXT_ID, "predictions2", MINI_RATING_TABLE.length);
		for (int r = 0; r<predictions1.length; r++) {
			assertEquals("predictions should be the same", (Double)predictions1[r][3], (Double)predictions2[r][3], 0.001);
		}
	}
}
