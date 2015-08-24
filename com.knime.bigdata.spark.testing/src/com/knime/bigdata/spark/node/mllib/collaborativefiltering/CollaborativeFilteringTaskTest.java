package com.knime.bigdata.spark.node.mllib.collaborativefiltering;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.CollaborativeFilteringJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
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
			final Integer aRatingIndex, final Double aLambda,
			final String aResult) {
		return CollaborativeFilteringTask.paramsAsJason(aInputTableName,
				aUserIndex, aProductIndex, aRatingIndex, aLambda, null, null,
				null, null, null, null, null, null, null, aResult);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		CollaborativeFilteringTask testObj = new CollaborativeFilteringTask(
				null, "inputRDD", 4, 0, 1, 2d, "u");
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new CollaborativeFilteringJob().validate(config));
	}

	@Test
	public void verifyThatCollaborativeFilteringJobStoresResultMatrixAsNamedRDDAndReturnsModel()
			throws Throwable {
		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_RATING_TABLE,
				"tab1");

		// data must be entirely numeric!
		final double alpha = 0.01;
		final int rank = 10;
		final int numIterations = 20;
		CollaborativeFilteringTask testObj = new CollaborativeFilteringTask(
				context, "tab1", 0, 1, 2, null, "predictions");
		testObj.withAlpha(alpha).withRank(rank)
				.withNumIterations(numIterations);

		MatrixFactorizationModel model = testObj.execute(null);
		assertTrue("model expected", model != null);

		// not sure what else to check here....
		fetchResultTable(context, "predictions", 4);

	}
}
