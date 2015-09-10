package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.jobs.KMeansLearner;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.clustering.kmeans.KMeansTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class KMeansLearnerTest extends SparkWithJobServerSpec {

	private static String getParams(final String aInputDataPath,
			final Integer aNoOfCluster, final Integer aNumIterations,
			final String aOutputTableName) {
		return KMeansTask.kmeansLearnerDef(aInputDataPath, new Integer[] { 0,
				1, 2 }, aNumIterations, aNoOfCluster, aOutputTableName);
	}

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = getParams(null, 6, 99, "~spark/data/spark");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingNumClustersParameter()
			throws Throwable {
		String params = getParams("xx", null, 99, "~spark/data/spark");
		myCheck(params, KMeansLearner.PARAM_NUM_CLUSTERS, "Input");
	}

	@Test
	public void jobValidationShouldCheckIncorrectNumClustersParameter()
			throws Throwable {
		String params = getParams("xx", -1, 99, "~spark/data/spark");
		String msg = "Input parameter '" + KMeansLearner.PARAM_NUM_CLUSTERS
				+ "' must be a positive number.";
		myCheck(params, msg);

	}

	@Test
	public void jobValidationShouldCheckMissingNumIterationsParameter()
			throws Throwable {
		String params = getParams("xx", 99, null, "~spark/data/spark");
		myCheck(params, ParameterConstants.PARAM_NUM_ITERATIONS, "Input");
	}

	@Test
	public void jobValidationShouldCheckIncorrectNumIterationsParameter()
			throws Throwable {
		String params = getParams("xx", 99, -1, "~spark/data/spark");
		String msg = "Input parameter '"
				+ ParameterConstants.PARAM_NUM_ITERATIONS
				+ "' must be a positive number.";
		myCheck(params, msg);

	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KMeansLearner testObj = new KMeansLearner();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

	@Test
	public void runningKMeansDirectlyShouldProduceResult() throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();
		try {
			importTestTable(contextName, TEST_TABLE, "tabKmeans");

			String params = KMeansTask.kmeansLearnerDef("tabKmeans",
					new Integer[] { 0, 2 }, 3, 2, "tabKmeansOut");

			String jobId = JobControler.startJob(contextName,
					KMeansLearner.class.getCanonicalName(), params.toString());

			JobControler.waitForJobAndFetchResult(contextName, jobId, null);

			// KMeans spark model is serialized as a string
			assertFalse("job should not be running anymore",
					JobStatus.OK.equals(JobControler.getJobStatus(contextName,
							jobId)));

			checkResult(contextName, "tabKmeansOut");

		} finally {
			KnimeContext.destroySparkContext(contextName);
		}

	}

	private static void checkResult(final KNIMESparkContext aContextName,
			final String aResTable) throws Exception {

		// now check result:
		String takeJobId = JobControler.startJob(aContextName,
				FetchRowsJob.class.getCanonicalName(),
				rowFetcherDef(10, aResTable));

		JobResult res = JobControler.waitForJobAndFetchResult(aContextName,
				takeJobId, null);
		assertNotNull("row fetcher must return a result", res);
		Object[][] arrayRes = (Object[][]) res.getObjectResult();
		assertEquals("fetcher should return correct number of rows",
				ImportKNIMETableJobTest.TEST_TABLE.length, arrayRes.length);
		for (int i = 0; i < arrayRes.length; i++) {
			Object[] o = arrayRes[i];
			System.out.println("row[" + i + "]: " + o);
		}
	}

	private static String rowFetcherDef(final int aNumRows,
			final String aTableName) {
		return JsonUtils.asJson(new Object[] {
				ParameterConstants.PARAM_INPUT,
				new String[] { ParameterConstants.PARAM_NUMBER_ROWS,
						"" + aNumRows, KnimeSparkJob.PARAM_INPUT_TABLE,
						aTableName } });
	}

}