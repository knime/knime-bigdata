package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.jobs.KMeansLearner;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.clustering.kmeans.KMeansTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
public class KMeansLearnerTest {

	public static String getInputOutputParamPair(final String aInputDataPath,
			final String aNumClusters, final String aNumIterations,
			final String aOutputDataPath) {
		StringBuilder params = new StringBuilder("");
		params.append("   \"").append(ParameterConstants.PARAM_INPUT)
				.append("\" {\n");
        params.append("         \"")
        .append(ParameterConstants.PARAM_COL_IDXS)
        .append("\": [0,1,2],\n");

		if (aInputDataPath != null) {
			params.append("         \"")
					.append(ParameterConstants.PARAM_TABLE_1)
					.append("\": \"").append(aInputDataPath).append("\",\n");
		}
		if (aNumClusters != null) {
			params.append("         \"")
					.append(ParameterConstants.PARAM_NUM_CLUSTERS)
					.append("\": \"").append(aNumClusters).append("\",\n");
		}
		if (aNumIterations != null) {
			params.append("         \"")
					.append(ParameterConstants.PARAM_NUM_ITERATIONS)
					.append("\": \"").append(aNumIterations).append("\",\n");
		}
		params.append("    }\n");
		params.append("    \"").append(ParameterConstants.PARAM_OUTPUT)
				.append("\" {\n");
		if (aOutputDataPath != null) {
			params.append("         \"")
					.append(ParameterConstants.PARAM_TABLE_1)
					.append("\": \"").append(aOutputDataPath).append("\"\n");
		}
		params.append("    }\n");
		params.append("    \n");
		return params.toString();
	}

	private static String getParams(final String aInputDataPath,
			final String aNumClusters, final String aNumIterations,
			final String aOutputDataPath) {
		StringBuilder params = new StringBuilder("{\n");
		params.append(getInputOutputParamPair(aInputDataPath, aNumClusters,
				aNumIterations, aOutputDataPath));
		params.append("}");
		return params.toString();
	}

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = getParams(null, "6", "99", "~spark/data/spark");
		myCheck(params, ParameterConstants.PARAM_INPUT + "."
				+ ParameterConstants.PARAM_TABLE_1, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingNumClustersParameter()
			throws Throwable {
		String params = getParams("xx", null, "99", "~spark/data/spark");
		myCheck(params, ParameterConstants.PARAM_INPUT + "."
				+ ParameterConstants.PARAM_NUM_CLUSTERS, "Input");
	}

	@Test
	public void jobValidationShouldCheckIncorrectNumClustersParameter()
			throws Throwable {
		String params = getParams("xx", "not a number", "99",
				"~spark/data/spark");
		String msg = "Input parameter '" + ParameterConstants.PARAM_INPUT + "."
				+ ParameterConstants.PARAM_NUM_CLUSTERS
				+ "' is not of expected type 'integer'.";
		myCheck(params, msg);

	}

	@Test
	public void jobValidationShouldCheckMissingNumIterationsParameter()
			throws Throwable {
		String params = getParams("xx", "99", null, "~spark/data/spark");
		myCheck(params, ParameterConstants.PARAM_INPUT + "."
				+ ParameterConstants.PARAM_NUM_ITERATIONS, "Input");
	}

	@Test
	public void jobValidationShouldCheckIncorrectNumIterationsParameter()
			throws Throwable {
		String params = getParams("xx", "99", "not a number",
				"~spark/data/spark");
		String msg = "Input parameter '" + ParameterConstants.PARAM_INPUT + "."
				+ ParameterConstants.PARAM_NUM_ITERATIONS
				+ "' is not of expected type 'integer'.";
		myCheck(params, msg);

	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
	    KMeansLearner testObj = new KMeansLearner();
		Config config = ConfigFactory.parseString(params);
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate( config));
	}

	//@Test
	public void runningKMeansDirectlyShouldProduceResult() throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();
		try {

			String params = getParams("/home/spark/data/kmeans-input.txt", "6",
					"99", "/home/spark/data/spark/kmean-6-99");

			String jobId = JobControler.startJob(contextName,
					KMeansTask.class.getCanonicalName(), params.toString());

			assertFalse("job should have finished properly",
					JobControler.waitForJob(contextName, jobId, null).equals( JobStatus.UNKNOWN));

			// KMeans spark model is serialized as a string
			assertFalse("job should not be running anymore", JobStatus.OK.equals(
					JobControler.getJobStatus(contextName, jobId)));

			checkResult(contextName);

		} finally {
			KnimeContext.destroySparkContext(contextName);
		}

	}

	private void checkResult(final KNIMESparkContext aContextName) throws Exception {

		// now check result:
		String takeJobId = JobControler.startJob(aContextName,
				FetchRowsJob.class.getCanonicalName(), rowFetcherDef(10, "/home/spark/data/spark/kmean-6-99"));
		assertFalse("job should have finished properly",
				JobControler.waitForJob(aContextName, takeJobId, null).equals( JobStatus.UNKNOWN));
		JobResult res = JobControler.fetchJobResult(aContextName, takeJobId);
		assertNotNull("row fetcher must return a result", res);
		assertEquals("fetcher should return OK as result status", "OK",
				res.getMessage());
		Object[][]  arrayRes = (Object[][] )res.getObjectResult();
		assertEquals("fetcher should return correct number of rows", 10, arrayRes.length);
		for (int i = 0; i < arrayRes.length; i++) {
			Object[] o = arrayRes[i];
			System.out.println("row[" + i + "]: " + o);
		}
	}

	private String rowFetcherDef(final int aNumRows, final String aTableName) {
		return JsonUtils.asJson(new Object[] {
				ParameterConstants.PARAM_INPUT,
				new String[] { ParameterConstants.PARAM_NUMBER_ROWS,
						"" + aNumRows, ParameterConstants.PARAM_TABLE_1,
						aTableName } });
	}

}