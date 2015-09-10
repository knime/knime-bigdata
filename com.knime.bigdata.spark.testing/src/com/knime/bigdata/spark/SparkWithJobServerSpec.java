package com.knime.bigdata.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.*;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.knime.core.node.CanceledExecutionException;

import com.knime.bigdata.spark.jobserver.client.DataUploader;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.client.UploadUtil;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.io.table.writer.Table2SparkNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

/**
 *
 * @author dwk
 *
 */
public abstract class SparkWithJobServerSpec extends UnitSpec {

	private static Config origConfig = KNIMEConfigContainer.m_config;

	@SuppressWarnings("javadoc")
	protected static KNIMESparkContext CONTEXT_ID;

	final static String ROOT_PATH = ".." + File.separatorChar
			+ "com.knime.bigdata.spark" + File.separator;

	/**
	 * @return path to job jar (in main project)
	 */
	protected static String getJobJarPath() {
		return SparkPlugin.getDefault().getPluginRootPath()
				+ File.separatorChar + "resources" + File.separatorChar
				+ "knimeJobs.jar";
	}

	/**
	 * make sure that we do not connect to the server
	 *
	 * @throws GenericKnimeSparkException
	 */
	@BeforeClass
	public static void beforeSuite() throws GenericKnimeSparkException {
		new SparkPlugin() {
			@Override
			public String getPluginRootPath() {
				return new File(ROOT_PATH).getAbsolutePath();
			}
		};

		KNIMEConfigContainer.m_config = KNIMEConfigContainer.m_config
				.withValue("unitTestMode",
						ConfigValueFactory.fromAnyRef("true"));
		// comment this out if you want to test the real server
		// use a dummy RestClient to be able to test things locally
		// KNIMEConfigContainer.m_config =
		// KNIMEConfigContainer.m_config.withValue("spark.jobServer",
		// ConfigValueFactory.fromAnyRef("dummy"));

		CONTEXT_ID = KnimeContext.getSparkContext();

		// TODO: Upload the static jobs jar only if not exists
		JobControler.uploadJobJar(CONTEXT_ID, getJobJarPath());
	}

	/**
	 * restore original configuration
	 *
	 * @throws Exception
	 */
	@AfterClass
	public static void afterSuite() throws Exception {
		KNIMEConfigContainer.m_config = origConfig;
		// KnimeContext.destroySparkContext(CONTEXT_ID);
		// //need to wait a bit before we can actually test whether it is really
		// gone
		// Thread.sleep(200);
		// // TODO - what would be the expected status?
		// assertTrue("context status should NOT be OK after destruction",
		// KnimeContext.getSparkContextStatus(CONTEXT_ID) != JobStatus.OK);

	}

	protected static final Object[][] TEST_TABLE = new Object[][] {
			new Object[] { 1, true, 3.2d, "my string" },
			new Object[] { 2, false, 3.2d, "my string" },
			new Object[] { 3, true, 38d, "my other string" },
			new Object[] { 4, false, 34.2d, "my other string" } };

	protected static final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
			{ 5.1, 3.5, 1.4, 0.2, "Iris-setosa" },
			{ 4.9, 3.0, 1.4, 0.2, "Iris-setosa" },
			{ 4.7, 3.2, 1.3, 0.2, "Iris-versicolor" },
			{ 4.6, 3.1, 1.5, 0.2, "Iris-virginica" } };

	protected static final Object[][] MINI_RATING_TABLE = new Object[][] {
			// user, product, rating
			{ 1, 1, 5.0 }, { 1, 2, 1.0 }, { 1, 3, 5.0 }, { 1, 4, 1.0 },
			{ 2, 1, 5.0 }, { 2, 2, 1.0 }, { 2, 3, 5.0 }, { 2, 4, 1.0 },
			{ 3, 1, 1.0 }, { 3, 2, 5.0 }, { 3, 3, 1.0 }, { 3, 4, 5.0 },
			{ 4, 1, 1.0 }, { 4, 2, 5.0 }, { 4, 3, 1.0 }, { 4, 4, 5.0 } };

	/**
	 * @param contextName
	 * @param resTableName
	 * @return
	 * @throws GenericKnimeSparkException
	 * @throws CanceledExecutionException
	 */
	public static String importTestTable(final KNIMESparkContext contextName,
			final Object[][] aTable, final String resTableName)
			throws GenericKnimeSparkException, CanceledExecutionException {
		final String fName = System.currentTimeMillis()+"unittest";
		final UploadUtil util = new UploadUtil(contextName, aTable, fName);
        util.upload();
        
		String params = Table2SparkNodeModel.paramDef(util.getServerFileName(), resTableName);
		String jobId = JobControler
				.startJob(contextName,
						ImportKNIMETableJob.class.getCanonicalName(),
						params.toString());

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);
		
		String[] files = DataUploader.listFiles(contextName);
		boolean found = false;
		for (String f : files) {
			found = found || f.contains(fName);
		}
		assertTrue("temp file must be known on server", found);
		util.cleanup();
		
		files = DataUploader.listFiles(contextName);
		found = false;
		for (String f : files) {
			found = found || f.contains(fName);
		}
		assertFalse("temp file must have been removed from server", found);
		return jobId;
	}

	protected void checkResult(final KNIMESparkContext aContextName,
			final String resTableName, final Object[][] aExpected)
			throws Exception {

		Object[][] arrayRes = fetchResultTable(aContextName, resTableName,
				aExpected.length);

		for (int i = 0; i < arrayRes.length; i++) {
			boolean found = false;
			for (int j = 0; j < aExpected.length; j++) {
				found = found || Arrays.equals(arrayRes[i], aExpected[j]);
			}
			assertTrue("result row[" + i + "]: " + Arrays.toString(arrayRes[i])
					+ " - not found.", found);
		}
	}

	private String rowFetcherDef(final int aNumRows, final String aTableName) {
		return JsonUtils.asJson(new Object[] {
				ParameterConstants.PARAM_INPUT,
				new String[] { ParameterConstants.PARAM_NUMBER_ROWS,
						"" + aNumRows, KnimeSparkJob.PARAM_INPUT_TABLE,
						aTableName } });
	}

	/**
	 * @param aContextName
	 * @param aResTableName
	 * @param aExpected
	 * @return
	 * @throws GenericKnimeSparkException
	 * @throws CanceledExecutionException
	 */
	protected Object[][] fetchResultTable(final KNIMESparkContext aContextName,
			final String aResTableName, final int aExpectedLength)
			throws GenericKnimeSparkException, CanceledExecutionException {
		return fetchResultTable(aContextName, aResTableName, aExpectedLength, 0);
	}

	protected Object[][] fetchResultTable(final KNIMESparkContext aContextName,
			final String aResTableName, final int aExpectedLength,
			final double aAllowedPercentageOff)
			throws GenericKnimeSparkException, CanceledExecutionException {
		// now check result:
		String takeJobId = JobControler.startJob(aContextName,
				FetchRowsJob.class.getCanonicalName(),
				rowFetcherDef(aExpectedLength, aResTableName));
		JobResult res = JobControler.waitForJobAndFetchResult(aContextName,
				takeJobId, null);
		assertNotNull("row fetcher must return a result", res);

		Object[][] arrayRes = (Object[][]) res.getObjectResult();
		if (aAllowedPercentageOff == 0) {
			assertEquals("fetcher should return correct number of rows",
					aExpectedLength, arrayRes.length);
		} else {
			assertTrue("fetcher should return correct number of rows, got "
					+ arrayRes.length + ", expected: " + aExpectedLength,
					aExpectedLength * ((double) (100 - aAllowedPercentageOff))
							/ 100 <= arrayRes.length);
			assertTrue("fetcher should return correct number of rows, got "
					+ arrayRes.length + ", expected: " + aExpectedLength,
					aExpectedLength * ((double) (100 + aAllowedPercentageOff))
							/ 100 >= arrayRes.length);

		}
		return arrayRes;
	}

}