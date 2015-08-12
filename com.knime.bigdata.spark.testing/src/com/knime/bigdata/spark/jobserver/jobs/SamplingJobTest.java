package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.junit.Test;
import org.knime.base.node.preproc.sample.SamplingNodeSettings;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.CountMethods;
import org.knime.base.node.preproc.sample.SamplingNodeSettings.SamplingMethods;
import org.knime.core.node.CanceledExecutionException;

import com.knime.bigdata.spark.SparkSpec;
import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.node.mllib.sampling.MLlibSamplingNodeModel;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 *
 * @author dwk
 * 
 *         (these tests require a running job-server)
 *
 */
@SuppressWarnings("javadoc")
public class SamplingJobTest extends SparkSpec {

	private static String getParams(final String aTableToSample,
			final SamplingMethods aMethod, final int aClassColIx, final CountMethods aCountMethod,
			boolean aIsWithReplacement, boolean aExact, final Long aSeed, double aCount,
			String aOut1, String aOut2) {
		SamplingNodeSettings settings = new SamplingNodeSettings();
		settings.samplingMethod(aMethod);
		settings.countMethod(aCountMethod);
		settings.count((int) aCount);
		settings.fraction(aCount);
		return MLlibSamplingNodeModel.paramDef(aTableToSample, settings, aClassColIx,
				aIsWithReplacement, aSeed, aExact, aOut1, aOut2);
	}

	@Test
	public void absoluteSamplingFromTopEntireTable() throws Throwable {
		final String resTableName = "OutTab";
		String params = getParams("inTab", SamplingMethods.First,-1,
				CountMethods.Absolute, false, false,99l, 42, resTableName, null);

		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_1,
				"inTab");

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);
		checkResult(contextName, resTableName, TEST_TABLE_1);
	}

	@Test
	public void absoluteSamplingFromTopPartOfTable() throws Throwable {
		final String resTableName = "OutTab";
		String params = getParams("inTab", SamplingMethods.First,-1,
				CountMethods.Absolute, false, false,99l, 2, resTableName, null);

		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_1,
				"inTab");

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		fetchResultTable(contextName, resTableName, 2);
	}

	@Test
	public void absoluteSamplingFromTopPartOfSortedTable() throws Throwable {

		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2,
				"inTab");

		sortTable("inTab", 1, "inTabSorted");

		final String resTableName = "OutTab";
		String params = getParams("inTabSorted", SamplingMethods.First,-1,
				CountMethods.Absolute, false, false,99l, 2, resTableName, null);

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		Object[][] table = fetchResultTable(contextName, resTableName, 2);
		// when sorted by key 1, then sort order is 0,2,1,3
		assertArrayEquals("ping", TEST_TABLE_2[0], table[0]);
		assertArrayEquals("ping2", TEST_TABLE_2[2], table[1]);

		// use different sort index
		sortTable("inTab", 2, "inTabSorted");
		jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		table = fetchResultTable(contextName, resTableName, 2);
		// when sorted by key 2, then sort order is 3,2,1,0
		assertArrayEquals("w", TEST_TABLE_2[3], table[0]);
		assertArrayEquals("x", TEST_TABLE_2[2], table[1]);
	}

	@Test
	public void relativeSamplingFromTopPartOfSortedTable() throws Throwable {

		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		ImportKNIMETableJobTest.importTestTable(contextName, TEST_TABLE_2,
				"inTab");

		sortTable("inTab", 1, "inTabSorted");

		final String resTableName = "OutTab";
		String params = getParams("inTabSorted", SamplingMethods.First,1,
				CountMethods.Relative, false, false,99l, 0.25d, resTableName, null);

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		Object[][] table = fetchResultTable(contextName, resTableName, 1);
		// when sorted by key 1, then sort order is 0,2,1,3
		assertArrayEquals("ping", TEST_TABLE_2[0], table[0]);

		// use different sort index
		sortTable("inTab", 2, "inTabSorted");
		params = getParams("inTabSorted", SamplingMethods.First,1,
				CountMethods.Relative, false, false,99l, 0.75d, resTableName, null);
		jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		table = fetchResultTable(contextName, resTableName, 3);
		// when sorted by key 2, then sort order is 3,2,1,0
		assertArrayEquals("w", TEST_TABLE_2[3], table[0]);
		assertArrayEquals("x", TEST_TABLE_2[2], table[1]);
		assertArrayEquals("y", TEST_TABLE_2[1], table[2]);
	}

	@Test
	public void absoluteRandomSamplingFromSortedTable() throws Throwable {

		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		final Object[][] inTab = getTable3(1000);
		final int numToSample = (int) inTab.length / 3;

		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		sortAndTestRandomSampling(contextName, inTab, numToSample, numToSample,
				0, CountMethods.Absolute);

		sortAndTestRandomSampling(contextName, inTab, numToSample, numToSample,
				2, CountMethods.Absolute);
	}

	@Test
	public void relativeRandomSamplingFromSortedTable() throws Throwable {

		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		final Object[][] inTab = getTable3(1000);

		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		sortAndTestRandomSampling(contextName, inTab, 0.25, 250, 0,
				CountMethods.Relative);

		sortAndTestRandomSampling(contextName, inTab, 0.9, 900, 2,
				CountMethods.Relative);
	}

	@Test
	public void relativeInexactStratifiedSamplingFromSortedTable() throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		// classes are Ping (1/10) and Pong (9/10)
		final Object[][] inTab = getTable3(900);
		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		sortAndTestStratifiedSampling(contextName, inTab, 0.10, 90, 0,
				CountMethods.Relative, false, null);

		sortAndTestStratifiedSampling(contextName, inTab, 0.50, 450, 2,
				CountMethods.Relative, false, null);
	}
	
	@Test
	public void relativeStratifiedSamplingFromSortedTable() throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		// classes are Ping (1/10) and Pong (9/10)
		final Object[][] inTab = getTable3(900);
		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		sortAndTestStratifiedSampling(contextName, inTab, 0.10, 90, 0,
				CountMethods.Relative, true, null);

		sortAndTestStratifiedSampling(contextName, inTab, 0.50, 450, 2,
				CountMethods.Relative, true, null);
	}

	@Test
	public void absoluteStratifiedSamplingFromTable() throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		// classes are Ping (1/10) and Pong (9/10)
		final Object[][] inTab = getTable3(900);
		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		sortAndTestStratifiedSampling(contextName, inTab, 90, 90, 0,
				CountMethods.Absolute, true, null);

		sortAndTestStratifiedSampling(contextName, inTab, 450, 450, 2,
				CountMethods.Absolute, true, null);
	}
	
	@Test
	public void absoluteStratifiedSplit() throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		// classes are Ping (1/10) and Pong (9/10)
		final Object[][] inTab = getTable3(900);
		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		sortAndTestStratifiedSampling(contextName, inTab, 90, 90, 0,
				CountMethods.Absolute, true, "tab2");

		sortAndTestStratifiedSampling(contextName, inTab, 450, 450, 2,
				CountMethods.Absolute, true, "tab2");
	}
	
	//discussed with TK - we will implement this later iff customers ask for it
	//@Test
	public void relativeLinearSamplingFromTable() throws Throwable {
		// must sample first, last and every n-th row inbetween
		KNIMESparkContext contextName = KnimeContext.getSparkContext();

		// classes are Ping (1/10) and Pong (9/10)
		final Object[][] inTab = getTable3(100);
		ImportKNIMETableJobTest.importTestTable(contextName, inTab, "inTab");

		testLinearSampling(contextName, inTab, 0.1, 10, 0,
				CountMethods.Relative);

		testLinearSampling(contextName, inTab, 0.50, 50, 2,
				CountMethods.Relative);
	}

	//@Test
	public void absoluteLinearSamplingFromTable() throws Throwable {
		fail("TODO");

	}

	private void testLinearSampling(KNIMESparkContext contextName,
			Object[][] inTab, double numToSample, int aNumExpected,
			final int aIndex, CountMethods method)
			throws GenericKnimeSparkException, IOException,
			CanceledExecutionException {

		final String resTableName = "OutTab";
		String params = getParams("inTab", SamplingMethods.Linear, -1,method,
				false, false,99l, numToSample, resTableName, null);

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		Object[][] table = fetchResultTable(contextName, resTableName,
				aNumExpected, 5);
		// must sample first, last and every n-th row inbetween
		assertArrayEquals("first row must be in sample", inTab[0], table[0]);
		assertArrayEquals("last row must be in sample",
				inTab[inTab.length - 1], table[table.length - 1]);

	}

//	@Test(enabled=false)
//	public void linearSelector() throws Throwable {
//		assertTrue("first index must always be selected",
//				SamplingJob.isLinearlySelected(10, 0.1, 0));
//		assertTrue("last index must always be selected",
//				SamplingJob.isLinearlySelected(100, 0.1, 99));
//
//		// out of 10, 2 are already more than 10%
//		for (int i = 1; i < 9; i++) {
//			assertFalse("no further elements should be selected (" + i + ")",
//					SamplingJob.isLinearlySelected(10, 0.0, i));
//		}
//
//		// out of 10, we need 1 more for 25%
//		for (int i = 1; i < 8; i++) {
//			assertFalse("only one further elements should be selected (" + i
//					+ ")", SamplingJob.isLinearlySelected(10, 1d / 8, i));
//		}
//		assertTrue("second to last element should be selected as well",
//				SamplingJob.isLinearlySelected(10, 1d / 8, 8));
//		assertTrue("last element should be selected as well",
//				SamplingJob.isLinearlySelected(10, 1d / 8, 9));
//
//		// out of 32, we need 6 more for 25%
//		assertTrue("first index must always be selected (0/32)",
//				SamplingJob.isLinearlySelected(32, 6d / 30, 0));
//		for (int i = 0; i < 31; i++) {
//			assertEquals("every fifth element should be selected (" + i + ")",
//					i % 5 == 0, SamplingJob.isLinearlySelected(32, 6d / 30, i));
//		}
//		assertTrue("last index must always be selected (31/32)",
//				SamplingJob.isLinearlySelected(32, 6d / 30, 31));
//
//		// out of 37, we need 22 more for 63%
//		double fraction = (Math.ceil(37 * 0.63) - 2) / 35;
//		assertTrue("first index must always be selected (0/37)",
//				SamplingJob.isLinearlySelected(37, fraction, 0));
//		int nSelected = 0;
//		for (int i = 0; i < 37; i++) {
//			if (SamplingJob.isLinearlySelected(37, fraction, i)) {
//				nSelected++;
//			}
//		}
//		assertEquals("expected 63% selections", (int)Math.ceil(37 * 0.63), nSelected);
//		for (int i = 0; i < 36; i++) {
//			assertEquals("every fifth element should be selected (" + i + ")",
//					i % 5 == 0, SamplingJob.isLinearlySelected(37, fraction, i));
//		}
//		assertTrue("last index must always be selected (36/37)",
//				SamplingJob.isLinearlySelected(37, fraction, 37));
//
//	}

	private void sortAndTestRandomSampling(KNIMESparkContext contextName,
			Object[][] inTab, double numToSample, int aNumExpected,
			final int aIndex, CountMethods method)
			throws GenericKnimeSparkException, IOException,
			CanceledExecutionException {
		sortTable("inTab", aIndex, "inTabSorted");

		final String resTableName = "OutTab";
		String params = getParams("inTabSorted", SamplingMethods.Random,-1,
				method, false, false,99l, numToSample, resTableName, null);

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		Object[][] table = fetchResultTable(contextName, resTableName,
				aNumExpected, 3);
		// when sorted by key 0, then sort order is 0,1,2,3,4....
		int last = -1;
		for (int i = 0; i < table.length; i++) {
			final int val;
			if (table[i][aIndex] instanceof Integer) {
				val = (int) table[i][aIndex];
			} else {
				val = (char) table[i][aIndex];
			}
			assertTrue("values must increase " + i + ": " + val, val >= last);
			last = val;
		}
	}

	private void sortAndTestStratifiedSampling(KNIMESparkContext contextName,
			Object[][] inTab, double numToSample, int aNumExpected,
			final int aIndex, CountMethods method, final boolean aExact, final String aSplitTable)
			throws GenericKnimeSparkException, IOException,
			CanceledExecutionException {
		sortTable("inTab", aIndex, "inTabSorted");

		int expectedPingCtr = findClassDistribution(inTab);
		double expectedPing = ((double) expectedPingCtr) / inTab.length;

		final String resTableName = "OutTab";
		String params = getParams("inTabSorted", SamplingMethods.Stratified,1,
				method, false, aExact,99l, numToSample, resTableName, aSplitTable);

		String jobId = JobControler.startJob(contextName,
				SamplingJob.class.getCanonicalName(), params);

		JobControler.waitForJobAndFetchResult(contextName, jobId, null);

		Object[][] table = fetchResultTable(contextName, resTableName,
				aNumExpected, aExact ? 0 : 7);
		// when sorted by key 0, then sort order is 0,1,2,3,4....
		int last = -1;
		final int pingCtr = findClassDistribution(table);
		for (int i = 0; i < table.length; i++) {
			final int val;
			if (table[i][aIndex] instanceof Integer) {
				val = (int) table[i][aIndex];
			} else {
				val = (char) table[i][aIndex];
			}
			assertTrue("values must increase " + i + ": " + val, val >= last);
			last = val;
		}

		assertEquals("class distribution", expectedPing, ((double) pingCtr)
				/ table.length, aExact ? 0.001 : 0.02);
		
		if (aSplitTable != null) {
			//all other records must be in this table!
			Object[][] table2 = fetchResultTable(contextName, aSplitTable,
					inTab.length - table.length, 0);
			final int pingCtr2 = findClassDistribution(table2);
			assertEquals("class distribution should be same in both splits", expectedPing, ((double) pingCtr2)
					/ table2.length, aExact ? 0.001 : 0.02);
		}
	}

	/**
	 * @param aTab
	 * @return
	 */
	private int findClassDistribution(Object[][] aTab) {
		int expectedPingCtr = 0;
		for (int i = 0; i < aTab.length; i++) {
			if (aTab[i][1].equals("Ping")) {
				expectedPingCtr++;
			}
		}
		return expectedPingCtr;
	}

	private static void sortTable(final String aInputTable, final int aIndex,
			final String aOutputTable) throws GenericKnimeSparkException,
			IOException, CanceledExecutionException {
		final String aJarPath = Files.createTempFile("knimeJobUtils", "jar")
				.toString();

		final SparkJobCompiler testObj = new SparkJobCompiler();

		KnimeSparkJob job = testObj
				.addKnimeSparkJob2Jar(
						getJobJarPath(),
						aJarPath,
						"import org.apache.spark.api.java.function.Function;\nimport org.apache.spark.api.java.JavaRDD;\nimport org.apache.spark.sql.api.java.Row;",
						"",
						"JavaRDD<Row> in = getFromNamedRdds(\""
								+ aInputTable
								+ "\"); "
								+ "JavaRDD<Row> res = sortRDD(in, true,"
								+ aIndex
								+ "); "
								+ "addToNamedRdds(\""
								+ aOutputTable
								+ "\", res); "
								+ "return JobResult.emptyJobResult().withMessage(\"OK\");",
						"static JavaRDD<Row> sortRDD(final JavaRDD<Row> aRdd, final boolean aAscending, final int aIndex) {\n"
								+ "return aRdd.sortBy(new Function<Row, Object>() {\n"
								+ "private static final long serialVersionUID = 1L;\n"
								+ "@Override\n"
								+ "public Object call(Row aRow) throws Exception {return aRow.get(aIndex);}"
								+ "}, aAscending, aRdd.partitions().size());"
								+ "}");

		// upload jar to job-server
		JobControler.uploadJobJar(CONTEXT_ID, aJarPath);
		// start job
		String jobId = JobControler.startJob(CONTEXT_ID, job, "");
		JobControler.waitForJobAndFetchResult(CONTEXT_ID, jobId, null);
	}

	private void checkResult(final KNIMESparkContext aContextName,
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

	/**
	 * @param aContextName
	 * @param aResTableName
	 * @param aExpected
	 * @return
	 * @throws GenericKnimeSparkException
	 * @throws CanceledExecutionException
	 */
	private Object[][] fetchResultTable(final KNIMESparkContext aContextName,
			final String aResTableName, final int aExpectedLength)
			throws GenericKnimeSparkException, CanceledExecutionException {
		return fetchResultTable(aContextName, aResTableName, aExpectedLength, 0);
	}

	private Object[][] fetchResultTable(final KNIMESparkContext aContextName,
			final String aResTableName, final int aExpectedLength,
			final double aAllowedPercentageOff) throws GenericKnimeSparkException,
			CanceledExecutionException {
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

	private static final Object[][] TEST_TABLE_1 = new Object[][] {
			new Object[] { 1, "Ping", "my string" },
			new Object[] { 1, "Pong", "my string" },
			new Object[] { 1, "Ping2", "my other string" },
			new Object[] { 1, "Pong2", "my other string" } };

	private static final Object[][] TEST_TABLE_2 = new Object[][] {
			new Object[] { 1, "Ping", "z" }, new Object[] { 1, "Pong", "y" },
			new Object[] { 1, "Ping2", "x" }, new Object[] { 1, "Pong2", "w" } };

	private static final Object[][] getTable3(final int aNRows) {
		Object[][] table3 = new Object[aNRows][];
		for (int i = 0; i < aNRows; i++) {
			table3[i] = new Object[] { i, (i % 10 == 0) ? "Ping" : "Pong",
					(char) Math.abs(102 - i) };
		}
		return table3;
	}

	private String rowFetcherDef(final int aNumRows, final String aTableName) {
		return JsonUtils.asJson(new Object[] {
				ParameterConstants.PARAM_INPUT,
				new String[] { ParameterConstants.PARAM_NUMBER_ROWS,
						"" + aNumRows, ParameterConstants.PARAM_TABLE_1,
						aTableName } });
	}

}