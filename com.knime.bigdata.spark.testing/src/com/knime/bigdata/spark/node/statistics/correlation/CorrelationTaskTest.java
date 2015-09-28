package com.knime.bigdata.spark.node.statistics.correlation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.knime.base.util.HalfDoubleMatrix;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.CorrelationJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.EnumContainer.CorrelationMethods;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class CorrelationTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final Integer[] aColIds, final CorrelationMethods aMethod,
			final String aResTable) {
		return CorrelationTask.paramsAsJason(aInputTableName, aColIds, aMethod,
				aResTable, false);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		CorrelationTask testObj = new CorrelationTask(null, "inputRDD",
				new Integer[] { 0, 1, 6 }, CorrelationMethods.pearson, "out",
				false);
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new CorrelationJob().validate(config));
	}

	@Test
	public void correlationOf2Indices() throws Throwable {

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");
		CorrelationTask testObj = new CorrelationTask(CONTEXT_ID, "tab1",
				new Integer[] { 0, 1 }, CorrelationMethods.pearson, null, false);

		final double correlation = testObj.execute(null).get(0, 0);
		assertTrue(correlation > -0.99999 && correlation < 1.00001);
	}

	@Test
	public void correlationOfIndexWithItself() throws Throwable {

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");
		CorrelationTask testObj = new CorrelationTask(CONTEXT_ID, "tab1",
				new Integer[] { 2, 2 }, CorrelationMethods.spearman, null,
				false);

		final double correlation = testObj.execute(null).get(0, 0);
		assertEquals(correlation, 1, 0.001d);
	}

	@Test
	public void correlationOfAllNumericIndices() throws Throwable {

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");
		Integer[] indices = new Integer[] { 0, 1, 2, 3 };
		CorrelationTask testObj = new CorrelationTask(CONTEXT_ID, "tab1",
				indices, CorrelationMethods.pearson, "out", false);

		final double correlation = testObj.execute(null).get(0, 0);
		assertEquals(
				"no single sensible value should be returned when correlation is computed for more than 2 indices",
				Double.MIN_VALUE, correlation, 0.00001d);

		Object[][] arrayRes = fetchResultTable(CONTEXT_ID, "out",
				indices.length);
		int i = 0;
		for (; i < indices.length; i++) {
			assertEquals(
					"number of columns must equal number of selected indices",
					indices.length, arrayRes[i].length);
			// diagonal must be 1
			assertEquals("cell[" + i + ",0]", 1d, (Double) arrayRes[i][i],
					0.00001d);
			// correlation is symmetric
			for (int j = 0; j < indices.length; j++) {
				assertEquals("cell[" + i + "," + j + "]",
						(Double) arrayRes[j][i], (Double) arrayRes[i][j],
						0.00001d);
			}
		}
	}

	@Test
	public void correlationOfAllNumericIndicesWithHDMatrix() throws Throwable {

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");
		Integer[] indices = new Integer[] { 0, 1, 2, 3, 1, 2 };
		CorrelationTask testObj = new CorrelationTask(CONTEXT_ID, "tab1",
				indices, CorrelationMethods.pearson, "out", true);

		final HalfDoubleMatrix correlation = testObj.execute(null);
		assertEquals(
				"full matrix should be returned when correlation is computed for more than 2 indices",
				indices.length, correlation.getRowCount(), 0.00001d);

		for (int i = 0; i < indices.length; i++) {
			// correlation is symmetric
			for (int j = 0; j < indices.length; j++) {
				if (i != j) {
					assertEquals("cell[" + i + "," + j + "]",
							correlation.get(i, j), correlation.get(j, i),
							0.00001d);
				}
			}
		}
		// 1 and 4 as well as 2 and 5 are identical
		assertEquals("identical cell[" + 1 + "," + 4 + "]", 1d, correlation.get(1, 4),
				0.00001d);
		assertEquals("identical cell[" + 2 + "," + 5 + "]", 1d, correlation.get(2, 5),
				0.00001d);
		assertTrue("not idential: cell[" + 1 + "," + 2 + "]", correlation.get(1, 2) < 0.95);

	}

}
