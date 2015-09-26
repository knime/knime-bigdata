package com.knime.bigdata.spark.node.preproc.sorter;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.SortJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class SortTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final Integer[] aColIdx, final Boolean[] aIsAscending,
			final String aResTable) {
		return SortTask.paramsAsJason(aInputTableName, aColIdx, aIsAscending, false, aResTable);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final SortTask testObj = new SortTask(null, "inputRDD",
				new Integer[] { 0, 1 }, new Boolean[] { true, false }, false, "out");
		final String params = testObj.paramsAsJason();
		final JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new SortJob().validate(config));
	}

	@Test
	public void sortBySingleColumnInAscendingOrder() throws Throwable {

		final Serializable[][] UNSORTED_MINI_IRIS_TABLE = new Serializable[][] {
				{ 4.6, 4.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.7, 3.2, 1.3, 0.2, "Iris-versicolor" },
				{ 4.9, 5.0, 1.4, 0.2, "Iris-setosa" },
				{ 5.1, 2.5, 1.4, 0.2, "Iris-setosa" } };

		final Serializable[][] SORTED_MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 2.5, 1.4, 0.2, "Iris-setosa" },
				{ 4.7, 3.2, 1.3, 0.2, "Iris-versicolor" },
				{ 4.6, 4.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.9, 5.0, 1.4, 0.2, "Iris-setosa" } };

		final KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, UNSORTED_MINI_IRIS_TABLE,
				"tab1");
		final SortTask testObj = new SortTask(context, "tab1",
				new Integer[] { 1 }, new Boolean[] { true }, false, "out");

		testObj.execute(null);

		final Object[][] arrayRes = fetchResultTable(context, "out",
				UNSORTED_MINI_IRIS_TABLE.length);
		int i = 0;
		for (; i < SORTED_MINI_IRIS_TABLE.length; i++) {
			assertEquals("number of columns must not change", 5,
					arrayRes[i].length);
			assertEquals("table 1, cell[" + i + ",0]",
					SORTED_MINI_IRIS_TABLE[i][0], arrayRes[i][0]);
		}
	}

	@Test
	public void sortBySingleColumnInDescendingOrder() throws Throwable {

		final Serializable[][] UNSORTED_MINI_IRIS_TABLE = new Serializable[][] {
				{ 4.6, 4.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.7, 3.2, 1.3, 0.2, "Iris-versicolor" },
				{ 4.9, 5.0, 1.4, 0.2, "Iris-setosa" },
				{ 5.1, 2.5, 1.4, 0.2, "Iris-setosa" } };

		final Serializable[][] SORTED_MINI_IRIS_TABLE = new Serializable[][] {
				{ 4.9, 5.0, 1.4, 0.2, "Iris-setosa" },
				{ 4.6, 4.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.7, 3.2, 1.3, 0.2, "Iris-versicolor" },
				{ 5.1, 2.5, 1.4, 0.2, "Iris-setosa" } };

		final KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, UNSORTED_MINI_IRIS_TABLE,
				"tab1");
		final SortTask testObj = new SortTask(context, "tab1",
				new Integer[] { 1 }, new Boolean[] { false }, false, "out");

		testObj.execute(null);

		final Object[][] arrayRes = fetchResultTable(context, "out",
				UNSORTED_MINI_IRIS_TABLE.length);
		int i = 0;
		for (; i < SORTED_MINI_IRIS_TABLE.length; i++) {
			assertEquals("number of columns must not change", 5,
					arrayRes[i].length);
			assertEquals("table 1, cell[" + i + ",0]",
					SORTED_MINI_IRIS_TABLE[i][0], arrayRes[i][0]);
		}
	}

	@Test
	public void verifyThatColumnsAreSorted() throws Throwable {

		final Serializable[][] SORTED_MINI_IRIS_TABLE = new Serializable[][] {
				{ 4.6, 3.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.7, 3.2, 1.3, 0.2, "Iris-versicolor" },
				{ 4.9, 3.0, 1.4, 0.2, "Iris-setosa" },
				{ 5.1, 3.5, 1.4, 0.2, "Iris-setosa" } };

		final KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");
		final SortTask testObj = new SortTask(context, "tab1",
				new Integer[] { 4, 0 }, new Boolean[] { false, true }, false, "out");

		testObj.execute(null);

		final Object[][] arrayRes = fetchResultTable(context, "out",
				MINI_IRIS_TABLE.length);
		int i = 0;
		for (; i < SORTED_MINI_IRIS_TABLE.length; i++) {
			assertEquals("number of columns must not change", 5,
					arrayRes[i].length);
			assertEquals("table 1, cell[" + i + ",0]",
					SORTED_MINI_IRIS_TABLE[i][0], arrayRes[i][0]);
		}

	}


	@Test
	public void sortColumnsWithNullValues() throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE_With_NULL = new Serializable[][] {
				{ 4.9, 3.0, 1.4, 0.1, "Iris-setosa" },
				{ 4.6, 3.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.7, null, 1.3, 0.2, "Iris-versicolor" },
				{ 5.1, null, 1.4, 0.1, "Iris-setosa" } };

		final Serializable[][] SORTED_MINI_IRIS_TABLE = new Serializable[][] {
				{ 4.6, 3.1, 1.5, 0.2, "Iris-virginica" },
				{ 4.7, null, 1.3, 0.2, "Iris-versicolor" },
				{ 4.9, 3.0, 1.4, 0.1, "Iris-setosa" },
				{ 5.1, null, 1.4, 0.1, "Iris-setosa" } };

		final KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE_With_NULL,
				"tab1");
		final SortTask testObj = new SortTask(context, "tab1",
				new Integer[] { 3, 1 }, new Boolean[] { false, true }, false, "out");

		testObj.execute(null);

		final Object[][] arrayRes = fetchResultTable(context, "out",
				MINI_IRIS_TABLE_With_NULL.length);
		int i = 0;
		for (; i < SORTED_MINI_IRIS_TABLE.length; i++) {
			assertEquals("number of columns must not change", 5,
					arrayRes[i].length);
			assertEquals("table 1, cell[" + i + ",0]",
					SORTED_MINI_IRIS_TABLE[i][0], arrayRes[i][0]);
		}

	}
}
