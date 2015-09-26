package com.knime.bigdata.spark.node.statistics.compute;

import static org.junit.Assert.assertEquals;

import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.StatisticsJob;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.statistics.compute.StatisticsTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class StatisticsTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final Integer[] aColIds) {
		return StatisticsTask.paramsAsJason(aInputTableName, aColIds);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		StatisticsTask testObj = new StatisticsTask(null, "inputRDD",
				new Integer[] { 0, 1, 6 });
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new StatisticsJob().validate(config));
	}

	@Test
	public void statisticsOf2Indices() throws Throwable {

		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");
		StatisticsTask testObj = new StatisticsTask(context, "tab1",
				new Integer[] { 0, 1 });

		final MultivariateStatisticalSummary stats = testObj.execute(null);
		assertEquals(MINI_IRIS_TABLE.length, stats.count());
		final double[] max = stats.max().toArray();
		assertEquals("stats should only be computed for two values", 2,
				max.length);
		assertEquals(5.1, max[0], 0.0001d);
		assertEquals(3.5, max[1], 0.0001d);
	}

	@Test
	public void statisticsOf1Index() throws Throwable {

		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");
		StatisticsTask testObj = new StatisticsTask(context, "tab1",
				new Integer[] { 3 });

		final MultivariateStatisticalSummary stats = testObj.execute(null);
		assertEquals(MINI_IRIS_TABLE.length, stats.count());
		final double[] var = stats.variance().toArray();
		assertEquals("stats should only be computed for single value", 1,
				var.length);
		assertEquals(0, var[0], 0.0001d);
	}

	@Test
	public void statisticsOfAllNumericIndices() throws Throwable {

		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");
		Integer[] indices = new Integer[] { 0, 1, 2, 3 };
		StatisticsTask testObj = new StatisticsTask(context, "tab1", indices);

		final MultivariateStatisticalSummary stats = testObj.execute(null);
		assertEquals(MINI_IRIS_TABLE.length, stats.count());
		final double[] tmp = stats.numNonzeros().toArray();
		assertEquals("stats should be computed for four columns",
				indices.length, tmp.length);
		assertEquals("there are no null values", MINI_IRIS_TABLE.length, tmp[2], 0.00001d);
	}
}
