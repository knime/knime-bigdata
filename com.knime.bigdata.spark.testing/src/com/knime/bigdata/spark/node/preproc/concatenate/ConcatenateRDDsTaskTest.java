package com.knime.bigdata.spark.node.preproc.concatenate;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ConcatenateRDDsJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.concatenate.ConcatenateRDDsTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ConcatenateRDDsTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String[] aInputTableNames,
			final String aResTable) {
		return ConcatenateRDDsTask.paramsAsJason(aInputTableNames, aResTable);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		ConcatenateRDDsTask testObj = new ConcatenateRDDsTask(null,
				new String[] { "inputRDD1", "inputRDD2" }, "out");
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new ConcatenateRDDsJob().validate(config));
	}

	@Test
	public void verifyThatInputRDDsAreAppended() throws Throwable {

		final Serializable[][] MINI_IRIS_TABLE2 = new Serializable[][] {
				{ 2, 3.5, 1.4, 0.2, "Iris-setosa" },
				{ 2, 3.0, 1.4, 0.2, "Iris-setosa" },
				{ 2, 3.1, 1.5, 0.2, "Iris-virginica" } };

		final Serializable[][] MINI_IRIS_TABLE3 = new Serializable[][] {
				{ 3, 3.5, 1.4, 0.2, "Iris-setosa" },
				{ 3, 3.0, 1.4, 0.2, "Iris-setosa" },
				{ 3, 3.5, 1.4, 0.2, "Iris-setosa" },
				{ 3, 3.0, 1.4, 0.2, "Iris-setosa" },
				{ 3, 3.2, 1.3, 0.2, "Iris-versicolor" },
				{ 3, 3.1, 1.5, 0.2, "Iris-virginica" } };

		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE2,
				"tab2");
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE3,
				"tab3");
		ConcatenateRDDsTask testObj = new ConcatenateRDDsTask(context,
				new String[] { "tab1", "tab2", "tab3" }, "out");

		testObj.execute(null);

		Object[][] arrayRes = fetchResultTable(context, "out",
				MINI_IRIS_TABLE.length + MINI_IRIS_TABLE2.length
						+ MINI_IRIS_TABLE3.length);
		int i = 0;
		for (; i < MINI_IRIS_TABLE.length; i++) {
			assertEquals("table 1, cell[" + i + ",0]", MINI_IRIS_TABLE[i][0],
					arrayRes[i][0]);
		}
		for (; i < MINI_IRIS_TABLE.length + MINI_IRIS_TABLE2.length; i++) {
			assertEquals("table 2, cell[" + i + ",0]", 2, arrayRes[i][0]);
		}
		for (; i < MINI_IRIS_TABLE.length + MINI_IRIS_TABLE2.length
				+ MINI_IRIS_TABLE3.length; i++) {
			assertEquals("table 3, cell[" + i + ",0]", 3, arrayRes[i][0]);
		}
	}

}
