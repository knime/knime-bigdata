package com.knime.bigdata.spark.node.preproc.filter.column;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ColumnSelectionJob;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.preproc.filter.column.ColumnFilterTask;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ColumnSelectionTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final Integer[] aNumericColIdx, final String aResTable) {
		return ColumnFilterTask.paramsAsJason(aInputTableName,
				aNumericColIdx, aResTable);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		ColumnFilterTask testObj = new ColumnFilterTask(null, "inputRDD",
				new Integer[] { 0, 1 }, "u");
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new ColumnSelectionJob().validate(config));
	}

	@Test
	public void verifyThatColumnsAreSelectedFromInputRDD() throws Throwable {
		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");

		ColumnFilterTask testObj = new ColumnFilterTask(context, "tab1",
				new Integer[] { 4, 0, 3, 1 }, "out");

		testObj.execute(null);

		// check that columns 4, 0, 3, and 1 are selected, in that order
		Object[][] arrayResV = fetchResultTable(context, "out",
				MINI_IRIS_TABLE.length);
		for (int i=0; i<MINI_IRIS_TABLE.length; i++) {
			assertEquals("row length", 4, arrayResV[i].length);
			assertEquals("cell ["+i+",0]:", MINI_IRIS_TABLE[i][0], arrayResV[i][1]);
			assertEquals("cell ["+i+",1]:", MINI_IRIS_TABLE[i][1], arrayResV[i][3]);
			assertEquals("cell ["+i+",3]:", MINI_IRIS_TABLE[i][3], arrayResV[i][2]);
			assertEquals("cell ["+i+",4]:", MINI_IRIS_TABLE[i][4], arrayResV[i][0]);
		}
	}

}
