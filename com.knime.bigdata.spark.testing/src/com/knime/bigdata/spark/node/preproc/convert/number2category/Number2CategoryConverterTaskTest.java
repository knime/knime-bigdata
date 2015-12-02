package com.knime.bigdata.spark.node.preproc.convert.number2category;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.MapValuesJob;
import com.knime.bigdata.spark.jobserver.server.ColumnBasedValueMapping;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class Number2CategoryConverterTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName,
			final ColumnBasedValueMapping aMap, final String aOutputRDD)
			throws GenericKnimeSparkException {
		return Number2CategoryConverterTask.paramDef(aInputTableName, aMap, true, 
				aOutputRDD);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		ColumnBasedValueMapping map = new ColumnBasedValueMapping();
		Number2CategoryConverterTask testObj = new Number2CategoryConverterTask(
				null, "inputRDD", map,  true, "outputRDD");
		final String params = testObj.paramDef();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new MapValuesJob().validate(config));
	}

	@Test
	public void verifyThatValuesAreMapped()
			throws Throwable {
		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		ColumnBasedValueMapping map = new ColumnBasedValueMapping();
		map.add(0, MINI_IRIS_TABLE[0][0], "0-0");
		map.add(0, MINI_IRIS_TABLE[1][0], "0-0");
		map.add(0, MINI_IRIS_TABLE[2][0], "0-0");
		map.add(0, MINI_IRIS_TABLE[3][0], "0-0");
		map.add(4, "Iris-setosa", "II");
		map.add(4, "Iris-versicolor", "II");
		map.add(4, "Iris-virginica", "II");
		Number2CategoryConverterTask testObj = new Number2CategoryConverterTask(
				CONTEXT_ID, "tab1", map, true, "outTab");

		testObj.execute(null);

		// not sure what else to check here....
		Object[][] arrayRes = fetchResultTable(CONTEXT_ID, "outTab", 4);
		for (int i=0; i< arrayRes.length; i++) {
			Object[] row = arrayRes[i];
			assertEquals("column 0 all mapped to ", "0-0", row[5]);
			assertEquals("column 1 all mapped to ", "II", row[6]);
		}
	}
	
	@Test
	public void verifyThatValuesAreMappedAndOrigColsAreDropped()
			throws Throwable {
		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, MINI_IRIS_TABLE,
				"tab1");

		ColumnBasedValueMapping map = new ColumnBasedValueMapping();
		map.add(0, MINI_IRIS_TABLE[0][0], "0-0");
		map.add(0, MINI_IRIS_TABLE[1][0], "0-0");
		map.add(0, MINI_IRIS_TABLE[2][0], "0-0");
		map.add(0, MINI_IRIS_TABLE[3][0], "0-0");
		map.add(4, "Iris-setosa", "II");
		map.add(4, "Iris-versicolor", "II");
		map.add(4, "Iris-virginica", "II");
		Number2CategoryConverterTask testObj = new Number2CategoryConverterTask(
				CONTEXT_ID, "tab1", map, false, "outTab");

		testObj.execute(null);

		// not sure what else to check here....
		Object[][] arrayRes = fetchResultTable(CONTEXT_ID, "outTab", 4);
		for (int i=0; i< arrayRes.length; i++) {
			Object[] row = arrayRes[i];
			assertEquals("only mapped cols should remain ", 5, row.length);
			
			assertEquals("column 0 all mapped to ", "0-0", row[3]);
			assertEquals("column 1 all mapped to ", "II", row[4]);
		}
	}

}
