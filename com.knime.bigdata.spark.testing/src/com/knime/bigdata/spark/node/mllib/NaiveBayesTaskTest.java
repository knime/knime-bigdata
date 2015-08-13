package com.knime.bigdata.spark.node.mllib;

import static org.junit.Assert.assertEquals;

import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.NaiveBayesJob;
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
public class NaiveBayesTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputTableName, final int aLabelColIndex,
			final Integer[] aNumericColIdx, final double aLambda, final String aResult) {
		return NaiveBayesTask.paramsAsJason(aInputTableName, aLabelColIndex, aNumericColIdx, aLambda,
				aResult);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		NaiveBayesTask testObj = new NaiveBayesTask(null, "inputRDD", 4, new Integer[] { 0, 1 },
				2, "u");
		final String params = testObj.paramsAsJason();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new NaiveBayesJob().validate(config));
	}

	@Test
	public void verifyThatNaiveBayesJobStoresResultMatrixAsNamedRDDAndReturnsVector()
			throws Throwable {
		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");

		//data must be entirely numeric!
		final double l = 0.3;
		final Integer[] cols = new Integer[] { 0, 1, 3 };
		NaiveBayesTask testObj = new NaiveBayesTask(context, "tab1", 2, cols, l, "u");

		NaiveBayesModel model = testObj.execute(null);
		
		// not sure what else to check here....
		fetchResultTable(context, "u", 4);

	}	
}
