package com.knime.bigdata.spark.node.mllib.prediction.decisiontree;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.DecisionTreeLearner;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.NominalFeatureInfo;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class DecisionTreeTaskTest extends SparkWithJobServerSpec {

//	public static String paramsAsJason(final String aInputTableName, final int aLabelColIndex,
//			final Integer[] aNumericColIdx, final double aLambda, final String aResult) {
//		return DecisionTreeTask.paramsAsJason(aInputTableName, aLabelColIndex, aNumericColIdx, aLambda,
//				aResult);
//	}

	        
	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		DecisionTreeTask testObj = new DecisionTreeTask(null, "inputRDD", new Integer[] { 0, 1 }, new NominalFeatureInfo(), 1, 2l, 1, 2, "qa");
		final String params = testObj.learnerDef();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new DecisionTreeLearner().validate(config));
	}

	@Test
	public void createDecisionTreeFromEntirelyNumericDataWithoutNominalFeatureInfo()
			throws Throwable {
		
		final Serializable[][] MINI_IRIS_TABLE = new Serializable[][] {
				{ 5.1, 3.5, 1.4, 1},
				{ 4.9, 3.0, 1.4, 0 },
				{ 4.7, 3.2, 1.3, 2},
				{ 4.6, 3.1, 1.5, 1 } };
		
		KNIMESparkContext context = KnimeContext.getSparkContext();
		ImportKNIMETableJobTest.importTestTable(context, MINI_IRIS_TABLE,
				"tab1");

		//data must be entirely numeric!
		final Integer[] cols = new Integer[] { 0, 2, 1 };
		DecisionTreeTask testObj = new DecisionTreeTask(context, "tab1",  cols, new NominalFeatureInfo(), 3, 3l, 11, 12, DecisionTreeLearner.VALUE_GINI);

		DecisionTreeModel model = testObj.execute(null);
		assertTrue(model != null);
		// not sure what else to check here....
		

	}	
}
