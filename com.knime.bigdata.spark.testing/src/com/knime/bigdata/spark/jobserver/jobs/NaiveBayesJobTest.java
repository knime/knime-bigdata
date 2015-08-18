package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.prediction.bayes.naive.NaiveBayesTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class NaiveBayesJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = NaiveBayesTaskTest.paramsAsJason(null, 1, new Integer[] { 0, 1 },
				8, "U");
		myCheck(params, ParameterConstants.PARAM_TABLE_1, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingLabelIndexParameter()
			throws Throwable {
		String params = NaiveBayesTaskTest.paramsAsJason("tab", 1, new Integer[] { 0, 1 },
				8, "U");
		myCheck(params, ParameterConstants.PARAM_LABEL_INDEX, "Input");
	}
	
	@Test
	public void jobValidationShouldCheckKParameter() throws Throwable {
		for (Double lambda : new double[] { -1.1, 0.0d, 1.5d, 3d }) {
			String params = NaiveBayesTaskTest.paramsAsJason("tab1", 1, new Integer[] {
					0, 1 }, lambda, "U");
			Config config = ConfigFactory.parseString(params);
			JobConfig config2 = new JobConfig(config);
			assertEquals("'lambda' parameter not set", lambda, NaiveBayesJob.getLambda(config2));
		}
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = NaiveBayesTaskTest.paramsAsJason("tab1",1, 
				new Integer[] { 0, 1 }, 5, "U");
		KnimeSparkJob testObj = new NaiveBayesJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private static class MyMapper implements Serializable {
		private static final long serialVersionUID = 1L;

		private JavaRDD<LabeledPoint> getTestRdd(JavaDoubleRDD o) {
			return o.map(new Function<Double, LabeledPoint>() {
				private static final long serialVersionUID = 1L;
				private int ix = 0;

				@Override
				public LabeledPoint call(final Double x) {
					ix = ix + 1;
					double val = Math.abs(x);
					double[] v = new double[] {val, 2.0 * val, ix, val + 14};
					return new LabeledPoint(ix, Vectors.dense(v));
				}
			});
		}
	}

	@Test
	public void NaiveBayesOfSimpleRowRDD() throws Throwable {
		JavaDoubleRDD o = getRandomDoubleRDD(100, 2);
		final double lambda = 0.2;
		String params = NaiveBayesTaskTest.paramsAsJason("tab1",4,
				new Integer[] { 0, 1 }, lambda, "U");
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		NaiveBayesModel model = NaiveBayesJob.execute(super.sparkContextResource.sparkContext.sc(), config2, new MyMapper().getTestRdd(o));
		assertTrue("model expected", model != null);
		
	}


	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new NaiveBayesJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}