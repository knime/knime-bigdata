package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;

import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.mllib.CollaborativeFilteringTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class CollaborativeFilteringJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason(null, 1,
				2, 3, 8d, "U");
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingUserParameter()
			throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab", null,
				2, 3, 8d, "U");
		myCheck(params, CollaborativeFilteringJob.PARAM_USER_INDEX, "Input");
	}
	
	@Test
	public void jobValidationShouldCheckMissingProductParameter()
			throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab", 
				2, null,3, 8d, "U");
		myCheck(params, CollaborativeFilteringJob.PARAM_PRODUCT_INDEX, "Input");
	}
	
	@Test
	public void jobValidationShouldCheckMissingRatingParameter()
			throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab", 
				2, 3, null,8d, "U");
		myCheck(params, CollaborativeFilteringJob.PARAM_RATING_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab1", 1,
				2, 3, 5d, "U");
		KnimeSparkJob testObj = new CollaborativeFilteringJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private static class MyMapper implements Serializable {
		private static final long serialVersionUID = 1L;

		private JavaRDD<Rating> getTestRdd(JavaDoubleRDD o) {
			return o.map(new Function<Double, Rating>() {
				private static final long serialVersionUID = 1L;
				private int ix = 0;

				@Override
				public Rating call(final Double x) {
					ix = ix + 1;
					return new Rating(ix, ix * 2, x);
				}
			});
		}
	}

	@Test
	public void CollaborativeFilteringOfSimpleRowRDD() throws Throwable {
		JavaDoubleRDD o = getRandomDoubleRDD(100, 2);
		final double lambda = 0.2;
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab1", 4,
				2, 3, lambda, "U");
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		MatrixFactorizationModel model = CollaborativeFilteringJob.execute(
				super.sparkContextResource.sparkContext.sc(), config2,
				new MyMapper().getTestRdd(o));
		assertTrue("model expected", model != null);

	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new CollaborativeFilteringJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

}