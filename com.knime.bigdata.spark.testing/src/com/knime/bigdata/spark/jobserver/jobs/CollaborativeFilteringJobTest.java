package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ModelUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtils;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJava;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.jobserver.server.transformation.RowBuilder;
import com.knime.bigdata.spark.node.mllib.collaborativefiltering.CollaborativeFilteringTaskTest;
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
				2, 3, 8d);
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingUserParameter() throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab",
				null, 2, 3, 8d);
		myCheck(params, CollaborativeFilteringJob.PARAM_USER_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingProductParameter()
			throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab", 2,
				null, 3, 8d);
		myCheck(params, CollaborativeFilteringJob.PARAM_PRODUCT_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissingRatingParameter()
			throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab", 2,
				3, null, 8d);
		myCheck(params, CollaborativeFilteringJob.PARAM_RATING_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab1", 1,
				2, 3, 5d);
		KnimeSparkJob testObj = new CollaborativeFilteringJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private static class MyMapper implements Serializable {
		private static final long serialVersionUID = 1L;

		private JavaRDD<Row> getTestRdd(JavaSparkContext sparkContext, final int aNumUsers,
				final int aNumRatings, final int aNumProducts) {
			List<Row> rows = new ArrayList<>();
			for (int u = 0; u < aNumUsers; u++) {
				for (int p = 0; p < aNumProducts; p++) {
					if (Math.random() <= ((float) aNumRatings) / aNumProducts) {
						RowBuilder rb = RowBuilder.emptyRow();
						rb.add(u).add(p).add((int) (5 * Math.random()) + 1.0);
						rows.add(rb.build());
					}
				}
			}
			return sparkContext.parallelize(rows);
//			return o.zipWithIndex().map(
//					new Function<Tuple2<Double, Long>, Row>() {
//						private static final long serialVersionUID = 1L;
//
//						@Override
//						public Row call(Tuple2<Double, Long> arg0)
//								throws Exception {
//							RowBuilder rb = RowBuilder.emptyRow();
//							rb.add(arg0._2.intValue())
//									.add(arg0._2.intValue() * 2).add(arg0._1);
//							return rb.build();
//						}
//					});
		}
	}

	@Test
	public void CollaborativeFilteringOfSimpleRowRDD() throws Throwable {
		
		final double lambda = 0.2;
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab1", 0,
				1, 2, lambda);
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		JavaRDD<Rating> ratings = RDDUtilsInJava.convertRowRDD2RatingsRdd(0, 1,
				2, new MyMapper().getTestRdd(sparkContextResource.sparkContext, 100, 5, 10));
		MatrixFactorizationModel model = CollaborativeFilteringJob.learn(
				super.sparkContextResource.sparkContext.sc(), config2, ratings);
		assertTrue("model expected", model != null);

	}

	private static class MyCollaborativeFilteringJob extends
			CollaborativeFilteringJob {
		private static final long serialVersionUID = 1L;
		public JavaRDD<Row> mPredictions;

		protected void predict(final JobConfig aConfig,
				final JavaRDD<Row> aRowRDD, final JavaRDD<Rating> aRatings,
				final MatrixFactorizationModel aServerModel) {
			final int userIdx = aConfig.getInputParameter(PARAM_USER_INDEX,
					Integer.class);
			final int productIdx = aConfig.getInputParameter(
					PARAM_PRODUCT_INDEX, Integer.class);
			mPredictions = ModelUtils.predict(aRowRDD, aRatings, userIdx,
					productIdx, aServerModel);
		}
	};

	@Test
	public void CollaborativeFilteringOfSimpleRowRDDWithPredict()
			throws Throwable {
		final double lambda = 0.2;
		String params = CollaborativeFilteringTaskTest.paramsAsJason("tab1", 0,
				1, 2, lambda, "out");
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);

		MyCollaborativeFilteringJob job = new MyCollaborativeFilteringJob();
		JavaRDD<Row> rows = new MyMapper().getTestRdd(sparkContextResource.sparkContext, 100, 5, 10);
		JavaRDD<Rating> ratings = RDDUtilsInJava.convertRowRDD2RatingsRdd(0, 1,
				2, rows);
		job.execute(super.sparkContextResource.sparkContext.sc(), config2,
				rows, ratings);
		assertTrue("prediction expected", job.mPredictions != null);
		List<Row> predictions = job.mPredictions.collect();
		assertEquals("there should be one prediction for each training record",
				rows.count(), predictions.size());
		int numErrors = 0;
		for (Row r : predictions) {
			if (Math.round(RDDUtils.getDouble(r, 2)) != Math.round(RDDUtils.getDouble(r, 3))) {
				numErrors++;
			}
		}
		assertTrue("unexpected large number of errors", numErrors < 0.01*predictions.size());
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