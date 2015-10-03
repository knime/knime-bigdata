package com.knime.bigdata.spark.jobserver.jobs;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.summary.SumOfSquares;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.api.java.Row;
import org.junit.Test;

import com.knime.bigdata.spark.LocalSparkSpec;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.NumericScorerData;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJavaTest;
import com.knime.bigdata.spark.jobserver.server.ScorerData;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.node.scorer.accuracy.ScorerTaskTest;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ScorerJobTest extends LocalSparkSpec {

	@Test
	public void jobValidationShouldCheckMissingInputDataParameter()
			throws Throwable {
		String params = ScorerTaskTest.paramsAsJason(null, 1, 2, true);
		myCheck(params, KnimeSparkJob.PARAM_INPUT_TABLE, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_ACTUAL_COL_INDEX_Parameter()
			throws Throwable {
		String params = ScorerTaskTest.paramsAsJason("in", null, 2, true);
		myCheck(params, ScorerJob.PARAM_ACTUAL_COL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PRED_COL_INDEX_Parameter()
			throws Throwable {
		String params = ScorerTaskTest.paramsAsJason("in", 1, null, true);
		myCheck(params, ScorerJob.PARAM_PREDICTION_COL_INDEX, "Input");
	}

	@Test
	public void jobValidationShouldCheckMissing_PARAM_IS_CLASSIFICATION_Parameter()
			throws Throwable {
		String params = ScorerTaskTest.paramsAsJason("in", 1, 2, null);
		myCheck(params, ScorerJob.PARAM_IS_CLASSIFICATION, "Input");
	}

	@Test
	public void jobValidationShouldCheckAllValidParams() throws Throwable {
		String params = ScorerTaskTest.paramsAsJason("in", 1, 2, true);
		KnimeSparkJob testObj = new ScorerJob();
		Config config = ConfigFactory.parseString(params);
		JobConfig config2 = new JobConfig(config);
		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(), testObj.validate(config2));
	}

	private void myCheck(final String params, final String aParam,
			final String aPrefix) {
		myCheck(params, aPrefix + " parameter '" + aParam + "' missing.");
	}

	private void myCheck(final String params, final String aMsg) {
		KnimeSparkJob testObj = new ScorerJob();
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));
		assertEquals("Configuration should be recognized as invalid",
				ValidationResultConverter.invalid(aMsg),
				testObj.validate(config));
	}

	@Test
	public void computeScoresForClassification() throws Throwable {

		JavaDoubleRDD o = getRandomDoubleRDD(100L, 2);
		JavaRDD<Row> rowRDD = new RDDUtilsInJavaTest.MyMapper()
				.toRowRddWithSomeTeamChanges(o, 0.25d).cache();
		String params = ScorerTaskTest.paramsAsJason("in", 0, 1, true);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		ScorerData res = (ScorerData) ScorerJob.scoreClassification(config,
				rowRDD);
		evaluateScores(rowRDD.count(), rowRDD.collect(), res);

	}

	/**
	 * @param aRowRDD
	 * @param rows
	 * @param aScores
	 */
	public static void evaluateScores(final long aCount, final List<Row> rows,
			ScorerData aScores) {
		final String[][] values = new String[rows.size()][2];
		int i = 0;
		for (Row row : rows) {
			values[i][0] = row.getString(0);
			values[i][1] = row.getString(1);
			i++;
		}
		ScorerTaskTest.evaluateScores(aCount, values, aScores);
	}

	private static JavaRDD<Row> getData(JavaDoubleRDD aRandomDoubleRDD) {
		return aRandomDoubleRDD.map(new Function<Double, Row>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Row call(final Double x) {
				return Row.create(x, Math.random());
			}
		}).cache();
	}

	@Test
	public void computeScoresForRegression() throws Throwable {
		final JavaRDD<Row> rowRDD = getData(getRandomDoubleRDD(100L, 2));

		Double[] expected = computeStatsAsKnimeDoesIt(rowRDD.collect(), 0, 1);
		String params = ScorerTaskTest.paramsAsJason("in", 0, 1, false);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		NumericScorerData res = (NumericScorerData) ScorerJob.scoreRegression(
				config, rowRDD);
		assertEquals("number of records must equal length of orig table, ",
				rowRDD.count(), res.getNrRows());
		assertEquals("class col ", 0, res.getFirstCompareColumn());
		assertEquals("prediction col ", 1, res.getSecondCompareColumn());

		assertEquals("mean abs error ", expected[0], res.getMeanAbsError(),
				0.0001d);
		assertEquals("mean squared error ", expected[1],
				res.getMeanSquaredError(), 0.0001d);
		assertEquals("signed diff ", expected[2],
				res.getMeanSignedDifference(), 0.0001d);
		assertEquals("root mse ", expected[3], res.getRmsd(), 0.0001d);
		assertEquals("R-squared ", expected[4], res.getRSquare(), 0.0001d);
	}

	private Double[] computeStatsAsKnimeDoesIt(final List<Row> aRows,
			final int referenceIdx, final int predictionIdx) {
		final Mean meanObserved = new Mean(), meanPredicted = new Mean();
		final Mean absError = new Mean(), squaredError = new Mean();
		final Mean signedDiff = new Mean();
		for (Row row : aRows) {
			double ref = row.getDouble(referenceIdx);
			double pred = row.getDouble(predictionIdx);

			meanObserved.increment(ref);
			meanPredicted.increment(pred);
			absError.increment(Math.abs(ref - pred));
			squaredError.increment(Math.pow(ref - pred, 2));
			signedDiff.increment(pred - ref);
		}
		final SumOfSquares ssTot = new SumOfSquares();
		for (Row row : aRows) {
			double ref = row.getDouble(referenceIdx);
			ssTot.increment(ref - meanObserved.getResult());
		}
		return new Double[] {
				absError.getResult(),
				squaredError.getResult(),
				signedDiff.getResult(),
				Math.sqrt(squaredError.getResult()),
				(1 - squaredError.getResult()
						/ (ssTot.getResult() / aRows.size())) };
	}

	@Test
	public void computeScoresForRegressionOnIdenticalColumn() throws Throwable {
		final JavaRDD<Row> rowRDD = getData(getRandomDoubleRDD(100L, 2));

		String params = ScorerTaskTest.paramsAsJason("in", 0, 0, false);
		JobConfig config = new JobConfig(ConfigFactory.parseString(params));

		NumericScorerData res = (NumericScorerData) ScorerJob.scoreRegression(
				config, rowRDD);
		assertEquals("number of records must equal length of orig table, ",
				rowRDD.count(), res.getNrRows());
		assertEquals("class col ", 0, res.getFirstCompareColumn());
		assertEquals("prediction col ", 0, res.getSecondCompareColumn());

		assertEquals("mean abs error ", 0, res.getMeanAbsError(), 0.0001d);
		assertEquals("signed diff ", 0, res.getMeanSignedDifference(), 0.0001d);
		assertEquals("mean squared error ", 0, res.getMeanSquaredError(),
				0.0001d);
		assertEquals("root mse ", 0, res.getRmsd(), 0.0001d);
		assertEquals("R-square ", 1, res.getRSquare(), 0.0001d);
	}

}