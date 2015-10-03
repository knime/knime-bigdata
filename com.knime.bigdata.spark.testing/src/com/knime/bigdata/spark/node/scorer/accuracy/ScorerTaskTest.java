package com.knime.bigdata.spark.node.scorer.accuracy;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.summary.SumOfSquares;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.ScorerJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.NumericScorerData;
import com.knime.bigdata.spark.jobserver.server.RDDUtilsInJavaTest.MyMapper;
import com.knime.bigdata.spark.jobserver.server.ScorerData;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
@SuppressWarnings("javadoc")
public class ScorerTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputRDD,
			final Integer aActualColumnIdx, final Integer aPredictionColumnIdx,
			final Boolean aIsClassification) throws GenericKnimeSparkException {

		return ScorerTask.paramsAsJason(aInputRDD, aActualColumnIdx,
				aPredictionColumnIdx, aIsClassification);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final ScorerTask testObj = new ScorerTask(null, "inputRDD", 1, 5, true);
		final String params = testObj.learnerDef();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new ScorerJob().validate(config));
	}

	private static Double[][] getNumericData(final int nRows, final int nCols) {
		final Double[][] res = new Double[nRows][nCols];
		for (int i = 0; i < nRows; i++) {
			for (int j = 0; j < nCols; j++) {
				res[i][j] = Math.random();
			}
		}
		return res;
	}

	public static String[][] getClassificationData(final int nRows) {
		final String[][] res = new String[nRows][2];
		for (int i = 0; i < nRows; i++) {
			res[i][0] = MyMapper.teams[(int) (Math.random() * MyMapper.teams.length)];
			res[i][1] = MyMapper.teams[(int) (Math.random() * MyMapper.teams.length)];
		}
		return res;
	}

	@Test
	public void createScorerForClassification() throws Throwable {

		final String[][] tab1 = getClassificationData(100);

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, tab1, "tab1");

		final ScorerTask testObj = new ScorerTask(CONTEXT_ID, "tab1", 0, 1,
				true);

		ScorerData res = (ScorerData) testObj.execute(null);
		evaluateScores(tab1.length, tab1, res);
	}

	public static void evaluateScores(final long aCount, final String[][] rows,
			ScorerData aScores) {
		assertEquals("number of records must equal length of orig table, ",
				aCount, aScores.getNrRows());
		assertEquals(
				"sum of correct and incorrect predictions must equal number of records, ",
				aCount, aScores.getCorrectCount() + aScores.getFalseCount());
		int sum = 0;
		int[][] confMatrix = aScores.getScorerCount();
		for (int i = 0; i < confMatrix.length; i++) {
			for (int j = 0; j < confMatrix[i].length; j++) {
				sum += confMatrix[i][j];
			}
		}
		assertEquals("values in matrix must count up to number of rows",
				aCount, sum);

		final Object[] teams = aScores.getTargetValues();
		final int[] team0Counts = new int[teams.length];
		Arrays.fill(team0Counts, 0);
		for (String[] row : rows) {
			if (row[0].equals(teams[0])) {
				for (int i = 0; i < teams.length; i++) {
					if (row[1].equals(teams[i])) {
						team0Counts[i] = team0Counts[i] + 1;
					}
				}
			}
		}
		for (int i = 0; i < teams.length; i++) {
			assertEquals("incorrect count for Team 0[" + i + "]",
					team0Counts[i], confMatrix[0][i]);

		}
	}

	@Test
	public void createScorerForRegression() throws Throwable {

		final Double[][] tab1 = getNumericData(100, 2);

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, tab1, "tab1");

		final ScorerTask testObj = new ScorerTask(CONTEXT_ID, "tab1", 0, 1,
				false);

		NumericScorerData res = (NumericScorerData) testObj.execute(null);

		Double[] expected = computeStatsAsKnimeDoesIt(tab1);

		assertEquals("number of records must equal length of orig table, ",
				tab1.length, res.getNrRows());
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

	public static Double[] computeStatsAsKnimeDoesIt(final Double[][] aRows) {
		final Mean meanObserved = new Mean(), meanPredicted = new Mean();
		final Mean absError = new Mean(), squaredError = new Mean();
		final Mean signedDiff = new Mean();
		for (Double[] row : aRows) {
			double ref = row[0];
			double pred = row[1];

			meanObserved.increment(ref);
			meanPredicted.increment(pred);
			absError.increment(Math.abs(ref - pred));
			squaredError.increment(Math.pow(ref - pred, 2));
			signedDiff.increment(pred - ref);
		}
		final SumOfSquares ssTot = new SumOfSquares();
		for (Double[] row : aRows) {
			ssTot.increment(row[0] - meanObserved.getResult());
		}
		return new Double[] {
				absError.getResult(),
				squaredError.getResult(),
				signedDiff.getResult(),
				Math.sqrt(squaredError.getResult()),
				(1 - squaredError.getResult()
						/ (ssTot.getResult() / aRows.length)) };
	}

}
