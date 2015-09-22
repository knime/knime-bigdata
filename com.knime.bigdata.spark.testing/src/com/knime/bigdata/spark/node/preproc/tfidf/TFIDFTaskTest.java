package com.knime.bigdata.spark.node.preproc.tfidf;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.mllib.linalg.Vector;
import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.jobs.ImportKNIMETableJobTest;
import com.knime.bigdata.spark.jobserver.jobs.TFIDFJob;
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
public class TFIDFTaskTest extends SparkWithJobServerSpec {

	public static String paramsAsJason(final String aInputRDD,
			final Integer aColIdx, final Integer aMaxNumTerms,
			final Integer aMinFrequency, final String aSeparator,
			final String aOutputTable) throws GenericKnimeSparkException {

		return TFIDFTask.paramsAsJason(aInputRDD, aColIdx, aMaxNumTerms,
				aMinFrequency, aSeparator, aOutputTable);
	}

	@Test
	public void ensureThatAllRequiredParametersAreSet() throws Throwable {
		final TFIDFTask testObj = new TFIDFTask(null, "inputRDD", 1, 23, 5,
				" ", "out");
		final String params = testObj.learnerDef();
		final JobConfig config = new JobConfig(
				ConfigFactory.parseString(params));

		assertEquals("Configuration should be recognized as valid",
				ValidationResultConverter.valid(),
				new TFIDFJob().validate(config));
	}

	@Test
	public void createTFIDFFromSomeNormalText() throws Throwable {

		final Serializable[][] TABLE = new Serializable[][] {
				{ "this is some text" }, { "this is some more text" },
				{ "this and that and is" }, { "jo man" }, { "not and" },
				{ "and and that that this is" } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, TABLE, "tab1");

		final TFIDFTask testObj = new TFIDFTask(CONTEXT_ID, "tab1", 0, 9999, 0,
				" ", "out");

		testObj.execute(null);

		// check result table...
		Object[][] terms = fetchResultTable(CONTEXT_ID, "out", TABLE.length);
		evaluate(terms, TABLE);
	}

	private void evaluate(Object[][] terms, Serializable[][] TABLE) {
		try {
			assertEquals("two elements expected, orig and vector", 2,
					terms[0].length);
			for (int r = 0; r < TABLE.length; r++) {
				assertEquals("first column must be original text", TABLE[r][0],
						terms[r][0]);
				// there should be at most as many terms as there are words,
				// fewer if there are collisions (unlikely here, but still)
				assertTrue(
						"first column must be original text",
						TABLE[r][0].toString().split(" ").length >= nonZero(
								(Vector) terms[r][1]).size());
			}
			// row 2 and 5 contain the same terms
			assertEquals("row 2 and 5 contain the same terms",
					nonZero((Vector) terms[2][1]).size(),
					nonZero((Vector) terms[5][1]).size());
			assertEquals("row 5 has 3 distinct words,", 4,
					nonZero((Vector) terms[5][1]).size());
			assertEquals("row 1 has 5 distinct words ", 5,
					nonZero((Vector) terms[1][1]).size());
		} catch (Throwable t) {
			throw t;
		}
	}

	private ArrayList<Integer> nonZero(final Vector aVector) {
		final double[] vals = aVector.toArray();
		final ArrayList<Integer> nonZero = new ArrayList<>();
		for (int i = 0; i < vals.length; i++) {
			if (vals[i] > 0) {
				nonZero.add(i);
			}
		}
		return nonZero;
	}

	@Test
	public void createTFIDFFromSpecialText() throws Throwable {

		final Serializable[][] TABLE = new Serializable[][] { { null },
				{ "this 5698)(($% \t \t bad text text" },
				{ "a a a a a a a a a a a " }, { "                       " } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, TABLE, "tab1");

		final TFIDFTask testObj = new TFIDFTask(CONTEXT_ID, "tab1", 0, 9999, 0,
				" ", "out");

		testObj.execute(null);

		// check result table...
		Object[][] terms = fetchResultTable(CONTEXT_ID, "out", TABLE.length);
		assertEquals("row 0 is empty", 0, nonZero((Vector) terms[0][1]).size());
		assertEquals("row 1 contains 5 terms", 5, nonZero((Vector) terms[1][1])
				.size());
		assertEquals("row 2 contains 1 term ", 1, nonZero((Vector) terms[2][1])
				.size());
		assertEquals("row 3 contains no terms ", 0,
				nonZero((Vector) terms[3][1]).size());
	}
	
	@Test
	public void createTFIDFFromMultiColumnWithOtherContent() throws Throwable {

		final Serializable[][] TABLE = new Serializable[][] { { null, 0.6d },
				{ "this 5698)(($% \t \t bad text text", 0.9d },
				{ "a a a a a a a a a a a ", 0.77d }, { "                       ", 888d } };

		ImportKNIMETableJobTest.importTestTable(CONTEXT_ID, TABLE, "tab3");

		final TFIDFTask testObj = new TFIDFTask(CONTEXT_ID, "tab3", 0, 99, 0,
				" ", "out");

		testObj.execute(null);

		// check result table...
		Object[][] terms = fetchResultTable(CONTEXT_ID, "out", TABLE.length);
		assertEquals("row 0 is empty", 0, nonZero((Vector) terms[0][2]).size());
		assertEquals("row 1 contains 5 terms", 5, nonZero((Vector) terms[1][2])
				.size());
		assertEquals("row 2 contains 1 term ", 1, nonZero((Vector) terms[2][2])
				.size());
		assertEquals("row 3 contains no terms ", 0,
				nonZero((Vector) terms[3][2]).size());
	}
}
