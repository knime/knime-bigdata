package com.knime.bigdata.spark.jobserver.server;

import static org.junit.Assert.assertArrayEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.spark.SparkContext;
import org.junit.Test;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.UnitSpec;
import com.typesafe.config.Config;

/**
 *
 * @author dwk
 *
 */
public class KnimeSparkJobLogWatcherTest extends UnitSpec {

	private final static String WARN_1 = "Some validation warning";
	private final static String ERROR_1 = "Some execution error";

	private static class MyDummyJob extends KnimeSparkJob {

		private List<Logger> m_otherLoggers;

		MyDummyJob(final List<Logger> aOtherLoggers) {
			m_otherLoggers = aOtherLoggers;
		}
		private final Logger LOGGER = Logger.getLogger(KnimeSparkJob.class
				.getName());

		@Override
		public SparkJobValidation validate(JobConfig aConfig) {
			return ValidationResultConverter.valid();
		}

		@Override
		protected JobResult runJobWithContext(SparkContext aSparkContext,
				JobConfig aConfig) throws GenericKnimeSparkException {
			LOGGER.warning(WARN_1);
			LOGGER.severe(ERROR_1);
			for (Logger l : m_otherLoggers) {
				l.warning(WARN_1);
			}
			return JobResult.emptyJobResult();
		}
	}

	/**
     *
     */
	@Test
	public void reportErrorAndWarningFromOwnJob() {
		final KnimeSparkJob testObj = new MyDummyJob(new ArrayList<Logger>());
		Config config = null;
		testObj.validate(null, config);
		final JobResult res = (JobResult) testObj.runJob(null, config);
		assertArrayEquals("single warning message expected",
				new String[] { WARN_1 },
				res.getWarnings().toArray(new String[0]));
		assertArrayEquals("single error message expected",
				new String[] { ERROR_1 }, res.getErrors()
						.toArray(new String[0]));
	}

	/**
    *
    */
	@Test
	public void reportErrorAndWarningFromSomewhereElse() {
		final List<Logger> loggers = new ArrayList<>();
		loggers.add(Logger.getLogger("a"));
		loggers.add(Logger.getLogger("b"));
		loggers.add(Logger.getLogger("c"));
		loggers.add(Logger.getLogger("d"));
		
		final KnimeSparkJob testObj = new MyDummyJob(loggers);
		Config config = null;
		final JobResult res = (JobResult) testObj.runJob(null, config);

		assertArrayEquals("single warning message expected",
				new String[] { WARN_1, WARN_1 , WARN_1 , WARN_1 , WARN_1  },
				res.getWarnings().toArray(new String[0]));
		assertArrayEquals("single error message expected",
				new String[] { ERROR_1  }, res.getErrors()
						.toArray(new String[0]));
	}

}
