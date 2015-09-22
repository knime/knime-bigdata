package com.knime.bigdata.spark.jobserver.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;

/**
 *
 * @author dwk
 *
 */
public class KnimeContextTest extends SparkWithJobServerSpec {

	/**
	 *
	 * @throws Throwable
	 */
	@Test
	public void knimeContextShouldHaveConfiguredPrefix() throws Throwable {

		assertNotNull("context name must never be null", CONTEXT_ID);
		assertTrue("context names must start with proper prefix", CONTEXT_ID
				.getContextName().startsWith(CONTEXT_PREFIX_UNIT_TESTS));
	}

	/**
	 *
	 * @throws Throwable
	 */
	@Test
	public void knimeContextShouldBeAbleToCreateCheckStatusOfAndDestroyContext()
			throws Throwable {

		final JobStatus status = KnimeContext.getSparkContextStatus(CONTEXT_ID);
		assertTrue("Status of context " + CONTEXT_ID
				+ " should be OK after creation, got: " + status,
				status == JobStatus.OK);

	}
}
