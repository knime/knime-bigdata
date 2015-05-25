package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.KnimeConfigContainer;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.testing.UnitSpec;


/**
 *
 * @author dwk
 *
 */
public class KnimeContextTest extends UnitSpec {

    /**
     *
     * @throws Throwable
     */
	@Test
	public void any2KnimeContextsShouldHaveDifferentNamesButStartWithCorrectPrefix()
			throws Throwable {

		String prefix = KnimeConfigContainer.m_config
				.getString("spark.contextNamePrefix");
		String contextName1 = KnimeContext.getSparkContext();
		assertNotNull("context name must never be null", contextName1);
		assertTrue("context names must start with proper prefix",
				contextName1.startsWith(prefix));
		Thread.sleep(1000);
		String contextName2 = KnimeContext.getSparkContext();
		assertNotNull("context name must never be null", contextName2);
		assertTrue("context names must start with proper prefix",
				contextName2.startsWith(prefix));
		assertTrue("contexts should be re-used",
				contextName1.equals(contextName2));
		KnimeContext.destroySparkContext(contextName1);
		KnimeContext.destroySparkContext(contextName2);

        Thread.sleep(1000);
		String contextName3 = KnimeContext.getSparkContext();
		assertFalse("destroyed contexts should NOT be re-used",
				contextName1.equals(contextName3));

	}

    /**
     *
     * @throws Throwable
     */
	@Test
	public void knimeContextShouldBeAbleToCreateCheckStatusOfAndDestroyContext()
			throws Throwable {

		String contextName = KnimeContext.getSparkContext();
		try {
			JobStatus status = KnimeContext.getSparkContextStatus(contextName);
			assertTrue("Status of context "+contextName+" should be OK after creation, got: "
					+ status, status == JobStatus.OK);
		} finally {
			KnimeContext.destroySparkContext(contextName);
			//need to wait a bit before we can actually test whether it is really gone
			Thread.sleep(200);
		}
		// TODO - what would be the expected status?
		assertTrue("context status should NOT be OK after destruction",
				KnimeContext.getSparkContextStatus(contextName) != JobStatus.OK);

	}
}
