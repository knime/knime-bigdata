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
		assertNotNull("context name must never be null", contextName);
		assertTrue("context names must start with proper prefix",
		    contextName.startsWith(prefix));
		Thread.sleep(1000);
		String contextName2 = KnimeContext.getSparkContext();
		assertNotNull("context name must never be null", contextName2);
		assertTrue("context names must start with proper prefix",
				contextName2.startsWith(prefix));
		assertTrue("contexts should be re-used",
		    contextName.equals(contextName2));
		KnimeContext.destroySparkContext(contextName);

        Thread.sleep(1000);
		String contextName3 = KnimeContext.getSparkContext();
		assertFalse("destroyed contexts should NOT be re-used",
		    contextName.equals(contextName3));
		contextName = contextName3;
	}

    /**
     *
     * @throws Throwable
     */
	@Test
	public void knimeContextShouldBeAbleToCreateCheckStatusOfAndDestroyContext()
			throws Throwable {

			JobStatus status = KnimeContext.getSparkContextStatus(contextName);
			assertTrue("Status of context "+contextName+" should be OK after creation, got: "
					+ status, status == JobStatus.OK);

	}
}
