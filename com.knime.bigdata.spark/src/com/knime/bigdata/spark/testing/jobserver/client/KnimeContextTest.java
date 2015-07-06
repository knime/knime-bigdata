package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;
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

		String prefix = KNIMEConfigContainer.m_config
				.getString("spark.contextNamePrefix");
		assertNotNull("context name must never be null", CONTEXT_ID);
		assertTrue("context names must start with proper prefix",
		    CONTEXT_ID.startsWith(prefix));
		Thread.sleep(1000);
		String contextName2 = KnimeContext.getSparkContext().getContextName();
		assertNotNull("context name must never be null", contextName2);
		assertTrue("context names must start with proper prefix",
				contextName2.startsWith(prefix));
		assertTrue("contexts should be re-used",
		    CONTEXT_ID.equals(contextName2));
		KnimeContext.destroySparkContext(CONTEXT_ID);

        Thread.sleep(1000);
		String contextName3 = KnimeContext.getSparkContext().getContextName();
		assertFalse("destroyed contexts should NOT be re-used",
		    CONTEXT_ID.equals(contextName3));
		CONTEXT_ID = contextName3;
	}

    /**
     *
     * @throws Throwable
     */
	@Test
	public void knimeContextShouldBeAbleToCreateCheckStatusOfAndDestroyContext()
			throws Throwable {

			JobStatus status = KnimeContext.getSparkContextStatus(CONTEXT_ID);
			assertTrue("Status of context "+CONTEXT_ID+" should be OK after creation, got: "
					+ status, status == JobStatus.OK);

	}
}
