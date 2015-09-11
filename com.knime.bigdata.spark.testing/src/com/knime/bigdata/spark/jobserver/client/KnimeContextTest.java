package com.knime.bigdata.spark.jobserver.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.preferences.KNIMEConfigContainer;


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
	public void knimeContextsShouldBeReused()
			throws Throwable {

		final String prefix = KNIMEConfigContainer.m_config
				.getString("spark.contextName");
		assertNotNull("context name must never be null", CONTEXT_ID);
		assertTrue("context names must start with proper prefix",
		    CONTEXT_ID.getContextName().startsWith(prefix));
		Thread.sleep(1000);
		final KNIMESparkContext context2 = KnimeContext.getSparkContext();
		assertNotNull("context name must never be null", context2);
		assertTrue("context names must start with proper prefix",
				context2.getContextName().startsWith(prefix));
		assertTrue("contexts should be re-used",
		    CONTEXT_ID.getContextName().equals(context2.getContextName()));
		KnimeContext.destroySparkContext(context2);

        Thread.sleep(1000);
		final KNIMESparkContext contextName3 = KnimeContext.getSparkContext();
		assertTrue("destroyed contexts should also be re-used",
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

			final JobStatus status = KnimeContext.getSparkContextStatus(KnimeContext.getSparkContext());
			assertTrue("Status of context "+CONTEXT_ID+" should be OK after creation, got: "
					+ status, status == JobStatus.OK);

	}
}
