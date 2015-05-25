package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;

/**
 *
 * @author dwk
 *
 */
public class JobControlerTest {

	@Test
	public void jobControlerShouldBeAbleToUploadJar()
			throws Throwable {
		try {
			JobControler.uploadJobJar("");
		} finally {
			//
		}
	}


	@Test
	public void jobControlerShouldCreateJobWithProperName()
			throws Throwable {
		String contextName = KnimeContext.getSparkContext();
		try {
			String jobId = JobControler.startJob(contextName, FetchRowsJob.class.getCanonicalName());
			assertNotNull("JobId should not be null", jobId);
			assertTrue("JobId should be some lengthy string", jobId.length() > 25);

		} finally {
			KnimeContext.destroySparkContext(contextName);
		}
	}

}