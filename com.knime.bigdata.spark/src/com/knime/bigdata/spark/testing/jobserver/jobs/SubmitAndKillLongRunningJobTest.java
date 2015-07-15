package com.knime.bigdata.spark.testing.jobserver.jobs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;

/**
 *
 * @author dwk
 *
 */
public class SubmitAndKillLongRunningJobTest {

	@Test
	public void jobControlerShouldCreateAndKillLongRunningJob()
			throws Throwable {
		KNIMESparkContext contextName = KnimeContext.getSparkContext();
		try {
			String jobId = JobControler.startJob(contextName, "org.knime.sparkClient.jobs.EndlessJob");
			for (int i = 1; i < 10; i++) {
				Thread.sleep(1000);
				assertEquals("job should still be running (" + i + ")",
						JobStatus.RUNNING, JobControler.getJobStatus(contextName, jobId));
			}
			JobControler.killJob(contextName, jobId);
			assertFalse("job should not be running anymore", JobStatus.RUNNING.equals(
					JobControler.getJobStatus(contextName, jobId)));

		} finally {
			KnimeContext.destroySparkContext(contextName);
		}
	}

}