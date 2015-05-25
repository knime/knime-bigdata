package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.KnimeContext;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.testing.UnitSpec;

/**
 *
 * @author dwk
 *
 */
public class JobControlerTest extends UnitSpec {

    /**
     *
     * @throws GenericKnimeSparkException
     */
    @Test(expected = GenericKnimeSparkException.class)
    public void jobControlerShouldCheckForJar() throws GenericKnimeSparkException {
        JobControler.uploadJobJar("");
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void jobControlerShouldBeAbleToUploadJar() throws Throwable {
        JobControler.uploadJobJar("resources/knimeJobs.jar");
    }

    @Test
    public void jobControlerShouldCreateJobWithProperName() throws Throwable {
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