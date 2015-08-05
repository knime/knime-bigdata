package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JsonUtils;
import com.knime.bigdata.spark.jobserver.jobs.FetchRowsJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.testing.SparkSpec;
import com.typesafe.config.ConfigFactory;

/**
 *
 * @author dwk
 *
 */
public class JobControlerTest extends SparkSpec {

    /**
     *
     * @throws GenericKnimeSparkException
     */
    @Test(expected = GenericKnimeSparkException.class)
    public void jobControlerShouldCheckForJar() throws GenericKnimeSparkException {
        JobControler.uploadJobJar(CONTEXT_ID, "");
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void jobControlerShouldBeAbleToUploadJar() throws Throwable {
        JobControler.uploadJobJar(CONTEXT_ID, "resources/knimeJobs.jar");
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void jobControlerShouldCreateJobWithProperName() throws Throwable {
        final String params = JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new Object[]{ParameterConstants.PARAM_NUMBER_ROWS, 9, ParameterConstants.PARAM_TABLE_1, "someRDD"}});

        JobConfig conf = new JobConfig(ConfigFactory.parseString(params));
        assertEquals("configuration should be valid",ValidationResultConverter.valid(), new FetchRowsJob().validate(conf));
        String jobId = JobControler.startJob(CONTEXT_ID, FetchRowsJob.class.getCanonicalName(), params);
        assertNotNull("JobId should not be null", jobId);
        assertTrue("JobId should be some lengthy string", jobId.length() > 25);
    }

}