package com.knime.bigdata.spark.jobserver.client;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.Test;

import com.knime.bigdata.spark.SparkWithJobServerSpec;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.jobs.TransformationTestJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;

/**
 *
 * @author dwk
 *
 */
public class CompiledTransformationJobTest extends SparkWithJobServerSpec {

    private final String params2Json(final String aInputKey, final String aOutputKey) {
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{KnimeSparkJob.PARAM_INPUT_TABLE, aInputKey,
            ParameterConstants.PARAM_SEPARATOR, " "}, ParameterConstants.PARAM_OUTPUT,
            new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, aOutputKey}});
    }

    /**
     * test whether validation error is properly reported to client
     *
     * @throws Throwable
     */
    @Test
    public void addTransformationJobWithValidationError2JarAndExecuteOnServer() throws Throwable {

        final String params =
            JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_OUTPUT,
                new String[]{KnimeSparkJob.PARAM_RESULT_TABLE, "someRDD"}});
        final File f = File.createTempFile("knimeJobUtils", ".jar");
        f.deleteOnExit();

        final String jarPath = f.toString();

        final SparkJobCompiler testObj = new SparkJobCompiler();

        testObj
            .addPrecompiledKnimeSparkJob2Jar(getJobJarPath(), jarPath, TransformationTestJob.class.getCanonicalName());

        //upload jar to job-server
        JobControler.uploadJobJar(CONTEXT_ID, jarPath);
        //start job
        boolean exceptionThrown = false;
        try {
            JobControler.startJob(CONTEXT_ID, TransformationTestJob.class.getCanonicalName(), params);
        } catch (final GenericKnimeSparkException ge) {
            //this is what should happen
            //check that exception makes sense...
        	assertTrue("exception message should contain some text about an invalid job, got: "+ge.getMessage(), ge.getMessage().toLowerCase().contains("jobinvalid"));
            exceptionThrown = true;
        }
        assertTrue("job validation job have thrown an exception", exceptionThrown);
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void addTransformationJob2JarAndExecuteOnServer() throws Throwable {

    	importTestTable(CONTEXT_ID, MINI_IRIS_TABLE, "unitTestRDD1");

        final File f = File.createTempFile("knimeJobUtils", ".jar");
        f.deleteOnExit();

        final String jarPath = f.toString();

        final SparkJobCompiler testObj = new SparkJobCompiler();

        testObj
            .addPrecompiledKnimeSparkJob2Jar(getJobJarPath(), jarPath, TransformationTestJob.class.getCanonicalName());

            //upload jar to job-server
            JobControler.uploadJobJar(CONTEXT_ID, jarPath);
            //start job
            final String jobId =
                JobControler.startJob(CONTEXT_ID, TransformationTestJob.class.getCanonicalName(), params2Json("unitTestRDD1", "unitTestRDD2"));

            DummyRestClient.jobResponse = "{\"result\":\"OK\"}";

            //throws exception if job did not finish properly
            JobControler.waitForJobAndFetchResult(CONTEXT_ID, jobId, null);

            assertNotSame("job should not be running anymore", JobStatus.OK, JobControler.getJobStatus(CONTEXT_ID, jobId));
    }

}