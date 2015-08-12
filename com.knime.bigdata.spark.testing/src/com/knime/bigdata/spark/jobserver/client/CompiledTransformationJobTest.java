package com.knime.bigdata.spark.jobserver.client;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.File;

import javax.json.JsonObject;

import org.junit.Test;

import com.knime.bigdata.spark.SparkSpec;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.jobs.TransformationTestJob;
import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.typesafe.config.ConfigValueFactory;

/**
 *
 * @author dwk
 *
 */
public class CompiledTransformationJobTest extends SparkSpec {

    private final String params2Json(final String aInputKey, final String aOutputKey) {
        return JsonUtils.asJson(new Object[]{ParameterConstants.PARAM_INPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, aInputKey,
            ParameterConstants.PARAM_SEPARATOR, " "}, ParameterConstants.PARAM_OUTPUT,
            new String[]{ParameterConstants.PARAM_TABLE_1, aOutputKey}});
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
                new String[]{ParameterConstants.PARAM_TABLE_1, "someRDD"}});
        File f = File.createTempFile("knimeJobUtils", ".jar");
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
        } catch (GenericKnimeSparkException ge) {
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
    	
        File f = File.createTempFile("knimeJobUtils", ".jar");
        f.deleteOnExit();

        final String jarPath = f.toString();

        final SparkJobCompiler testObj = new SparkJobCompiler();

        testObj
            .addPrecompiledKnimeSparkJob2Jar(getJobJarPath(), jarPath, TransformationTestJob.class.getCanonicalName());

            //upload jar to job-server
            JobControler.uploadJobJar(CONTEXT_ID, jarPath);
            //start job
            String jobId =
                JobControler.startJob(CONTEXT_ID, TransformationTestJob.class.getCanonicalName(), params2Json("unitTestRDD1", "unitTestRDD2"));

            KNIMEConfigContainer.m_config =
                KNIMEConfigContainer.m_config.withValue(JobControler.JOBS_PATH + jobId,
                    ConfigValueFactory.fromAnyRef("{\"result\":\"OK\"}"));

            //throws exception if job did not finish properly
            JobControler.waitForJobAndFetchResult(CONTEXT_ID, jobId, null);
            
            assertNotSame("job should not be running anymore", JobStatus.OK, JobControler.getJobStatus(CONTEXT_ID, jobId));
    }

}