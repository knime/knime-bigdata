package com.knime.bigdata.spark.testing.jobserver.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.nio.file.Files;

import javax.json.JsonObject;

import org.junit.Test;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.client.JobControler;
import com.knime.bigdata.spark.jobserver.client.JobStatus;
import com.knime.bigdata.spark.jobserver.client.KNIMEConfigContainer;
import com.knime.bigdata.spark.jobserver.client.RestClient;
import com.knime.bigdata.spark.jobserver.client.jar.SparkJobCompiler;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.testing.SparkSpec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

/**
 *
 * @author dwk
 *
 */
public class SparkJobCompilerTest extends SparkSpec {

    /**
     *
     * @throws Throwable
     */
    @Test
    public void compilePrimitiveJobThatDoesNothing() throws Throwable {

        SparkJobCompiler testObj = new SparkJobCompiler();
        KnimeSparkJob job = testObj.newKnimeSparkJob("", "", "return null;", "");
        SparkJobValidation valRes = job.validate(null, null);
        assertEquals("empty validate should return valid ", ValidationResultConverter.valid(), valRes);
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void compilePrimitiveJobThatOnlyChecksParams() throws Throwable {

        final String validationCode =
            "try {\n" + "	int t = aConfig.getInputParameter(\"timeout\", Integer.class);\n" + "	timeout = TimeUnit.SECONDS.toMillis(t);\n"
                + "} catch (Exception e) {\n" + "	// OK, ignore\n" + "}";

        final String mainStr = "public static void main(String[] args) {" + "System.out.println(\"Hello World\");}";

        final String additionalImports = "import java.util.concurrent.TimeUnit;";

        SparkJobCompiler testObj = new SparkJobCompiler();
        KnimeSparkJob job =
            testObj.newKnimeSparkJob(additionalImports, validationCode, "return null;",
                "long timeout = TimeUnit.HOURS.toMillis(1L); \n" + mainStr);

        Config config = ConfigFactory.parseString("{\"input\":{\"timeout\":\"67\"}}");
        SparkJobValidation valRes = job.validate(null, config);
        assertEquals("correct validate should return valid for correct params", ValidationResultConverter.valid(),
            valRes);
    }

    /**
     *
     * @throws Throwable
     */
    @Test
    public void addPrimitiveJobThatOnlyChecksParams2JarAndExecuteOnServer() throws Throwable {

        final String RES_STR = "HELLO WORLD!!!";
        final String configText = "{\"input\":{\"message\":\"" + RES_STR + "\"}}";

        final String validationCode =
            "try {\n" + "String s = aConfig.getInputParameter(\"message\");\n" + "} catch (Exception e) {\n"
                + "	// OK, ignore\n" + "}";

        final String additionalImports = "import java.util.concurrent.TimeUnit;";

        final String aJarPath = Files.createTempFile("knimeJobUtils", "jar").toString();

        final SparkJobCompiler testObj = new SparkJobCompiler();

        KnimeSparkJob job =
            testObj
                .newKnimeSparkJob(
                    additionalImports,
                    validationCode,
                    "System.out.println(\"Hello World\"); return JobResult.emptyJobResult().withMessage(aConfig.getInputParameter(\"message\"));",
                    "");

        Config config = ConfigFactory.parseString(configText);
        assertEquals("config should be valid", ValidationResultConverter.valid(), job.validate(null, config));

        final KnimeSparkJob jobInstance =
            testObj
                .addKnimeSparkJob2Jar(
                    "resources/knimeJobs.jar",
                    aJarPath,
                    additionalImports,
                    validationCode,
                    "System.out.println(\"Hello World\"); return JobResult.emptyJobResult().withMessage(aConfig.getInputParameter(\"message\"));",
                    "");

            //upload jar to job-server
            JobControler.uploadJobJar(CONTEXT_ID, aJarPath);
            //start job
            String jobId = JobControler.startJob(CONTEXT_ID, jobInstance, configText);

            KNIMEConfigContainer.m_config =
                KNIMEConfigContainer.m_config.withValue(JobControler.JOBS_PATH + jobId,
                    ConfigValueFactory.fromAnyRef("{\"result\":\""+RES_STR+"\"}"));

            assertNotSame("job should have finished properly", JobControler.waitForJob(CONTEXT_ID, jobId, null), JobStatus.UNKNOWN);

            assertNotSame("job should not be running anymore", JobStatus.OK, JobControler.getJobStatus(CONTEXT_ID, jobId));

            JsonObject res = RestClient.toJSONObject(CONTEXT_ID, JobControler.JOBS_PATH + jobId); //JobControler.fetchJobResult(jobId).getMessage();
            assertTrue("job result", res.getString("result").contains(RES_STR));

    }

    /**
    *
    * @throws Throwable
    */
   @Test
   public void addTransformationJob2JarAndExecuteOnServer() throws Throwable {

       final String RES_STR = "HELLO WORLD!!!";
       final String configText = "{\"input\":{\"message\":\"" + RES_STR + "\"}}";

       final String validationCode =
           "try {\n" + "String s = aConfig.getInputParameter(\"message\");\n" + "} catch (Exception e) {\n"
               + " // OK, ignore\n" + "}";

       final String additionalImports = "import java.util.concurrent.TimeUnit;";

       final String aJarPath = Files.createTempFile("knimeJobUtils", "jar").toString();

       final SparkJobCompiler testObj = new SparkJobCompiler();

       KnimeSparkJob job =
           testObj
               .newKnimeSparkJob(
                   additionalImports,
                   validationCode,
                   "System.out.println(\"Hello World\"); return JobResult.emptyJobResult().withMessage(aConfig.getInputParameter(\"message\"));",
                   "");

       Config config = ConfigFactory.parseString(configText);
       assertEquals("config should be valid", ValidationResultConverter.valid(), job.validate(null, config));

       final KnimeSparkJob jobInstance =
           testObj
               .addKnimeSparkJob2Jar(
                   "resources/knimeJobs.jar",
                   aJarPath,
                   additionalImports,
                   validationCode,
                   "System.out.println(\"Hello World\"); return JobResult.emptyJobResult().withMessage(aConfig.getInputParameter(\"message\"));",
                   "");

           //upload jar to job-server
           JobControler.uploadJobJar(CONTEXT_ID, aJarPath);
           //start job
           String jobId = JobControler.startJob(CONTEXT_ID, jobInstance, configText);

           KNIMEConfigContainer.m_config =
               KNIMEConfigContainer.m_config.withValue(JobControler.JOBS_PATH + jobId,
                   ConfigValueFactory.fromAnyRef("{\"result\":\""+RES_STR+"\"}"));

           assertNotSame("job should have finished properly", JobControler.waitForJob(CONTEXT_ID, jobId, null), JobStatus.UNKNOWN);

           assertNotSame("job should not be running anymore", JobStatus.OK, JobControler.getJobStatus(CONTEXT_ID, jobId));

           JsonObject res = RestClient.toJSONObject(CONTEXT_ID, JobControler.JOBS_PATH + jobId); //JobControler.fetchJobResult(jobId).getMessage();
           assertTrue("job result", res.getString("result").contains(RES_STR));


   }

}