package com.knime.bigdata.spark.jobserver.client;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;

import spark.jobserver.SparkJobValidation;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * handles the client side of the job-server in all requests related to jobs
 *
 * @author dwk
 *
 */
public class JobControler {

    /**
     * path prefix for jobs
     */
    public static final String JOBS_PATH = "/jobs/";

    private final static Logger LOGGER = Logger.getLogger(JobControler.class.getName());

    // TODO - this should probably be configurable and user-specific
    final static String appName = "knimeJobs";

    /**
     * upload a jar file to the server TODO - need to dynamically create jar file
     *
     * @param aContextContainer context configuration container
     * @param aJarPath
     *
     * @throws GenericKnimeSparkException
     */
    public static void uploadJobJar(final KNIMESparkContext aContextContainer, final String aJarPath) throws GenericKnimeSparkException {
        uploadJar(aContextContainer, aJarPath, appName);
    }

    /**
     * upload a jar file to the server
     *
     * @param aContextContainer context configuration container
     * @param aJarPath
     * @param name the application name for the jar
     *
     * @throws GenericKnimeSparkException
     */
    public static void uploadJar(final KNIMESparkContext aContextContainer, final String aJarPath, final String name)
        throws GenericKnimeSparkException {
        final File jarFile = new File(aJarPath);
        if (!jarFile.exists()) {
            final String msg =
                "ERROR: job jar file '" + jarFile.getAbsolutePath()
                    + "' does not exist. Make sure to set the proper (relative) path in the application.conf file.";
            LOGGER.severe(msg);
            throw new GenericKnimeSparkException(msg);
        }

        // upload jar
        // curl command:
        // curl --data-binary @job-server-tests/target/job-server-tests-$VER.jar
        // localhost:8090/jars/test
        Response response =
            RestClient.post(aContextContainer, "/jars/" + name, null, Entity.entity(jarFile, MediaType.APPLICATION_OCTET_STREAM));

        RestClient.checkStatus(response, "Error: failed to upload jar to server", Status.OK);
    }

    /**
     * start a new job within the given context
     *
     * @param aContextContainer context configuration container
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final KNIMESparkContext aContextContainer, final String aClassPath) throws GenericKnimeSparkException {
        return startJob(aContextContainer, aClassPath, "");
    }

    /**
     * start a new job within the given context
     *
     * @param aContextContainer context configuration container
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @param aJsonParams json formated string with job parameters
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final KNIMESparkContext aContextContainer, final String aClassPath, final String aJsonParams)
        throws GenericKnimeSparkException {
        try {
            KnimeSparkJob job = (KnimeSparkJob)Class.forName(aClassPath).newInstance();
            return startJob(aContextContainer, job, aJsonParams);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new GenericKnimeSparkException(e);
        }
    }

    /**
     * start a new job within the given context
     *
     * @param aContextContainer context configuration container
     * @param aJobInstance instance of the job (class must be in a jar that was previously uploaded to the
     *            server), used to validate the inputs
     * @param aJsonParams json formated string with job parameters
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String
        startJob(final KNIMESparkContext aContextContainer, final KnimeSparkJob aJobInstance, final String aJsonParams)
            throws GenericKnimeSparkException {

        Config config = ConfigFactory.parseString(aJsonParams);
        SparkJobValidation validation = aJobInstance.validate(config);
        if (!ValidationResultConverter.isValid(validation)) {
            throw new GenericKnimeSparkException(validation.toString());
        }
        // start actual job
        // curl command would be:
        // curl --data-binary @classification-config.json
        // 'xxx.xxx.xxx.xxx:8090/jobs?appName=knime&context=knime&classPath=com....SparkClassificationJob'&sync=true
        Response response =
            RestClient.post(aContextContainer, JOBS_PATH, new String[]{"appName", appName, "context", aContextContainer.getContextName(), "classPath",
                aJobInstance.getClass().getCanonicalName()}, Entity.text(aJsonParams));

        RestClient.checkStatus(response, "Error: failed to start job: " + aJobInstance.getClass().getCanonicalName()
            + "\nPossible reasons:\n\t'Bad Request' implies missing or incorrect parameters."
            + "\t'Not Found' implies that class file with job info was not uploaded to server.", new Status[]{
            Status.ACCEPTED, Status.OK});
        return RestClient.getJSONFieldFromResponse(response, "result", "jobId");
    }

    /**
     * start a new job within the given context
     *
     * @param aContextContainer context configuration container
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @param aJsonParams json formated string with job parameters
     * @param aDataFile reference to a file that is to be uploaded
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final KNIMESparkContext aContextContainer, final String aClassPath, final String aJsonParams,
        final File aDataFile) throws GenericKnimeSparkException {

        // start actual job
        // curl command would be:
        // curl --data-binary @classification-config.json
        // 'xxx.xxx.xxx.xxx:8090/jobs?appName=knime&context=knime&classPath=com....SparkClassificationJob'&sync=true

        try (final MultiPart multiPart = new MultiPart()) {
            multiPart.bodyPart(aJsonParams, MediaType.APPLICATION_JSON_TYPE).bodyPart(
                new FileDataBodyPart(ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_TABLE_1,
                    aDataFile));

            final Response response =
                RestClient.post(aContextContainer, JOBS_PATH, new String[]{"appName", appName, "context", aContextContainer.getContextName(), "classPath",
                    aClassPath}, Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE));
            multiPart.close();

            //			Response response = builder.post(Entity.text(aJsonParams));

            RestClient.checkStatus(response, "Error: failed to start job: " + aClassPath
                + "\nPossible reasons:\n\t'Bad Request' implies missing or incorrect parameters."
                + "\t'Not Found' implies that class file with job info was not uploaded to server.", new Status[]{
                Status.ACCEPTED, Status.OK});
            return RestClient.getJSONFieldFromResponse(response, "result", "jobId");
        } catch (final IOException e) {
            throw new GenericKnimeSparkException("Error closing multi part entity", e);
        }
    }

    /**
     * query the job-server for the status of the job with the given id
     *
     * @param aContextContainer context configuration container
     * @param aJobId job id as returned by startJob
     * @return the status
     * @throws GenericKnimeSparkException
     */
    public static JobStatus getJobStatus(final KNIMESparkContext aContextContainer, final String aJobId) throws GenericKnimeSparkException {

        JsonArray jobs = RestClient.toJSONArray(aContextContainer, "/jobs");

        for (int i = 0; i < jobs.size(); i++) {
            JsonObject jobInfo = jobs.getJsonObject(i);
            //LOGGER.log(Level.INFO, "job: " + jobInfo.getString("jobId") + ", searching for " + aJobId);
            if (aJobId.equals(jobInfo.getString("jobId"))) {
                return JobStatus.valueOf(jobInfo.getString("status"));
            }
        }

        return JobStatus.GONE;
    }

    /**
     * kill the given job
     *
     * @param aJobId job id as returned by startJob
     * @param aContextContainer
     * @throws GenericKnimeSparkException
     */
    public static void killJob(final KNIMESparkContext aContextContainer, final String aJobId) throws GenericKnimeSparkException {
        Response response = RestClient.delete(aContextContainer, JOBS_PATH + aJobId);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to kill job " + aJobId + "!", Status.OK);

    }

    /**
     * waits for the completion of a job with the given id for at most 1000 seconds
     *
     * @param aContextContainer context configuration container
     * @param aJobId job id as returned by startJob
     * @param aExecutionContext execution context to watch out for cancel
     * @return JobStatus status as returned by the server
     * @throws CanceledExecutionException user canceled the operation
     */
    public static JobStatus waitForJob(final KNIMESparkContext aContextContainer, final String aJobId, @Nullable final ExecutionContext aExecutionContext)
        throws CanceledExecutionException {
        return waitForJob(aContextContainer, aJobId, aExecutionContext, 1000, 1);
    }

    /**
     * waits for the completion of a job with the given id for at most the given number of seconds, pings the server for
     * the status approximately every aCheckFrequencyInSeconds seconds
     *
     * @param aContextContainer context configuration container
     * @param aJobId job id as returned by startJob
     * @param aExecutionContext execution context to watch out for cancel
     * @param aTimeoutInSeconds the maximal duration to wait for the job (in seconds)
     * @param aCheckFrequencyInSeconds (the wait interval between server requests, in seconds)
     * @return JobStatus status as returned by the server
     * @throws CanceledExecutionException user canceled the operation
     */
    public static JobStatus waitForJob(final KNIMESparkContext aContextContainer, final String aJobId, @Nullable final ExecutionContext aExecutionContext,
        final int aTimeoutInSeconds, final int aCheckFrequencyInSeconds) throws CanceledExecutionException {
        if (aExecutionContext != null) {
            aExecutionContext.setMessage("Waiting for Spark job to finish...");
        }
        int maxNumChecks = aTimeoutInSeconds / aCheckFrequencyInSeconds;
        for (int i = 0; i < maxNumChecks; i++) {
            try {
                Thread.sleep(1000 * aCheckFrequencyInSeconds);
                JobStatus status;
                try {
                    status = getJobStatus(aContextContainer, aJobId);
                    if (JobStatus.RUNNING != status) {
                        return status;
                    }
                    if (aExecutionContext != null) {
                        aExecutionContext.checkCanceled();
                    }
                } catch (GenericKnimeSparkException e) {
                    // Log and continue to wait... we might want to exit if
                    // this persists...
                    LOGGER.log(Level.SEVERE, e.getMessage());
                } catch (CanceledExecutionException c) {
                    try {
                        LOGGER.log(Level.WARNING, "Cancelling job on server side: " + aJobId);
                        killJob(aContextContainer, aJobId);
                    } catch (GenericKnimeSparkException e) {
                        LOGGER.log(Level.SEVERE, "Failed to cancel job " + aJobId + "\nMessage: " + e.getMessage());
                    }
                    throw c;
                }
            } catch (InterruptedException e) {
                // ignore and continue...
            }
        }
        return JobStatus.UNKNOWN;
    }

    /**
     * query the job-server for the result of the given job (it is typically a good idea to ask first whether the job
     * finished successfully)
     *
     * @param aContextContainer context configuration container
     * @param aJobId job id as returned by startJob
     * @return JSONObject with job status and result
     * @throws GenericKnimeSparkException
     */
    public static JobResult fetchJobResult(final KNIMESparkContext aContextContainer, final String aJobId) throws GenericKnimeSparkException {
        // GET /jobs/<jobId> - Gets the result or status of a specific job
        JsonObject json = RestClient.toJSONObject(aContextContainer, JOBS_PATH + aJobId);
        if (json.containsKey("result")) {
            return JobResult.fromBase64String(json.getString("result"));
        } else {
            return JobResult.emptyJobResult().withMessage("ERROR: no job result in: " + json.toString());
        }
    }

    /**
     * @param aContextContainer context configuration container
     * @param jobId
     * @param exec
     * @return JobResult if job did not finish with an error
     * @throws CanceledExecutionException
     * @throws GenericKnimeSparkException
     * @throws AssertionError if job failed
     */
    public static JobResult waitForJobAndFetchResult(final KNIMESparkContext aContextContainer, final String jobId, final ExecutionContext exec)
        throws CanceledExecutionException, GenericKnimeSparkException {
        JobStatus status = waitForJob(aContextContainer, jobId, exec);
        JobResult result = fetchJobResult(aContextContainer, jobId);
        if (JobStatus.isErrorStatus(status) || result.isError()) {
            //Object o = result.getObjectResult();
            throw new GenericKnimeSparkException("Job failure: " + result.getMessage());
        }
        return result;
    }
}
