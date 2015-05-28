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

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;

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
    final static String appName = "app";

    /**
     * upload a jar file to the server TODO - need to dynamically create jar file
     *
     * @param aJarPath
     *
     * @throws GenericKnimeSparkException
     */
    public static void uploadJobJar(final String aJarPath) throws GenericKnimeSparkException {

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
        Response response = RestClient.post("/jars/" + appName, null, Entity.entity(jarFile, MediaType.APPLICATION_OCTET_STREAM));

        RestClient.checkStatus(response, "Error: failed to upload jar to server", Status.OK);

    }

    /**
     * start a new job within the given context
     *
     * @param aContextName name of the Spark context to run the job in
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final String aContextName, final String aClassPath) throws GenericKnimeSparkException {
        return startJob(aContextName, aClassPath, "");
    }

    /**
     * start a new job within the given context
     *
     * @param aContextName name of the Spark context to run the job in
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @param aJsonParams json formated string with job parameters
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final String aContextName, final String aClassPath, final String aJsonParams)
        throws GenericKnimeSparkException {

        // start actual job
        // curl command would be:
        // curl --data-binary @classification-config.json
        // 'xxx.xxx.xxx.xxx:8090/jobs?appName=knime&context=knime&classPath=com....SparkClassificationJob'&sync=true
        Response response = RestClient.post(JOBS_PATH, new String[]{"appName", appName, "context", aContextName,
            "classPath", aClassPath}, Entity.text(aJsonParams));

        RestClient.checkStatus(response, "Error: failed to start job: " + aClassPath
            + "\nPossible reasons:\n\t'Bad Request' implies missing or incorrect parameters."
            + "\t'Not Found' implies that class file with job info was not uploaded to server.", new Status[]{
            Status.ACCEPTED, Status.OK});
        return RestClient.getJSONFieldFromResponse(response, "result", "jobId");
    }

    /**
     * start a new job within the given context
     *
     * @param aContextName name of the Spark context to run the job in
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @param aJsonParams json formated string with job parameters
     * @param aDataFile reference to a file that is to be uploaded
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final String aContextName, final String aClassPath, final String aJsonParams,
        final File aDataFile) throws GenericKnimeSparkException {

        // start actual job
        // curl command would be:
        // curl --data-binary @classification-config.json
        // 'xxx.xxx.xxx.xxx:8090/jobs?appName=knime&context=knime&classPath=com....SparkClassificationJob'&sync=true

        try (final MultiPart multiPart = new MultiPart()) {
            multiPart.bodyPart(aJsonParams, MediaType.APPLICATION_JSON_TYPE).bodyPart(
                new FileDataBodyPart(ParameterConstants.PARAM_INPUT + "." + ParameterConstants.PARAM_DATA_PATH,
                    aDataFile));

            final Response response = RestClient.post(JOBS_PATH, new String[]{"appName", appName, "context", aContextName,
                "classPath", aClassPath}, Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE));
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
     * @param aJobId job id as returned by startJob
     * @return the status
     * @throws GenericKnimeSparkException
     */
    public static JobStatus getJobStatus(final String aJobId) throws GenericKnimeSparkException {

        JsonArray jobs = RestClient.toJSONArray("/jobs");

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
     * @throws GenericKnimeSparkException
     */
    public static void killJob(final String aJobId) throws GenericKnimeSparkException {
        Response response = RestClient.delete(JOBS_PATH + aJobId);
        // we don't care about the response as long as it is "OK"
        RestClient.checkStatus(response, "Error: failed to kill job " + aJobId + "!", Status.OK);

    }

    /**
     * waits for the completion of a job with the given id for at most 1000 seconds
     *
     * @param aJobId job id as returned by startJob
     * @param aExecutionContext execution context to watch out for cancel
     * @return JobStatus status as returned by the server
     * @throws CanceledExecutionException user canceled the operation
     */
    public static JobStatus waitForJob(final String aJobId, @Nullable final ExecutionContext aExecutionContext)
        throws CanceledExecutionException {
        return waitForJob(aJobId, aExecutionContext, 1000, 1);
    }

    /**
     * waits for the completion of a job with the given id for at most the given number of seconds, pings the server for
     * the status approximately every aCheckFrequencyInSeconds seconds
     *
     * @param aJobId job id as returned by startJob
     * @param aExecutionContext execution context to watch out for cancel
     * @param aTimeoutInSeconds the maximal duration to wait for the job (in seconds)
     * @param aCheckFrequencyInSeconds (the wait interval between server requests, in seconds)
     * @return JobStatus status as returned by the server
     * @throws CanceledExecutionException user canceled the operation
     */
    public static JobStatus waitForJob(final String aJobId, @Nullable final ExecutionContext aExecutionContext,
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
                    status = getJobStatus(aJobId);
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
                        killJob(aJobId);
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
     * @param aJobId job id as returned by startJob
     * @return JSONObject with job status and result
     * @throws GenericKnimeSparkException
     */
    public static JobResult fetchJobResult(final String aJobId) throws GenericKnimeSparkException {
        // GET /jobs/<jobId> - Gets the result or status of a specific job
        JsonObject json = RestClient.toJSONObject(JOBS_PATH + aJobId);
        if (json.containsKey("result"))
        {
            return JobResult.fromBase64String(json.getString("result"));
        } else {
            return JobResult.emptyJobResult().withMessage("ERROR: no job result in: "+json.toString());
        }
    }

    /**
     * @param jobId
     * @param exec
     * @return JobResult if job did not finish with an error
     * @throws CanceledExecutionException
     * @throws GenericKnimeSparkException
     * @throws AssertionError if job failed
     */
    public static JobResult waitForJobAndFetchResult(final String jobId, final ExecutionContext exec) throws CanceledExecutionException, GenericKnimeSparkException {
        JobStatus status = waitForJob(jobId, exec);
        JobResult result = fetchJobResult(jobId);
        if (JobStatus.isErrorStatus(status)) {
            assert(false) : "Job failure: "+ result.toString();
        }
        return result;
    }

}
