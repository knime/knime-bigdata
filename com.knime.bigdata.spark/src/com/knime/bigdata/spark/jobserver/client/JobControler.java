package com.knime.bigdata.spark.jobserver.client;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import javax.annotation.Nullable;
import javax.json.JsonObject;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.file.FileDataBodyPart;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.jobserver.server.GenericKnimeSparkException;
import com.knime.bigdata.spark.jobserver.server.JobConfig;
import com.knime.bigdata.spark.jobserver.server.JobResult;
import com.knime.bigdata.spark.jobserver.server.KnimeSparkJob;
import com.knime.bigdata.spark.jobserver.server.ParameterConstants;
import com.knime.bigdata.spark.jobserver.server.ValidationResultConverter;
import com.knime.bigdata.spark.port.context.KNIMESparkContext;
import com.knime.bigdata.spark.preferences.KNIMEConfigContainer;
import com.knime.bigdata.spark.util.SparkIDs;
import com.typesafe.config.ConfigFactory;

import spark.jobserver.SparkJobValidation;

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

    private final static NodeLogger LOGGER = NodeLogger.getLogger(JobControler.class.getName());

    /**We use the Spark application ID which is based on the KNIME id which is unique per workspace.*/
    final static String APP_NAME = SparkIDs.getSparkApplicationID();

    /**
     * upload a jar file to the server TODO - need to dynamically create jar file
     *
     * @param aContextContainer context configuration container
     * @param aJarPath
     *
     * @throws GenericKnimeSparkException
     */
    public static void uploadJobJar(final KNIMESparkContext aContextContainer, final String aJarPath)
            throws GenericKnimeSparkException {
        DataUploader.uploadFile(aContextContainer, aJarPath, "/jars/" + APP_NAME);
    }

    /**
     * Upload a jar file to the server.
     * NOTICE: The classes within this jar are only visible to jobs with the same application name!
     *
     * @param aContextContainer context configuration container
     * @param aJarPath
     * @param appName the application name for the jar
     *
     * @throws GenericKnimeSparkException
    private static void uploadJar(final KNIMESparkContext aContextContainer, final String aJarPath, final String appName)
        throws GenericKnimeSparkException {
        final File jarFile = new File(aJarPath);
        if (!jarFile.exists()) {
            final String msg =
                "ERROR: job jar file '" + jarFile.getAbsolutePath()
                    + "' does not exist. Make sure to set the proper (relative) path in the application.conf file.";
            LOGGER.error(msg);
            throw new GenericKnimeSparkException(msg);
        }

        // upload jar
        // curl command:
        // curl --data-binary @job-server-tests/target/job-server-tests-$VER.jar
        // localhost:8090/jars/test
        Response response = RestClient.post(aContextContainer, "/jars/" + appName, null,
            Entity.entity(jarFile, MediaType.APPLICATION_OCTET_STREAM));

        RestClient.checkStatus(response, "Failed to upload jar to server. Jar path: " + aJarPath, Status.OK);
    }
     */



    /**
     * Upload a jar file to the server.
     * NOTICE: The classes within this jar are only visible to jobs with the same application name!
     *
     * @param aContextContainer context configuration container
     * @param aJarPath
     * @param appName the application name for the jar
     *
     * @throws GenericKnimeSparkException
    private static void uploadJar(final KNIMESparkContext aContextContainer, final String aJarPath, final String appName)
        throws GenericKnimeSparkException {
        final File jarFile = new File(aJarPath);
        if (!jarFile.exists()) {
            final String msg =
                "ERROR: job jar file '" + jarFile.getAbsolutePath()
                    + "' does not exist. Make sure to set the proper (relative) path in the application.conf file.";
            LOGGER.error(msg);
            throw new GenericKnimeSparkException(msg);
        }

        // upload jar
        // curl command:
        // curl --data-binary @job-server-tests/target/job-server-tests-$VER.jar
        // localhost:8090/jars/test
        Response response = RestClient.post(aContextContainer, "/jars/" + appName, null,
            Entity.entity(jarFile, MediaType.APPLICATION_OCTET_STREAM));

        RestClient.checkStatus(response, "Failed to upload jar to server. Jar path: " + aJarPath, Status.OK);
    }
     */

    /**
     * start a new job within the given context
     *
     * @param aContextContainer context configuration container
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @return job id
     * @throws GenericKnimeSparkException
     */
    public static String startJob(final KNIMESparkContext aContextContainer, final String aClassPath)
            throws GenericKnimeSparkException {
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
    public static String startJob(final KNIMESparkContext aContextContainer, final String aClassPath,
        final String aJsonParams) throws GenericKnimeSparkException {
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
        LOGGER.debug("Start Spark job");
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Context: " + aContextContainer);
            LOGGER.debug("Job: " + (aJobInstance == null ? "" : aJobInstance.getClass().getCanonicalName()));
            LOGGER.debug("Params: " + aJsonParams);
        }
        final JobConfig config = new JobConfig(ConfigFactory.parseString(aJsonParams));
        @SuppressWarnings("null")
        final SparkJobValidation validation = aJobInstance.validate(config);
        if (!ValidationResultConverter.isValid(validation)) {
            throw new GenericKnimeSparkException(validation.toString());
        }
        LOGGER.debug("Spark job successful validated");
        // start actual job
        // curl command would be:
        // curl --data-binary @classification-config.json
        // 'xxx.xxx.xxx.xxx:8090/jobs?appName=knime&context=knime&classPath=com....SparkClassificationJob'&sync=true
        final String jobClassName = aJobInstance.getClass().getCanonicalName();
        LOGGER.debug("Spark job class: " + jobClassName);
        final Response response =
                aContextContainer.getREST().post(aContextContainer, JOBS_PATH, new String[]{"appName", APP_NAME, "context",
                aContextContainer.getContextName(), "classPath", jobClassName}, Entity.text(aJsonParams));
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Job response:" + response);
        }
        aContextContainer.getREST().checkJobStatus(response, jobClassName, aJsonParams);
        LOGGER.debug("Spark job status checked");
//        RestClient.checkStatus(response, "Error: failed to start job: " + aJobInstance.getClass().getCanonicalName()
//            + "\nPossible reasons:\n\t'Bad Request' implies missing or incorrect parameters."
//            + "\t'Not Found' implies that class file with job info was not uploaded to server.", new Status[]{
//            Status.ACCEPTED, Status.OK});
        return aContextContainer.getREST().getJSONFieldFromResponse(response, "result", "jobId");
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
    public static String startJob(final KNIMESparkContext aContextContainer, final String aClassPath,
        final String aJsonParams, final File aDataFile) throws GenericKnimeSparkException {

        // start actual job
        // curl command would be:
        // curl --data-binary @classification-config.json
        // 'xxx.xxx.xxx.xxx:8090/jobs?appName=knime&context=knime&classPath=com....SparkClassificationJob'&sync=true
        LOGGER.debug("Start Spark data file job");
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Context: " + aContextContainer);
            LOGGER.debug("Job: " + aClassPath);
            LOGGER.debug("Params: " + aJsonParams);
            LOGGER.debug("File:" + (aDataFile == null ? "" : aDataFile.getAbsolutePath()));
        }
        try (final MultiPart multiPart = new MultiPart()) {
            multiPart.bodyPart(aJsonParams, MediaType.APPLICATION_JSON_TYPE).bodyPart(
                new FileDataBodyPart(ParameterConstants.PARAM_INPUT + "." + KnimeSparkJob.PARAM_INPUT_TABLE,
                    aDataFile));
            LOGGER.debug("Spark data file job class: " + aClassPath);
            RestClient client = aContextContainer.getREST();
            final Response response =
                    client.post(aContextContainer, JOBS_PATH, new String[]{"appName", APP_NAME, "context",
                    aContextContainer.getContextName(), "classPath", aClassPath},
                    Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA_TYPE));
            multiPart.close();
            if (KNIMEConfigContainer.verboseLogging()) {
                LOGGER.debug("Job response:" + response);
            }
            //			Response response = builder.post(Entity.text(aJsonParams));
            client.checkJobStatus(response, aClassPath, aJsonParams);
            LOGGER.debug("Spark data file job status checked");
            return client.getJSONFieldFromResponse(response, "result", "jobId");
        } catch (final IOException e) {
            throw new GenericKnimeSparkException("Error closing multi part entity", e);
        }
    }

    /**
     * Upload a jar file to the server.
     * NOTICE: The classes within this jar are only visible to jobs with the same application name!
     *
     * @param aContextContainer context configuration container
     * @param aJarPath
     * @param appName the application name for the jar
     *
     * @throws GenericKnimeSparkException
    private static void uploadJar(final KNIMESparkContext aContextContainer, final String aJarPath, final String appName)
        throws GenericKnimeSparkException {
        final File jarFile = new File(aJarPath);
        if (!jarFile.exists()) {
            final String msg =
                "ERROR: job jar file '" + jarFile.getAbsolutePath()
                    + "' does not exist. Make sure to set the proper (relative) path in the application.conf file.";
            LOGGER.error(msg);
            throw new GenericKnimeSparkException(msg);
        }

        // upload jar
        // curl command:
        // curl --data-binary @job-server-tests/target/job-server-tests-$VER.jar
        // localhost:8090/jars/test
        Response response = RestClient.post(aContextContainer, "/jars/" + appName, null,
            Entity.entity(jarFile, MediaType.APPLICATION_OCTET_STREAM));

        RestClient.checkStatus(response, "Failed to upload jar to server. Jar path: " + aJarPath, Status.OK);
    }
     */

    /**
     * @param aContextContainer context configuration container
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @param exec {@link ExecutionMonitor} to provide progress
     * @return the {@link JobResult}
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public static JobResult startJobAndWaitForResult(final KNIMESparkContext aContextContainer, final String aClassPath,
        final ExecutionMonitor exec)
                throws GenericKnimeSparkException, CanceledExecutionException {
        final String jobId = startJob(aContextContainer, aClassPath);
        return JobControler.waitForJobAndFetchResult(aContextContainer, jobId, exec);
    }

    /**
     * @param aContextContainer context configuration container
     * @param aClassPath full class path of the job to run (class must be in a jar that was previously uploaded to the
     *            server)
     * @param aJsonParams json formated string with job parameters
     * @param exec {@link ExecutionMonitor} to provide progress
     * @return the {@link JobResult}
     * @throws GenericKnimeSparkException
     * @throws CanceledExecutionException
     */
    public static JobResult startJobAndWaitForResult(final KNIMESparkContext aContextContainer, final String aClassPath,
        final String aJsonParams, final ExecutionMonitor exec)
                throws GenericKnimeSparkException, CanceledExecutionException {
        final String jobId = startJob(aContextContainer, aClassPath, aJsonParams);
        return JobControler.waitForJobAndFetchResult(aContextContainer, jobId, exec);
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
    public static JobResult waitForJobAndFetchResult(final KNIMESparkContext aContextContainer, final String jobId,
        final ExecutionMonitor exec) throws CanceledExecutionException, GenericKnimeSparkException {

        JobStatus status = waitForJob(aContextContainer, jobId, exec);
        if (JobStatus.isErrorStatus(status)) {
            throw new GenericKnimeSparkException("Job failure: " + status);
        }
        JobResult result = fetchJobResult(aContextContainer, jobId);
        //format of msg: [Source, Actual Message]
        for (String[] msg : result.getWarnings()) {
            LOGGER.warn(Arrays.toString(msg));
        }
        for (String[] msg : result.getErrors()) {
            LOGGER.error(Arrays.toString(msg));
        }
        if (result.isError()) {
            throw new GenericKnimeSparkException("Job failure: " + result.getMessage());
        }
        return result;
    }

    /**
     * waits for the completion of a job with the given id for at most 1000 seconds
     *
     * @param aContextContainer context configuration container
     * @param aJobId job id as returned by startJob
     * @param aExecutionContext execution context to watch out for cancel
     * @return JobStatus status as returned by the server
     * @throws CanceledExecutionException user canceled the operation
     * @throws GenericKnimeSparkException
     */
    static JobStatus waitForJob(final KNIMESparkContext aContextContainer, final String aJobId,
        @Nullable final ExecutionMonitor aExecutionContext) throws CanceledExecutionException, GenericKnimeSparkException {
        return waitForJob(aContextContainer, aJobId, aExecutionContext, aContextContainer.getJobTimeout(),
            aContextContainer.getJobCheckFrequency());
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
     * @throws GenericKnimeSparkException if the timeout is reached
     */
    static JobStatus waitForJob(final KNIMESparkContext aContextContainer, final String aJobId,
        @Nullable final ExecutionMonitor aExecutionContext, final int aTimeoutInSeconds,
        final int aCheckFrequencyInSeconds) throws CanceledExecutionException, GenericKnimeSparkException {
        LOGGER.debug("Start waiting for job...");
        if (aExecutionContext != null) {
            aExecutionContext.setMessage("Waiting for Spark job to finish...");
        }
        final int maxNumChecks = aTimeoutInSeconds / aCheckFrequencyInSeconds;
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
                        aExecutionContext.setMessage(
                            "Waiting for Spark job to finish (Execution time: "
                                    + (i + 1) * aCheckFrequencyInSeconds + " seconds)");
                    }
                } catch (GenericKnimeSparkException e) {
                    // Log and continue to wait... we might want to exit if
                    // this persists...
                    LOGGER.error(e.getMessage());
                } catch (CanceledExecutionException c) {
                    try {
                        LOGGER.warn("Cancelling job on server side: " + aJobId);
                        killJob(aContextContainer, aJobId);
                    } catch (GenericKnimeSparkException e) {
                        LOGGER.error("Failed to cancel job " + aJobId + "\nMessage: " + e.getMessage());
                    }
                    throw c;
                }
            } catch (InterruptedException e) {
                // ignore and continue...
            }
        }
        LOGGER.warn("Job timeout of " + aTimeoutInSeconds + " seconds reached. You might want to increase the "
            + "job timeout for this Spark context in the Spark preference page or the Create Spark Context node.");
        LOGGER.warn("Cancelling job on server side: " + aJobId);
        killJob(aContextContainer, aJobId);
        throw new GenericKnimeSparkException("Job timeout of " + aTimeoutInSeconds + " seconds reached.");
    }

    /**
     * query the job-server for the status of the job with the given id
     *
     * @param aContextContainer context configuration container
     * @param aJobId job id as returned by startJob
     * @return the status
     * @throws GenericKnimeSparkException
     */
    public static JobStatus getJobStatus(final KNIMESparkContext aContextContainer, final String aJobId)
            throws GenericKnimeSparkException {
        LOGGER.debug("Getting job status for id: " + aJobId);

        final JsonObject jobInfo = aContextContainer.getREST().toJSONObject(aContextContainer, "/jobs/" + aJobId);
        //LOGGER.log(Level.INFO, "job: " + jobInfo.getString("jobId") + ", searching for " + aJobId);
        final String statusString = jobInfo.getString("status");
        LOGGER.debug("Job status for id: " + aJobId + " is " + statusString);
        return JobStatus.valueOf(statusString);
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
    public static JobResult fetchJobResult(final KNIMESparkContext aContextContainer, final String aJobId)
            throws GenericKnimeSparkException {
        // GET /jobs/<jobId> - Gets the result or status of a specific job
        final JsonObject json = aContextContainer.getREST().toJSONObject(aContextContainer, JOBS_PATH + aJobId);
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Job result json:" + json);
        }
        if (json.containsKey("result")) {
            final JobResult result = JobResult.fromBase64String(json.getString("result"));
            return result;
        } else {
            return JobResult.emptyJobResult().withMessage("ERROR: no job result in: " + json.toString());
        }
    }

    /**
     * kill the given job
     *
     * @param aJobId job id as returned by startJob
     * @param aContextContainer
     * @throws GenericKnimeSparkException
     */
    public static void killJob(final KNIMESparkContext aContextContainer, final String aJobId)
            throws GenericKnimeSparkException {
        LOGGER.debug("Killing Spark job with id: " + aJobId);
        if (KNIMEConfigContainer.verboseLogging()) {
            LOGGER.debug("Context: " + aContextContainer);
        }
        Response response = aContextContainer.getREST().delete(aContextContainer, JOBS_PATH + aJobId);
        // we don't care about the response as long as it is "OK"
        aContextContainer.getREST().checkStatus(response, "Failed to kill job " + aJobId + "!", Status.OK);

    }
}
