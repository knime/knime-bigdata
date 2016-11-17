package com.knime.bigdata.spark.core.context.jobserver;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import javax.json.JsonObject;

import org.apache.log4j.Priority;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

import com.knime.bigdata.spark.core.context.JobController;
import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextNotFoundException;
import com.knime.bigdata.spark.core.context.jobserver.request.DeleteDataFileRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.GetJobStatusRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.JobAlreadyFinishedException;
import com.knime.bigdata.spark.core.context.jobserver.request.KillJobRequest;
import com.knime.bigdata.spark.core.context.jobserver.request.StartJobRequest;
import com.knime.bigdata.spark.core.context.jobserver.rest.RestClient;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import com.knime.bigdata.spark.core.job.SimpleJobRun;
import com.knime.bigdata.spark.core.jobserver.JobserverJobOutput;
import com.knime.bigdata.spark.core.jobserver.LogMessage;
import com.knime.bigdata.spark.core.jobserver.TypesafeConfigSerializationUtils;
import com.knime.bigdata.spark.core.port.context.SparkContextConfig;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * handles the client side of the job-server in all requests related to jobs
 *
 * this class is threadsafe
 *
 * @author dwk, Bjoern Lohrmann, KNIME.COMinputFile
 *
 */
class JobserverJobController implements JobController {

    private final static NodeLogger LOGGER = NodeLogger.getLogger(JobserverJobController.class.getName());

    /**
     * Many jobs are very quick to finish. So before going into the usually high default/user-configured check
     * frequency, this array defines a fixed number of checks with low wait time.
     */
    private final static long[] INITIAL_JOB_CHECK_WAIT_TIMES = new long[]{30, 100, 1000};

    private final UploadFileCache m_uploadFileCache = new UploadFileCache();

    private final SparkContextID m_contextId;

    private final SparkContextConfig m_contextConfig;

    private final RestClient m_restClient;

    private final String m_jobserverJobClass;

    private final String m_jobserverAppName;

    /**
     * Starting with pull request 469, Spark jobserver has a configuration flag
     * shiro.use-as-proxy-user. If it is on /and/ authentication is on, then
     * all contexts an authenticated user 'joe' creates will impersonate this user (Hadoop impersonation)
     * AND will have a different name, e.g. joe~myContextName (instead of just myContextName).
     *
     * This name needs to be specified for each job user joe executes. If this variable is true
     * and authentication is on, then job submission will use a context name of the form userName~contextName
     * instead of just contextName.
     */
    private boolean m_prependUserToContextName;

    JobserverJobController(final SparkContextID contextId, final SparkContextConfig contextConfig, final String jobserverAppName,
        final RestClient restClient, final String jobserverJobClass) {
        m_contextId = contextId;
        m_contextConfig = contextConfig;
        m_jobserverAppName = jobserverAppName;
        m_restClient = restClient;
        m_jobserverJobClass = jobserverJobClass;
        m_prependUserToContextName = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startJobAndWaitForResult(final SimpleJobRun<?> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {
        startJobAndWaitForResult(job, null, exec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <O extends JobOutput> O startJobAndWaitForResult(final JobWithFilesRun<?, O> job,
        final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {

        exec.setMessage("Uploading data to Spark jobserver");
        if (job.useInputFileCopyCache() && job.getInputFilesLifetime() != FileLifetime.CONTEXT) {
            throw new IllegalArgumentException(
                "File copy cache can only be used for files with lifetime " + FileLifetime.CONTEXT);
        }

        if (job.useInputFileCopyCache()) {
            final List<String> serverFilePaths = uploadInputFilesCached(job);
            return startJobAndWaitForResult(job, serverFilePaths, exec);
        } else {
            UploadUtil uploadUtil = uploadInputFiles(job.getInputFiles(), job.getInputFilesLifetime());
            try {
                return startJobAndWaitForResult(job, uploadUtil.getServerFileNames(), exec);
            } finally {
                uploadUtil.cleanup();
            }
        }

    }

    private UploadUtil uploadInputFiles(final List<File> filesToUplad, final FileLifetime fileLifetime)
        throws KNIMESparkException {

        // FIXME implement support for cleanup of files with FileLifetime.CONTEXT
        UploadUtil uploadUtil =
            new UploadUtil(m_contextId, m_contextConfig, m_restClient, filesToUplad, fileLifetime == FileLifetime.JOB);

        uploadUtil.upload();
        return uploadUtil;
    }

    /**
     * @param job
     * @param exec
     * @throws KNIMESparkException
     */
    private List<String> uploadInputFilesCached(final JobWithFilesRun<?, ?> job) throws KNIMESparkException {

        final List<String> serverFilenamesToReturn = new LinkedList<>();

        // first we determine the files we have to upload
        final List<File> filesToUpload = new LinkedList<>();
        for (File inputFile : job.getInputFiles()) {
            String cachedServerFile = m_uploadFileCache.tryToGetServerFileFromCache(inputFile);
            if (cachedServerFile != null) {
                serverFilenamesToReturn.add(cachedServerFile);
            } else {
                filesToUpload.add(inputFile);
            }
        }

        // now we upload those files
        UploadUtil uploadUtil = new UploadUtil(m_contextId, m_contextConfig, m_restClient, filesToUpload);
        uploadUtil.upload();

        // now we add the uploaded files to the upload file cache
        Iterator<File> localFileIter = filesToUpload.iterator();
        Iterator<String> serverFilesIter = uploadUtil.getServerFileNames().iterator();

        while (localFileIter.hasNext()) {
            final File localFile = localFileIter.next();
            final String serverFile = serverFilesIter.next();

            if (m_uploadFileCache.addFilesToCache(localFile, serverFile)) {
                serverFilenamesToReturn.add(serverFile);
            } else {
                // in the meantime someone else has uploaded the same file or a newer version of it
                // this means we can discard our file
                serverFilenamesToReturn.add(m_uploadFileCache.tryToGetServerFileFromCache(localFile));
                new DeleteDataFileRequest(m_contextId, m_contextConfig, m_restClient, serverFile).send();
            }
        }

        return serverFilenamesToReturn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <O extends JobOutput> O startJobAndWaitForResult(final JobRun<?, O> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        return startJobAndWaitForResult(job, null, exec);
    }

    private <O extends JobOutput> O startJobAndWaitForResult(final JobRun<?, O> job,
        final List<String> inputFilesOnServer, final ExecutionMonitor exec)
            throws KNIMESparkException, CanceledExecutionException {

        exec.setMessage("Running Spark job");

        String jobId = startJobAsynchronously(job, inputFilesOnServer);

        JobserverJobOutput jobserverOutput = waitForJob(job, jobId, exec);
        logMessages(jobserverOutput.getLogMessages());

        if (jobserverOutput.isError()) {
            Throwable cause = jobserverOutput.getThrowable();
            if (cause instanceof KNIMESparkException) {
                throw (KNIMESparkException)cause;
            } else {
                throw new KNIMESparkException("Job execution failed " + KNIMESparkException.SEE_LOG_SNIPPET, cause);
            }
        } else if (job instanceof SimpleJobRun) {
            return null;
        } else {
            try {
                return jobserverOutput.<O> getSparkJobOutput(job.getJobOutputClass());
            } catch (InstantiationException | IllegalAccessException e) {
                LOGGER.error("Failed to instantiate Spark job output: " + e.getMessage(), e);
                throw new KNIMESparkException("Failed to deserialize Spark job output", e);
            }
        }
    }

    private String startJobAsynchronously(final JobRun<?, ?> job, final List<String> inputFilesOnServer)
        throws KNIMESparkException {
        final String jobClassName = job.getJobClass().getCanonicalName();

        LOGGER.debug("Submitting Spark job: " + jobClassName);

        JsonObject jsonResponse;

        try {
            jsonResponse =
                new StartJobRequest(m_contextId,
                    m_contextConfig,
                    m_jobserverAppName,
                    m_prependUserToContextName,
                    m_restClient,
                    m_jobserverJobClass,
                    jobClassName,
                    job.getInput(),
                    inputFilesOnServer).send();
        } catch (SparkContextNotFoundException e) {
            if (!m_contextConfig.useAuthentication()) {
                throw e;
            }
            // if authentication is on, then first we try to toggle m_prependUserToContextName,
            // because we do not know whether jobserver has shiro.use-as-proxy-user on or off.
            try {
                jsonResponse = new StartJobRequest(m_contextId,
                    m_contextConfig,
                    m_jobserverAppName,
                    !m_prependUserToContextName,
                    m_restClient,
                    m_jobserverJobClass,
                    jobClassName,
                    job.getInput(),
                    inputFilesOnServer).send();

                // if we are here, toggling m_prependUserToContextName worked, so we memorize that
                m_prependUserToContextName = !m_prependUserToContextName;
            } catch (SparkContextNotFoundException toIgnore) {
                // if we are here, toggling m_prependUserToContextName did NOT work.
                // The context apparently does not exist anymore, hence we rethrow the original exception
                throw e;
            }
        }

        return jsonResponse.getJsonObject("result").getString("jobId");
    }

    private void logMessages(final List<LogMessage> logMessages) throws KNIMESparkException {
        for (LogMessage logMessage : logMessages) {
            switch (logMessage.getLog4jLogLevel()) {
                case Priority.DEBUG_INT:
                    LOGGER.debug(String.format("%s: %s", logMessage.getLoggerName(), logMessage.getMessage()));
                    break;
                case Priority.INFO_INT:
                    LOGGER.info(String.format("%s: %s", logMessage.getLoggerName(), logMessage.getMessage()));
                    break;
                case Priority.WARN_INT:
                    LOGGER.warn(String.format("%s: %s", logMessage.getLoggerName(), logMessage.getMessage()));
                    break;
                case Priority.ERROR_INT:
                    LOGGER.error(String.format("%s: %s", logMessage.getLoggerName(), logMessage.getMessage()));
                    break;
                case Priority.FATAL_INT:
                    LOGGER.fatal(String.format("%s: %s", logMessage.getLoggerName(), logMessage.getMessage()));
                    break;
                default:
                    break;
            }
        }
    }

    private JobserverJobOutput waitForJob(final JobRun<?, ?> jobExecution, final String jobID,
        final ExecutionMonitor exec) throws CanceledExecutionException, KNIMESparkException {

        final int aTimeoutInSeconds = m_contextConfig.getJobTimeout();
        final int aCheckFrequencyInSeconds = m_contextConfig.getJobCheckFrequency();

        LOGGER.debug("Start waiting for job...");
        exec.setMessage("Waiting for Spark job to finish...");

        final long timeOfStart = System.currentTimeMillis();
        final long timeOfTimeout = timeOfStart + (aTimeoutInSeconds * 1000);

        int checkCounter = 0;
        while (System.currentTimeMillis() <= timeOfTimeout) {
            sleepSafely(computeSleepTime(checkCounter, timeOfTimeout, aCheckFrequencyInSeconds));
            checkCounter++;

            // (re)throws CanceledExecutionException if a *running* job was killed
            // if job was not running anymore we will continue fetching the results
            killRunningJobIfCanceled(exec, jobID);

            // throws a KNIMESparkException if request failed
            JsonObject jobData = pollJobData(jobID);
            JobStatus status = JobStatus.valueOf(jobData.getString("status"));

            switch (status) {
                case RUNNING:
                    exec.setMessage(String.format("Waiting for Spark job to finish (Execution time: %d seconds",
                        (System.currentTimeMillis() - timeOfStart) / 1000));
                    break;
                case DONE:
                case FINISHED:
                case OK:
                    if (!jobData.containsKey("result")) {
                        throw new KNIMESparkException("Unable to fetch job result, might be removed from result cache."
                            + " See spark.jobserver.job-result-cache-size in environment.conf to increase cache size.");
                    }

                    try {
                        final Config typesafeConfig = ConfigFactory.parseString(jobData.getString("result"));
                        return JobserverJobOutput
                            .fromMap(TypesafeConfigSerializationUtils.deserializeFromTypesafeConfig(typesafeConfig,
                                jobExecution.getJobClass().getClassLoader()));
                    } catch (ClassNotFoundException | IOException e) {
                        throw new KNIMESparkException(e);
                    }
                case KILLED:
                    throw new KNIMESparkException("Spark job was cancelled");
            }
        }

        killJobSafely(jobID);
        throw new KNIMESparkException("Canceled job due to timeout. You might want to increase the "
            + "job timeout for this Spark context in File > Preferences > KNIME > Spark "
            + "(or the 'Create Spark Context node', if you are not using the default Spark context).");
    }

    private void killRunningJobIfCanceled(final ExecutionMonitor exec, final String jobID)
        throws CanceledExecutionException {

        try {
            exec.checkCanceled();
        } catch (CanceledExecutionException c) {
            LOGGER.warn("Cancelling job: " + jobID);
            try {
                new KillJobRequest(m_contextId, m_contextConfig, m_restClient, jobID).send();
                throw c;
            } catch (JobAlreadyFinishedException e) {
                // do nothing, not even rethrow CanceledExecutionException
            } catch (KNIMESparkException e) {
                LOGGER.error(String.format("Failed to cancel job %s (Message: %s).", jobID, e.getMessage()));
                throw c;
            }
        }
    }

    private void killJobSafely(final String jobID) {
        try {
            LOGGER.warn("Cancelling job: " + jobID);
            new KillJobRequest(m_contextId, m_contextConfig, m_restClient, jobID).send();
        } catch (KNIMESparkException e) {
            LOGGER.error("Failed to cancel job " + jobID + "\nMessage: " + e.getMessage());
        }
    }

    private long computeSleepTime(final int checkCounter, final long timeOfTimeout, final int checkFrequencyInSeconds) {

        long sleepTime = checkFrequencyInSeconds * 1000;
        if (checkCounter < INITIAL_JOB_CHECK_WAIT_TIMES.length) {
            sleepTime = INITIAL_JOB_CHECK_WAIT_TIMES[checkCounter];
        }

        return Math.max(0, Math.min(sleepTime, timeOfTimeout - System.currentTimeMillis()));
    }

    private void sleepSafely(final long sleepTimeMillis) {
        try {
            Thread.sleep(sleepTimeMillis);
        } catch (InterruptedException e) {
            // ignore and continue...
        }
    }

    /**
     * query the job-server for the result of the given job (it is typically a good idea to ask first whether the job
     * finished successfully)
     *
     * @param m_contextConfig context configuration container
     * @param jobID job id as returned by startJob
     * @return JSONObject with job status and result
     * @throws KNIMESparkException
     */
    private JsonObject pollJobData(final String jobID) throws KNIMESparkException {
        // GET /jobs/<jobId> - Gets the result or status of a specific job
        LOGGER.debug("Polling status of job: " + jobID);
        return new GetJobStatusRequest(m_contextId, m_contextConfig, m_restClient, jobID).send();
    }
}
