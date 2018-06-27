package org.knime.bigdata.spark.core.livy.context;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.livy.Job;
import org.apache.livy.JobHandle;
import org.apache.livy.LivyClient;
import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.util.UploadFileCache;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.util.FileUtil;

/**
 * Handles the client-side of the job-related interactions with Apache Livy.
 *
 * This class is threadsafe.
 *
 * @author Bjoern Lohrman, KNIME GmbH
 *
 */
class LivyJobController implements JobController {

    private static final  NodeLogger LOGGER = NodeLogger.getLogger(LivyJobController.class);

    private final UploadFileCache m_uploadFileCache = new UploadFileCache();

    private final LivyClient m_livyClient;

    private final Class<Job<LivyJobOutput>> m_livyJobClass;

    LivyJobController(final LivyClient livyClient, final Class<Job<LivyJobOutput>> livyJobClass) {

        m_livyClient = livyClient;
        m_livyJobClass = livyJobClass;
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

        final List<String> serverFilenames;
        if (job.useInputFileCopyCache()) {
            serverFilenames = uploadInputFilesCached(job, exec);
        } else {
            serverFilenames = doUploadFiles(job.getInputFiles(), exec);
        }

        return startJobAndWaitForResult(job, serverFilenames, exec);
    }

    private List<String> uploadInputFilesCached(final JobWithFilesRun<?, ?> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

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

        final Iterator<String> serverFilenameIter = doUploadFiles(filesToUpload, exec).iterator();
        final Iterator<File> localFileIter = filesToUpload.iterator();

        while (localFileIter.hasNext()) {
            final File localFile = localFileIter.next();
            final String serverFilename = serverFilenameIter.next();

            m_uploadFileCache.addFilesToCache(localFile, serverFilename);
            serverFilenamesToReturn.add(serverFilename);
        }

        return serverFilenamesToReturn;
    }

    private List<String> doUploadFiles(final List<File> filesToUpload, final ExecutionMonitor exec)
        throws CanceledExecutionException, KNIMESparkException {

        List<String> serverFilenames = new LinkedList<>();

        File currTempFile = null;
        try {

            for (File localFileToUpload : filesToUpload) {
                currTempFile = FileUtil.createTempFile(UUID.randomUUID().toString(), "");
                FileUtil.copy(localFileToUpload, currTempFile);

                Future<?> uploadFuture = m_livyClient.uploadFile(currTempFile);
                LivySparkContext.waitForFuture(uploadFuture, exec);

                serverFilenames.add(currTempFile.getName());
                currTempFile.delete();
            }
        } catch (IOException e) {
            if (currTempFile != null && currTempFile.exists()) {
                currTempFile.delete();
            }
            throw new KNIMESparkException(e);
        } catch (Exception e) {
            LivySparkContext.handleLivyException(e);
        }

        return serverFilenames;
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

        JobHandle<LivyJobOutput> jobHandle = startJobAsynchronously(job, inputFilesOnServer);

        final LivyJobOutput jobserverOutput = LivySparkContext.waitForFuture(jobHandle, exec);

        try {
            // FIXME this is a bad hack around classloading issues because Livy does not allow
            // us to set a classloader for deserialization of the job result
            jobserverOutput.setInternalMap(LivyJobSerializationUtils
                .postKryoDeserialize(jobserverOutput.getInternalMap(), job.getJobOutputClassLoader()));
        } catch (ClassNotFoundException | IOException e) {
            throw new KNIMESparkException(e);
        }

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

    private JobHandle<LivyJobOutput> startJobAsynchronously(final JobRun<?, ?> job,
        final List<String> inputFilesOnServer) throws KNIMESparkException {

        final String jobClassName = job.getJobClass().getCanonicalName();
        LOGGER.debug("Submitting Spark job: " + jobClassName);

        LivyJobInput jsInput = LivyJobInput.createFromSparkJobInput(job.getInput(), jobClassName);

        if (inputFilesOnServer != null) {
            jsInput = jsInput.withFiles(inputFilesOnServer);
        }

        jsInput.setInternalMap(LivyJobSerializationUtils.preKryoSerialize(jsInput.getInternalMap()));

        try {
            final Job<LivyJobOutput> livyJob = m_livyJobClass.getConstructor(LivyJobInput.class).newInstance(jsInput);
            return m_livyClient.submit(livyJob);
        } catch (Exception e) {
            LivySparkContext.handleLivyException(e);
            return null; // never reached
        }
    }
}
