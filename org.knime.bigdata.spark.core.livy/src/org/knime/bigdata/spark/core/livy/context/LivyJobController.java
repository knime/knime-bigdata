package org.knime.bigdata.spark.core.livy.context;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.livy.Job;
import org.apache.livy.JobHandle;
import org.apache.livy.LivyClient;
import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobInput;
import org.knime.bigdata.spark.core.livy.jobapi.LivyJobSerializationUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;

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


    private final LivyClient m_livyClient;
    
    private final RemoteFSController m_remoteFSController;

    private final Class<Job<WrapperJobOutput>> m_livyJobClass;

    private final JobBasedNamedObjectsController m_namedObjectsController;

    LivyJobController(final LivyClient livyClient, final RemoteFSController remoteFSController,
        final Class<Job<WrapperJobOutput>> livyJobClass, final JobBasedNamedObjectsController namedObjectsController) {

        m_livyClient = livyClient;
        m_remoteFSController = remoteFSController;
        m_livyJobClass = livyJobClass;
        m_namedObjectsController = namedObjectsController;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void startJobAndWaitForResult(final SimpleJobRun<?> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {
        startJobAndWaitForResult((JobRun<?,?>)job, exec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <O extends JobOutput> O startJobAndWaitForResult(final JobWithFilesRun<?, O> job,
        final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        
        final JobInput input = job.getInput();
        
        if (input.hasFiles()) {
            throw new IllegalArgumentException(
                "JobWithFilesRun does not support a JobInput with additional input files. Please use either one, but not both.");
        }

        exec.setMessage("Uploading input data to remote file system");
        if (job.useInputFileCopyCache() && job.getInputFilesLifetime() != FileLifetime.CONTEXT) {
            throw new IllegalArgumentException(
                "File copy cache can only be used for files with lifetime " + FileLifetime.CONTEXT);
        }

        for (File inputFile : job.getInputFiles()) {
            input.withFile(inputFile.toPath());
        }

        return startJobAndWaitForResult((JobRun<?, O>)job, exec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <O extends JobOutput> O startJobAndWaitForResult(final JobRun<?, O> job, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

            final LivyJobInput jsInput = LivyJobSerializationUtils.preKryoSerialize(
                LivyJobInput.createFromSparkJobInput(job.getInput(), job.getJobClass().getCanonicalName()),
                m_remoteFSController, new LivyJobInput());
            
            exec.checkCanceled();
            exec.setMessage("Running Spark job");
            
            JobHandle<WrapperJobOutput> jobHandle = startJobAsynchronously(jsInput);
            
            final WrapperJobOutput jobOutput = LivySparkContext.waitForFuture(jobHandle, exec);
            
            LivyJobSerializationUtils.postKryoDeserialize(jobOutput, job.getJobOutputClassLoader(), m_remoteFSController);
            
            if (jobOutput.isError()) {
                throw jobOutput.getException();
            } else {
                addNamedObjectStatistics(jobOutput);
                return unwrapActualJobOutput(jobOutput, job.getJobOutputClass());
            }
    }

    private static <O extends JobOutput> O unwrapActualJobOutput(final WrapperJobOutput jobOutput,
        final Class<O> jobOutputClass) throws KNIMESparkException {

        if (EmptyJobOutput.class.isAssignableFrom(jobOutputClass)) {
            return null;
        } else {
            try {
                return jobOutput.<O> getSparkJobOutput(jobOutputClass);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new KNIMESparkException(e);
            }
        }
    }

    /**
     * @param jobOutput
     */
    private void addNamedObjectStatistics(final WrapperJobOutput jobOutput) {
        final Map<String, NamedObjectStatistics> stats = jobOutput.getNamedObjectStatistics();
        if (stats != null) {
            for (Entry<String, NamedObjectStatistics> kv : stats.entrySet()) {
                m_namedObjectsController.addNamedObjectStatistics(kv.getKey(), kv.getValue());
            }
        }
    }

    private JobHandle<WrapperJobOutput> startJobAsynchronously(final LivyJobInput jsInput) throws KNIMESparkException {
        LOGGER.debug("Submitting Spark job: " + jsInput.getSparkJobClass());

        try {
            final Job<WrapperJobOutput> livyJob = m_livyJobClass.getConstructor(LivyJobInput.class).newInstance(jsInput);
            return m_livyClient.submit(livyJob);
        } catch (Exception e) {
            LivySparkContext.handleLivyException(e);
            return null; // never reached
        }
    }
}
