package org.knime.bigdata.spark.local.context;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.local.wrapper.LocalSparkJobInput;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapper;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 * {@link JobController} implementation for local Spark.
 * 
 * @author Bjoern Lohrmann, KNIME GmbH
 */
class LocalSparkJobController implements JobController {

    /**
     * Spark jobs in local Spark run synchronously, therefore we need to put them into a thread-pool,
     * otherwise we cannot detect whether the execution monitor has been canceled. 
     */
    private final ExecutorService THREAD_POOL = Executors.newCachedThreadPool();
    
    private final LocalSparkNamedObjectsController m_namedObjectsController;

    private LocalSparkWrapper m_wrapper;

    /**
     * Constructor.
     * 
     * @param wrapper The wrapper for the Spark instance to run jobs on.
     */
	LocalSparkJobController(LocalSparkWrapper wrapper, final LocalSparkNamedObjectsController namedObjectsController) {
		m_wrapper = wrapper;
		m_namedObjectsController = namedObjectsController;
	}

	@Override
	public <O extends JobOutput> O startJobAndWaitForResult(JobWithFilesRun<?, O> fileJob, ExecutionMonitor exec)
			throws KNIMESparkException, CanceledExecutionException {
		
		final List<String> files = fileJob.getInputFiles()
			.stream()
			.map((f) -> f.getAbsolutePath())
			.collect(Collectors.toList());
		
		return runInternal(fileJob, files, exec);
	}
	
	private <O extends JobOutput> O runInternal(final JobRun<?, O> job, final List<String> inputFilesOnServer,
			final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {

		exec.setMessage("Running Spark job");

		final LocalSparkJobInput input = LocalSparkJobInput.createFromSparkJobInput(job.getInput(),
				job.getJobClass().getName());

		if (!inputFilesOnServer.isEmpty()) {
			input.withFiles(inputFilesOnServer);
		}
		
		
        final Map<String, Object> serializedInput =
            LocalSparkSerializationUtil.serializeToPlainJavaTypes(input.getInternalMap());
        final String jobGroupId = UUID.randomUUID().toString();

        final Map<String, Object> serializedOutput =
            waitForFuture(THREAD_POOL.submit(() -> m_wrapper.runJob(serializedInput, jobGroupId)), jobGroupId, exec);
		
        WrapperJobOutput toReturn;
		try {
			toReturn = WrapperJobOutput.fromMap(LocalSparkSerializationUtil.deserializeFromPlainJavaTypes(serializedOutput, job.getJobOutputClassLoader()));
		} catch (IOException | ClassNotFoundException e) {
			throw new KNIMESparkException(e);
		}

		if (toReturn.isError()) {
		    throw toReturn.getException();
        } else {
            final Map<String, NamedObjectStatistics> stats = toReturn.getNamedObjectStatistics();
            if (stats != null) {
                for (Entry<String, NamedObjectStatistics> kv : stats.entrySet()) {
                    m_namedObjectsController.addNamedObjectStatistics(kv.getKey(), kv.getValue());
                }
            }

            if (job instanceof SimpleJobRun) {
                return null;
            } else {
                try {
                    return toReturn.<O> getSparkJobOutput(job.getJobOutputClass());
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new KNIMESparkException(e);
                }
            }
		}
	}
	
    private <O> O waitForFuture(final Future<O> future, final String jobGroupId, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        while (true) {
            try {
                return future.get(200, TimeUnit.MILLISECONDS);
            } catch (final TimeoutException | InterruptedException e) {
                checkForCancelation(future, jobGroupId, exec);
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof InterruptedException) {
                    throw new CanceledExecutionException();
                } else {
                    // this should normally not happen and is actually more a sign of a bug
                    throw new KNIMESparkException(cause);
                }
            }
        }
    }

    private void checkForCancelation(Future<?> future, String jobGroupId, ExecutionMonitor exec) throws CanceledExecutionException {
        if (exec != null) {
            try {
                exec.checkCanceled();
            } catch (final CanceledExecutionException canceledInKNIME) {
                future.cancel(true);
                m_wrapper.cancelJob(jobGroupId);
                throw canceledInKNIME;
            }
        }
    }

    
	@Override
	public <O extends JobOutput> O startJobAndWaitForResult(JobRun<?, O> job, ExecutionMonitor exec)
			throws KNIMESparkException, CanceledExecutionException {

		return runInternal(job, Collections.emptyList(), exec);
	}

	@Override
	public void startJobAndWaitForResult(SimpleJobRun<?> job, ExecutionMonitor exec)
			throws KNIMESparkException, CanceledExecutionException {
		runInternal(job, Collections.emptyList(), exec);
	}
}
