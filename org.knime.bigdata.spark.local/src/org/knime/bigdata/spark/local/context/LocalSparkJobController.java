package org.knime.bigdata.spark.local.context;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.local.wrapper.LocalSparkJobInput;
import org.knime.bigdata.spark.local.wrapper.LocalSparkJobOutput;
import org.knime.bigdata.spark.local.wrapper.LocalSparkWrapper;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

public class LocalSparkJobController implements JobController {

	private LocalSparkWrapper m_wrapper;

	public LocalSparkJobController(LocalSparkWrapper wrapper) {
		m_wrapper = wrapper;
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

		LocalSparkJobInput input = LocalSparkJobInput.createFromSparkJobInput(job.getInput(),
				job.getJobClass().getName());

		if (!inputFilesOnServer.isEmpty()) {
			input.withFiles(inputFilesOnServer);
		}
		
		final Map<String, Object> serializedInput = LocalSparkSerializationUtil.serializeToPlainJavaTypes(input.getInternalMap());
		final Map<String, Object> serializedOutput = m_wrapper.runJob(serializedInput);

		LocalSparkJobOutput out;
		try {
			out = LocalSparkJobOutput.fromMap(LocalSparkSerializationUtil.deserializeFromPlainJavaTypes(serializedOutput, job.getJobOutputClassLoader()));
		} catch (IOException | ClassNotFoundException e) {
			throw new KNIMESparkException(e);
		}

		if (out.isError()) {
			throw out.getException();
		} else if (job instanceof SimpleJobRun) {
			return null;
		} else {
			try {
				return out.<O>getSparkJobOutput(job.getJobOutputClass());
			} catch (InstantiationException | IllegalAccessException e) {
				throw new KNIMESparkException(e);
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
