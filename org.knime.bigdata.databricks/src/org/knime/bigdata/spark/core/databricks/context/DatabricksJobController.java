/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 */
package org.knime.bigdata.spark.core.databricks.context;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

import org.knime.bigdata.spark.core.context.JobController;
import org.knime.bigdata.spark.core.context.namedobjects.JobBasedNamedObjectsController;
import org.knime.bigdata.spark.core.context.namedobjects.NamedObjectStatistics;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobInput;
import org.knime.bigdata.spark.core.databricks.jobapi.DatabricksJobSerializationUtils;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.EmptyJobOutput;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.job.JobRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun;
import org.knime.bigdata.spark.core.job.JobWithFilesRun.FileLifetime;
import org.knime.bigdata.spark.core.job.SimpleJobRun;
import org.knime.bigdata.spark.core.job.WrapperJobOutput;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

/**
 * Handles the client-side of the job-related interactions with Databricks.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
class DatabricksJobController implements JobController {

    private final DatabricksClient m_databricksClient;

    private final RemoteFSController m_remoteFSController;

    private final Class<?> m_databricksJobClass;

    private final JobBasedNamedObjectsController m_namedObjectsController;

    private String m_contextId;

    DatabricksJobController(final DatabricksClient databricksClient, final RemoteFSController remoteFSController,
        final Class<?> databricksJobClass, final JobBasedNamedObjectsController namedObjectsController) {

        m_databricksClient = databricksClient;
        m_remoteFSController = remoteFSController;
        m_databricksJobClass = databricksJobClass;
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

        final String jobID = DatabricksJobSerializationUtils.nextJobID();
        DatabricksJobSerializationUtils.serializeJobInputToStagingFile(jobID,
            DatabricksJobInput.createFromSparkJobInput(job.getInput(), job.getJobClass().getCanonicalName()),
            m_remoteFSController);

        exec.checkCanceled();
        exec.setMessage("Running Spark job");
        m_databricksClient.run(m_contextId, jobID, exec);

        WrapperJobOutput jobOutput = DatabricksJobSerializationUtils.deserializeJobOutputFromStagingFile(jobID,
            job.getJobOutputClassLoader(), m_remoteFSController);

        if (jobOutput == null) {
            throw new KNIMESparkException("Unable to download result of job " + jobID);
        } else if (jobOutput.isError()) {
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

    /**
     * Creates a context if not already created
     *
     * @param exec execution monitor to check for cancellation
     * @throws KNIMESparkException on failures
     * @throws CanceledExecutionException if execution was canceled
     */
    protected void createContext(final ExecutionMonitor exec) throws KNIMESparkException, CanceledExecutionException {
        if (m_contextId == null) {
            exec.checkCanceled();
            exec.setMessage("Creating Spark Context");

            final String jobClassName = m_databricksJobClass.getName();
            m_contextId = m_databricksClient.createContext(jobClassName, m_remoteFSController.getStagingArea(),
                m_remoteFSController.getStagingAreaReturnsPath(), exec);
        }
    }

    /**
     * Destroys context if created
     *
     * @throws KNIMESparkException on failures
     */
    protected void destroyContext() throws KNIMESparkException {
        if (m_contextId != null) {
            m_databricksClient.destroyContext(m_contextId);
            m_contextId = null;
        }
    }

    /**
     * Reset cached context ID, useful on cluster termination.
     */
    protected void reset() {
        if (m_contextId != null) {
            m_contextId = null;
        }
    }
}
