/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on Mar 1, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.context;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.core.exception.KNIMESparkException;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.job.JobRun;
import com.knime.bigdata.spark.core.job.JobWithFilesRun;
import com.knime.bigdata.spark.core.job.SimpleJobRun;

/**
 *
 * Implementations must be threadsafe
 *
 * @author Bjoern Lohrmann, KNIME.com GmbH
 */
public interface JobController {

    public <O extends JobOutput> O startJobAndWaitForResult(final JobWithFilesRun<?, O> fileJob, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException;

    public <O extends JobOutput> O startJobAndWaitForResult(final JobRun<?, O> job, final ExecutionMonitor exec)
            throws KNIMESparkException, CanceledExecutionException;

    public void startJobAndWaitForResult(final SimpleJobRun<?> job, final ExecutionMonitor exec)
            throws KNIMESparkException, CanceledExecutionException;
}
