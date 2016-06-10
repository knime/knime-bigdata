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
 *   Created on 28.01.2016 by koetter
 */
package com.knime.bigdata.spark.core.job;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 *
 * @author Tobias Koetter, KNIME.com
 * @param <I> the {@link JobInput} object
 * @param <O> the {@link JobOutput} object
 */
public interface JobRun<I extends JobInput, O extends JobOutput> {

    Class<?> getJobClass();

    Class<O> getJobOutputClass();

    I getInput();

    O run(final SparkContextID contextID, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException;

    O run(final SparkContextID contextID) throws KNIMESparkException;

}
