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
 *   Created on Apr 11, 2016 by bjoern
 */
package com.knime.bigdata.spark.core.job;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextManager;
import com.knime.bigdata.spark.core.exception.KNIMESparkException;

/**
 * Default implementation of {@link JobRun} that is sufficient for many jobs.
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class DefaultJobRun<I extends JobInput, O extends JobOutput> implements JobRun<I, O> {

    private final I m_input;

    private final Class<?> m_sparkJobClass;

    private final Class<O> m_jobOutputClass;

    /**
     * @param input
     * @param sparkJobClass
     * @param jobOutputClass
     */
    public DefaultJobRun(final I input, final Class<?> sparkJobClass, final Class<O> jobOutputClass) {
        super();
        m_input = input;
        m_sparkJobClass = sparkJobClass;
        m_jobOutputClass = jobOutputClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public O run(final SparkContextID contextID) throws KNIMESparkException {
        try {
            return SparkContextManager.getOrCreateSparkContext(contextID).startJobAndWaitForResult(this, new ExecutionMonitor());
        } catch (CanceledExecutionException e) {
            // cannot happen
            return null;
        }
    }

    @Override
    public O run(final SparkContextID contextID, final ExecutionMonitor exec)
        throws KNIMESparkException, CanceledExecutionException {

        if (exec == null) {
            return run(contextID);
        } else {
            return SparkContextManager.getOrCreateSparkContext(contextID).startJobAndWaitForResult(this, exec);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?> getJobClass() {
        return m_sparkJobClass;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public I getInput() {
        return m_input;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<O> getJobOutputClass() {
        return m_jobOutputClass;
    }
}
